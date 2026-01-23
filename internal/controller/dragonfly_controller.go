/*
Copyright 2023 DragonflyDB authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// DragonflyReconciler reconciles a Dragonfly object
type DragonflyReconciler struct {
	Reconciler
}

//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dragonflydb.io,resources=dragonflies/finalizers,verbs=update
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.14.1/pkg/reconcile
func (r *DragonflyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	log.Info("reconciling dragonfly instance")

	dfi, err := r.getDragonflyInstance(ctx, req.NamespacedName, log)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(fmt.Errorf("failed to get dragonfly instance: %w", err))
	}

	if dfi.isTerminating() {
		// Ignore dragonfly instance that is being foreground deleted
		return ctrl.Result{}, nil
	}

	if err = dfi.reconcileResources(ctx); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile dragonfly resources: %w", err)
	}

	if dfi.isClusterMode() {
		return dfi.reconcileCluster(ctx)
	}

	dfiStatus := dfi.getStatus()

	if dfiStatus.Phase == PhaseReady || dfiStatus.Phase == PhaseReadyOld {
		dfiStatus, err = dfi.detectRollingUpdate(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to detect rolling update: %w", err)
		}
	}

	if dfiStatus.Phase == PhaseRollingUpdate || dfiStatus.IsRollingUpdate {
		log.Info("rolling out new version")

		statefulSet, err := dfi.getStatefulSet(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get statefulset: %w", err)
		}

		if result, err := dfi.allPodsHealthyAndHaveRole(ctx, statefulSet.Status.UpdateRevision); !result.IsZero() || err != nil {
			return result, err
		}

		replicas, err := dfi.getReplicas(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get replicas: %w", err)
		}

		if len(replicas.Items) != int(dfi.df.Spec.Replicas)-1 {
			dfi.log.Info("waiting for all replicas to be configured", "expected", int(dfi.df.Spec.Replicas)-1, "current", len(replicas.Items))
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		// We want to update the replicas first then the master
		// We want to have at most one updated replica in full sync phase at a time
		// if not, requeue
		if result, err := dfi.verifyUpdatedReplicas(ctx, replicas, statefulSet.Status.UpdateRevision); !result.IsZero() || err != nil {
			return result, err
		}

		// if we are here it means that all latest replicas are in stable sync
		// delete older version replicas
		if result, err := dfi.updateReplicas(ctx, replicas, statefulSet.Status.UpdateRevision); !result.IsZero() || err != nil {
			return result, err
		}

		master, err := dfi.getMaster(ctx)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get master: %w", err)
		}

		if !isPodOnLatestVersion(master, statefulSet.Status.UpdateRevision) {
			if err = dfi.updatedMaster(ctx, master, replicas, statefulSet.Status.UpdateRevision); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to update master: %w", err)
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		} else {
			if err = dfi.detectOldMasters(ctx, statefulSet.Status.UpdateRevision); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to detect old masters: %w", err)
			}
		}

		// If we are here all are on latest version
		dfiStatus.Phase = PhaseReady
		// TODO: remove this in a future release.
		dfiStatus.IsRollingUpdate = false
		if err = dfi.patchStatus(ctx, dfiStatus); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update the dragonfly status: %w", err)
		}

		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Completed")
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DragonflyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		// Listen only to spec changes
		For(&dfv1alpha1.Dragonfly{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.StatefulSet{}, builder.MatchEveryOwner).
		Owns(&corev1.Service{}, builder.MatchEveryOwner).
		// Watch pods for cluster mode resources to handle failover/split-brain
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(r.mapPodToDragonfly),
			builder.WithPredicates(clusterModePodPredicate()),
		).
		Named("Dragonfly").
		Complete(r)
}

// mapPodToDragonfly maps a pod to its parent Dragonfly CR for reconciliation.
func (r *DragonflyReconciler) mapPodToDragonfly(ctx context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	// Only process pods that belong to a Dragonfly cluster
	dfName, ok := pod.Labels[resources.DragonflyNameLabelKey]
	if !ok {
		return nil
	}

	// Only process cluster mode pods (those with shard label)
	if _, ok := pod.Labels[resources.ShardNameLabelKey]; !ok {
		return nil
	}

	return []reconcile.Request{
		{
			NamespacedName: types.NamespacedName{
				Name:      dfName,
				Namespace: pod.Namespace,
			},
		},
	}
}

// clusterModePodPredicate filters pod events to only those relevant to cluster mode.
func clusterModePodPredicate() predicate.Predicate {
	isClusterPod := func(obj client.Object) bool {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return false
		}
		// Only process cluster mode pods (those with shard label)
		_, hasShard := pod.Labels[resources.ShardNameLabelKey]
		_, hasDragonfly := pod.Labels[resources.DragonflyNameLabelKey]
		return hasShard && hasDragonfly
	}

	hasRoleOrClusterLabelChange := func(oldPod, newPod *corev1.Pod) bool {
		if oldPod == nil || newPod == nil {
			return false
		}
		if oldPod.Labels[resources.RoleLabelKey] != newPod.Labels[resources.RoleLabelKey] {
			return true
		}
		if oldPod.Labels[resources.ShardNameLabelKey] != newPod.Labels[resources.ShardNameLabelKey] {
			return true
		}
		if oldPod.Labels[resources.DragonflyNameLabelKey] != newPod.Labels[resources.DragonflyNameLabelKey] {
			return true
		}
		return false
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return isClusterPod(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPod, _ := e.ObjectOld.(*corev1.Pod)
			newPod, _ := e.ObjectNew.(*corev1.Pod)
			if !isClusterPod(e.ObjectNew) {
				return false
			}
			return hasRoleOrClusterLabelChange(oldPod, newPod)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return isClusterPod(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}
