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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	"github.com/go-logr/logr"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// errMasterGracePeriod is returned when a master is unhealthy but still within the grace period.
var errMasterGracePeriod = errors.New("master is within stability grace period")

// DragonflyInstance is an abstraction over the `Dragonfly` CRD and provides methods to handle replication.
type DragonflyInstance struct {
	// Dragonfly is the relevant Dragonfly CRD that it performs actions over
	df *dfv1alpha1.Dragonfly

	client                client.Client
	log                   logr.Logger
	scheme                *runtime.Scheme
	eventRecorder         record.EventRecorder
	defaultDragonflyImage string
}

// configureReplication configures the given pod as a master and other pods as replicas
func (dfi *DragonflyInstance) configureReplication(ctx context.Context, master *corev1.Pod) error {
	dfi.log.Info("configuring replication")

	pods, err := dfi.getPods(ctx)
	if err != nil {
		dfi.log.Error(err, "failed to get dragonfly pods")
		return err
	}

	dfiStatus := dfi.getStatus()
	dfiStatus.Phase = PhaseConfiguring
	if err = dfi.patchStatus(ctx, dfiStatus); err != nil {
		dfi.log.Error(err, "failed to update the dragonfly status")
		return err
	}

	dfi.log.Info("configuring pod as master", "pod", master.Name, "ip", master.Status.PodIP)
	if err = dfi.replicaOfNoOne(ctx, master); err != nil {
		dfi.log.Error(err, "failed to configure master", "pod", master.Name)
		return err
	}

	dfiStatus.Phase = PhaseReady
	if err = dfi.patchStatus(ctx, dfiStatus); err != nil {
		dfi.log.Error(err, "failed to update the dragonfly status")
		return err
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Replication", "Updated master instance")

	for _, pod := range pods.Items {
		if pod.Name == master.Name {
			continue
		}

		ready, readyErr := dfi.isPodReady(ctx, &pod)
		if readyErr != nil {
			dfi.log.Error(readyErr, "failed to check pod readiness", "pod", pod.Name)
			return readyErr
		}

		if ready {
			if err = dfi.configureReplica(ctx, &pod, master.Status.PodIP); err != nil {
				dfi.log.Error(err, "failed to configure replica", "pod", pod.Name)
				return err
			}
		}
	}

	return nil
}

// configureReplica configures the given pod as a replica to the given master
func (dfi *DragonflyInstance) configureReplica(ctx context.Context, pod *corev1.Pod, masterIp string) error {
	dfi.log.Info("configuring pod as replica", "pod", pod.Name, "ip", pod.Status.PodIP)

	if err := dfi.replicaOf(ctx, pod, masterIp); err != nil {
		return err
	}

	return nil
}

// checkReplicaRole returns true if the given pod is a replica and is connected to the correct master.
func (dfi *DragonflyInstance) checkReplicaRole(ctx context.Context, pod *corev1.Pod, masterIp string) (bool, error) {
	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}

	var redisRole string
	for _, line := range strings.Split(resp, "\n") {
		if strings.Contains(line, "role") {
			redisRole = strings.Trim(strings.Split(line, ":")[1], "\r")
		}
	}

	if redisRole == resources.Master {
		return false, nil
	}

	var redisMasterIp string
	// check if it is connected to the right master
	for _, line := range strings.Split(resp, "\n") {
		if strings.Contains(line, "master_host") {
			redisMasterIp = strings.Trim(strings.Split(line, ":")[1], "\r")
		}
	}

	// for compatibility, label can be removed in future version
	// check if the masterIp matches either the label (for compatibility) or the annotation
	if masterIp != redisMasterIp {
		if masterIp != pod.Labels[resources.MasterIpLabelKey] && masterIp != pod.Annotations[resources.MasterIpAnnotationKey] {
			return false, nil
		}
	}

	return true, nil
}

// isReplicaStable returns true if the given replica is stable.
func (dfi *DragonflyInstance) isReplicaStable(ctx context.Context, pod *corev1.Pod) (bool, error) {
	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		return false, err
	}

	info, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}

	if info == "" {
		return false, fmt.Errorf("empty info")
	}

	replicationData := parseInfoToMap(info)

	if val, ok := replicationData["master_sync_in_progress"]; ok && val == "1" {
		return false, nil
	}

	if val, ok := replicationData["master_link_status"]; ok && val != "up" {
		return false, nil
	}

	if val, ok := replicationData["master_last_io_seconds_ago"]; ok && val == "-1" {
		return false, nil
	}

	persistenceInfo, err := redisClient.Info(ctx, "persistence").Result()
	if err != nil {
		return false, err
	}

	persistenceData := parseInfoToMap(persistenceInfo)

	if val, ok := persistenceData["loading"]; ok && val != "0" && val != "" {
		return false, nil
	}

	if val, ok := persistenceData["load_state"]; ok && val != "" && val != "done" {
		return false, nil
	}

	return true, nil
}

// checkAndConfigureReplicas checks whether all replicas are configured correctly and if not configures them to the right master
func (dfi *DragonflyInstance) checkAndConfigureReplicas(ctx context.Context, masterIp string) error {
	pods, err := dfi.getPods(ctx)
	if err != nil {
		dfi.log.Error(err, "failed to get dragonfly pods")
		return err
	}

	for _, pod := range pods.Items {
		if isReplica(&pod) {
			dfi.log.Info("checking if replica is configured correctly", "pod", pod.Name)
			ok, err := dfi.checkReplicaRole(ctx, &pod, masterIp)
			if err != nil {
				return err
			}
			// Configure to the right master if not correct
			if !ok {
				dfi.log.Info("configuring pod as replica to the right master", "pod", pod.Name)
				if err := dfi.configureReplica(ctx, &pod, masterIp); err != nil {
					return err
				}
			}
		}

		if !roleExists(&pod) {
			ready, readyErr := dfi.isPodReady(ctx, &pod)
			if readyErr != nil {
				dfi.log.Error(readyErr, "failed to check pod readiness", "pod", pod.Name)
				return readyErr
			}

			if ready {
				if err := dfi.configureReplica(ctx, &pod, masterIp); err != nil {
					return err
				}
			}
		}
	}

	dfi.log.Info("All pods are configured correctly", "dfi", dfi.df.Name)
	return nil
}

// getStatefulSet gets the statefulset object for the dragonfly instance
func (dfi *DragonflyInstance) getStatefulSet(ctx context.Context) (*appsv1.StatefulSet, error) {
	dfi.log.Info("getting statefulset")
	var sts appsv1.StatefulSet
	if err := dfi.client.Get(ctx, client.ObjectKey{Namespace: dfi.df.Namespace, Name: dfi.df.Name}, &sts); err != nil {
		return nil, err
	}
	return &sts, nil
}

// getPods gets all the pods relevant to the dragonfly instance
func (dfi *DragonflyInstance) getPods(ctx context.Context) (*corev1.PodList, error) {
	dfi.log.Info("getting all pods relevant to the dragonfly instance")
	var pods corev1.PodList
	if err := dfi.client.List(ctx, &pods, client.InNamespace(dfi.df.Namespace), client.MatchingLabels{
		resources.DragonflyNameLabelKey:     dfi.df.Name,
		resources.KubernetesPartOfLabelKey:  resources.KubernetesPartOf,
		resources.KubernetesAppNameLabelKey: resources.KubernetesAppName,
	}); err != nil {
		return nil, err
	}

	return &pods, nil
}

// getMaster gets the master pod for the dragonfly instance
func (dfi *DragonflyInstance) getMaster(ctx context.Context) (*corev1.Pod, error) {
	var masterPods corev1.PodList

	matchingLabels := client.MatchingLabels{
		resources.DragonflyNameLabelKey:     dfi.df.Name,
		resources.KubernetesPartOfLabelKey:  resources.KubernetesPartOf,
		resources.KubernetesAppNameLabelKey: resources.KubernetesAppName,
		resources.RoleLabelKey:              resources.Master,
	}

	if dfi.getStatus().Phase == PhaseRollingUpdate || dfi.getStatus().IsRollingUpdate {
		statefulSet, err := dfi.getStatefulSet(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get statefulset: %w", err)
		}

		matchingLabels[appsv1.StatefulSetRevisionLabel] = statefulSet.Status.UpdateRevision

		if err = dfi.client.List(ctx, &masterPods, client.InNamespace(dfi.df.Namespace), matchingLabels); err != nil {
			return nil, fmt.Errorf("failed to list pods: %w", err)
		}
	}

	if dfi.getStatus().Phase == PhaseReady || dfi.getStatus().Phase == PhaseReadyOld || len(masterPods.Items) == 0 {
		delete(matchingLabels, appsv1.StatefulSetRevisionLabel)
		if err := dfi.client.List(ctx, &masterPods, client.InNamespace(dfi.df.Namespace), matchingLabels); err != nil {
			return nil, fmt.Errorf("failed to list pods: %w", err)
		}
	}

	if len(masterPods.Items) == 0 {
		return nil, ErrNoMaster
	}

	var healthyMasters []corev1.Pod
	for _, pod := range masterPods.Items {
		ready, readyErr := dfi.isPodReady(ctx, &pod)
		if readyErr != nil {
			return nil, fmt.Errorf("failed to verify master readiness: %w", readyErr)
		}
		if ready {
			healthyMasters = append(healthyMasters, pod)
		}
	}

	if len(healthyMasters) == 0 {
		return nil, ErrNoHealthyMaster
	}

	if len(healthyMasters) > 1 {
		return nil, ErrIncorrectMasters
	}

	return &healthyMasters[0], nil
}

// getReplicas gets all the replicas for the dragonfly instance
func (dfi *DragonflyInstance) getReplicas(ctx context.Context) (*corev1.PodList, error) {
	var replicas corev1.PodList
	if err := dfi.client.List(ctx, &replicas, client.InNamespace(dfi.df.Namespace), client.MatchingLabels{
		resources.DragonflyNameLabelKey:     dfi.df.Name,
		resources.KubernetesPartOfLabelKey:  resources.KubernetesPartOf,
		resources.KubernetesAppNameLabelKey: resources.KubernetesAppName,
		resources.RoleLabelKey:              resources.Replica,
	}); err != nil {
		return nil, err
	}

	return &replicas, nil
}

// getHealthyPod gets the first healthy pod for the dragonfly instance
func (dfi *DragonflyInstance) getHealthyPod(ctx context.Context) (*corev1.Pod, error) {
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get dragonfly pods: %w", err)
	}

	for _, pod := range pods.Items {
		ready, readyErr := dfi.isPodReady(ctx, &pod)
		if readyErr != nil {
			return nil, fmt.Errorf("failed to verify pod readiness: %w", readyErr)
		}
		if ready {
			return &pod, nil
		}
	}

	return nil, fmt.Errorf("no healthy pod found")
}

// getStatus gets the status of the dragonfly instance
func (dfi *DragonflyInstance) getStatus() dfv1alpha1.DragonflyStatus {
	return dfi.df.Status
}

// patchStatus patches the status of the dragonfly instance
func (dfi *DragonflyInstance) patchStatus(ctx context.Context, status dfv1alpha1.DragonflyStatus) error {
	dfi.log.Info("updating status", "status", status)

	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	dfi.df.Status = status
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return err
	}

	return nil
}

// updateShardMasterStatus updates the CR status with the new master info for a shard.
// This persists the masterSince timestamp so grace period survives pod deletion.
func (dfi *DragonflyInstance) updateShardMasterStatus(ctx context.Context, shardName string, masterPodName string) error {
	patchFrom := client.MergeFrom(dfi.df.DeepCopy())

	// Initialize cluster status if needed
	if dfi.df.Status.Cluster == nil {
		dfi.df.Status.Cluster = &dfv1alpha1.ClusterStatus{}
	}
	if dfi.df.Status.Cluster.ShardMasters == nil {
		dfi.df.Status.Cluster.ShardMasters = make(map[string]dfv1alpha1.ShardMasterInfo)
	}

	now := metav1.Now()
	dfi.df.Status.Cluster.ShardMasters[shardName] = dfv1alpha1.ShardMasterInfo{
		PodName:     masterPodName,
		MasterSince: &now,
	}

	dfi.log.Info("updating shard master status", "shard", shardName, "master", masterPodName, "masterSince", now)

	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return fmt.Errorf("failed to update shard master status: %w", err)
	}

	return nil
}

// getShardMasterSince retrieves the masterSince timestamp for a shard from CR status.
// Returns nil if no info exists for the shard.
func (dfi *DragonflyInstance) getShardMasterSince(shardName string, masterPodName string) *metav1.Time {
	if dfi.df.Status.Cluster == nil || dfi.df.Status.Cluster.ShardMasters == nil {
		return nil
	}
	info, ok := dfi.df.Status.Cluster.ShardMasters[shardName]
	if !ok {
		return nil
	}
	// Only return the timestamp if it's for the same master pod
	// (if a different pod is master, the old timestamp is stale)
	if info.PodName != masterPodName {
		return nil
	}
	return info.MasterSince
}

func (dfi *DragonflyInstance) adminPort() int32 {
	if dfi.df.Spec.Cluster != nil && dfi.df.Spec.Cluster.AdminPort != 0 {
		return dfi.df.Spec.Cluster.AdminPort
	}
	return resources.DragonflyAdminPort
}

func (dfi *DragonflyInstance) isClusterMode() bool {
	return dfi.df.Spec.Cluster != nil
}

// isTerminating returns true if the dragonfly instance is being deleted
func (dfi *DragonflyInstance) isTerminating() bool {
	return dfi.df.ObjectMeta.DeletionTimestamp != nil
}

// detectOldMasters checks whether there are any old masters and deletes them
func (dfi *DragonflyInstance) detectOldMasters(ctx context.Context, updateRevision string) error {
	var masterPods corev1.PodList

	if err := dfi.client.List(ctx, &masterPods, client.InNamespace(dfi.df.Namespace), client.MatchingLabels{
		resources.DragonflyNameLabelKey:     dfi.df.Name,
		resources.KubernetesPartOfLabelKey:  resources.KubernetesPartOf,
		resources.KubernetesAppNameLabelKey: resources.KubernetesAppName,
		resources.RoleLabelKey:              resources.Master,
	}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	for _, pod := range masterPods.Items {
		if !isPodOnLatestVersion(&pod, updateRevision) {
			dfi.log.Info("deleting old master pod", "pod", pod.Name, "pod_revision", pod.Labels[appsv1.StatefulSetRevisionLabel], "update_revision", updateRevision)
			if err := dfi.client.Delete(ctx, &pod); err != nil {
				return fmt.Errorf("failed to delete pod: %w", err)
			}
		}
	}

	return nil
}

// replicaOf configures the pod as a replica to the given master instance
func (dfi *DragonflyInstance) replicaOf(ctx context.Context, pod *corev1.Pod, masterIp string) error {
	podName := pod.Name
	podIP := pod.Status.PodIP
	var wasMaster bool

	// Sanitize masterIp in case ipv6
	masterIp = sanitizeIp(masterIp)

	// Retry the Redis SLAVEOF command
	err := retryWithBackoff(ctx, 3, time.Second, func() error {
		redisClient := redis.NewClient(&redis.Options{
			ClientName: resources.DragonflyOperatorName,
			Addr:       net.JoinHostPort(podIP, strconv.Itoa(int(dfi.adminPort()))),
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		})
		defer redisClient.Close()

		// Determine if we're switching from master to replica, or just pointing to a new master
		var err error
		wasMaster, err = dfi.hasMasterRole(ctx, redisClient)
		if err != nil {
			dfi.log.Info("retrying hasMasterRole check", "pod", podName, "err", err)
			return fmt.Errorf("failed to determine the current role of the instance: %w", err)
		}

		dfi.log.Info("Trying to invoke SLAVE OF command", "pod", podName, "master", masterIp, "addr", redisClient.Options().Addr)
		resp, err := redisClient.SlaveOf(ctx, masterIp, strconv.Itoa(int(dfi.adminPort()))).Result()
		if err != nil {
			dfi.log.Info("retrying SLAVE OF command", "pod", podName, "err", err)
			return err
		}

		if resp != "OK" {
			return fmt.Errorf("response of `SLAVE OF` on replica is not OK: %s", resp)
		}

		if dfi.df.Spec.Snapshot != nil && dfi.df.Spec.Snapshot.EnableOnMasterOnly {
			dfi.log.Info("clearing snapshot cron schedule on replica", "pod", podName)
			if _, err := redisClient.ConfigSet(ctx, "snapshot_cron", "").Result(); err != nil {
				return fmt.Errorf("failed to clear snapshot_cron on replica %s: %w", podName, err)
			}
		}

		if wasMaster {
			// Prevent clients from sending commands to this old master
			dfi.disconnectClients(ctx, redisClient, pod)
		}

		return nil
	})
	if err != nil {
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "FailedToConfigureReplica",
			fmt.Sprintf("Failed to configure pod %s as replica after retries: %v", podName, err))
		return fmt.Errorf("error running SLAVE OF command after retries: %w", err)
	}

	dfi.log.Info("Marking pod role as replica", "pod", podName, "masterIp", masterIp)
	pod.Labels[resources.RoleLabelKey] = resources.Replica
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[resources.MasterIpAnnotationKey] = masterIp

	// for compatibility, to be removed in future version
	ip := net.ParseIP(masterIp)
	if ip != nil && ip.To4() != nil {
		pod.Labels[resources.MasterIpLabelKey] = masterIp
	}

	if err := dfi.client.Update(ctx, pod); err != nil {
		return fmt.Errorf("could not update replica metadata: %w", err)
	}

	return nil
}

// replicaOfNoOne configures the pod as a master along while updating other pods to be replicas
func (dfi *DragonflyInstance) replicaOfNoOne(ctx context.Context, pod *corev1.Pod) error {
	podName := pod.Name
	podIP := pod.Status.PodIP

	// Retry the Redis SLAVEOF NO ONE command
	err := retryWithBackoff(ctx, 3, time.Second, func() error {
		redisClient := redis.NewClient(&redis.Options{
			ClientName: resources.DragonflyOperatorName,
			Addr:       net.JoinHostPort(podIP, strconv.Itoa(int(dfi.adminPort()))),
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		})
		defer redisClient.Close()

		dfi.log.Info("running SLAVE OF NO ONE command", "pod", podName, "addr", redisClient.Options().Addr)
		resp, err := redisClient.SlaveOf(ctx, "NO", "ONE").Result()
		if err != nil {
			dfi.log.Info("retrying SLAVE OF NO ONE", "pod", podName, "err", err)
			return err
		}

		if resp != "OK" {
			return fmt.Errorf("response of `SLAVE OF NO ONE` on master is not OK: %s", resp)
		}

		if dfi.df.Spec.Snapshot != nil && dfi.df.Spec.Snapshot.EnableOnMasterOnly {
			dfi.log.Info("setting snapshot cron schedule on master", "pod", podName)
			cron := dfi.df.Spec.Snapshot.Cron
			if _, err := redisClient.ConfigSet(ctx, "snapshot_cron", cron).Result(); err != nil {
				return fmt.Errorf("failed to set snapshot_cron on master %s: %w", podName, err)
			}
		}
		return nil
	})
	if err != nil {
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "FailedToPromoteMaster",
			fmt.Sprintf("Failed to promote pod %s to master after retries: %v", podName, err))
		return fmt.Errorf("error running SLAVE OF NO ONE command after retries: %w", err)
	}

	masterIp := podIP

	dfi.log.Info("Marking pod role as master", "pod", podName, "masterIp", masterIp)
	pod.Labels[resources.RoleLabelKey] = resources.Master
	delete(pod.Labels, resources.MasterIpLabelKey)

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[resources.MasterIpAnnotationKey] = masterIp
	pod.Annotations[resources.MasterSinceAnnotationKey] = time.Now().Format(time.RFC3339)

	if err := dfi.client.Update(ctx, pod); err != nil {
		return err
	}

	return nil
}

// disconnectClients disconnects all non-replication clients from a pod.
func (dfi *DragonflyInstance) disconnectClients(ctx context.Context, redisClient *redis.Client, pod *corev1.Pod) {
	dfi.log.Info("disconnecting clients from replica", "pod", pod.Name)
	clientList, err := redisClient.ClientList(ctx).Result()
	if err != nil {
		dfi.log.Error(err, "failed to get client list from replica", "pod", pod.Name)
		return
	}

	clients := []string{}
	for _, clientInfo := range strings.Split(clientList, "\n") {
		if clientInfo == "" {
			continue
		}
		// Example clientInfo: "id=2 addr=10.42.1.123:50342 ... name=..."
		// Avoid killing replication clients, internal clients, or this connection
		if strings.Contains(clientInfo, "addr=127.0.0.1") ||
			strings.Contains(clientInfo, "addr=::1") ||
			strings.Contains(clientInfo, "addr=[::1]") ||
			strings.Contains(clientInfo, "name=repl_") ||
			strings.Contains(clientInfo, "name=dragonfly-operator") {
			continue
		}

		parts := strings.Split(clientInfo, " ")
		for _, part := range parts {
			if strings.HasPrefix(part, "addr=") {
				addr := strings.TrimPrefix(part, "addr=")
				if _, err := redisClient.ClientKill(ctx, addr).Result(); err != nil {
					// Log and continue, don't block for a single failed kill
					dfi.log.Error(err, "failed to kill client", "addr", addr)
				} else {
					clients = append(clients, addr)
				}
				break
			}
		}
	}
	dfi.log.Info("killed clients", "pod", pod.Name, "clients", clients)
}

// hasMasterRole returns true if the given pod is a master based on the replication info.
func (dfi *DragonflyInstance) hasMasterRole(ctx context.Context, redisClient *redis.Client) (bool, error) {
	replInfo, err := redisClient.Info(ctx, "replication").Result()
	if err != nil {
		return false, err
	}
	return strings.Contains(replInfo, "role:master"), nil
}

// reconcileResources creates or updates the dragonfly resources
func (dfi *DragonflyInstance) reconcileResources(ctx context.Context) error {
	dfResources, err := resources.GenerateDragonflyResources(dfi.df, dfi.defaultDragonflyImage)
	if err != nil {
		return fmt.Errorf("failed to generate dragonfly resources")
	}
	for _, desired := range dfResources {
		dfi.log.Info("reconciling dragonfly resource", "kind", getGVK(desired, dfi.scheme).Kind, "namespace", desired.GetNamespace(), "Name", desired.GetName())

		existing := desired.DeepCopyObject().(client.Object)
		err = dfi.client.Get(ctx, client.ObjectKey{
			Namespace: desired.GetNamespace(),
			Name:      desired.GetName(),
		}, existing)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to get resource: %w", err)
			}
			// Resource does not exist, create it
			if err := controllerutil.SetControllerReference(dfi.df, desired, dfi.scheme); err != nil {
				return fmt.Errorf("failed to set controller reference: %w", err)
			}
			err = dfi.client.Create(ctx, desired)
			if err != nil {
				return fmt.Errorf("failed to create resource: %w", err)
			}
			dfi.log.Info("created resource", "resource", desired.GetName())
			continue
		}
		// Resource exists, prepare desired for potential update
		if err := controllerutil.SetControllerReference(dfi.df, desired, dfi.scheme); err != nil {
			return fmt.Errorf("failed to set controller reference: %w", err)
		}
		// Special handling for Services to preserve immutable fields
		if svcDesired, ok := desired.(*corev1.Service); ok {
			if svcExisting, ok := existing.(*corev1.Service); ok {
				svcDesired.Spec.ClusterIP = svcExisting.Spec.ClusterIP
				svcDesired.Spec.IPFamilies = svcExisting.Spec.IPFamilies
				svcDesired.Spec.IPFamilyPolicy = svcExisting.Spec.IPFamilyPolicy
				// Preserve NodePorts for NodePort and LoadBalancer services
				if svcDesired.Spec.Type == corev1.ServiceTypeNodePort || svcDesired.Spec.Type == corev1.ServiceTypeLoadBalancer {
					for i := range svcDesired.Spec.Ports {
						for j := range svcExisting.Spec.Ports {
							if svcDesired.Spec.Ports[i].Name == svcExisting.Spec.Ports[j].Name {
								svcDesired.Spec.Ports[i].NodePort = svcExisting.Spec.Ports[j].NodePort
								break
							}
						}
					}
				}
				// Also preserve HealthCheckNodePort if external
				if svcDesired.Spec.Type == corev1.ServiceTypeLoadBalancer && svcDesired.Spec.ExternalTrafficPolicy == corev1.ServiceExternalTrafficPolicyLocal {
					svcDesired.Spec.HealthCheckNodePort = svcExisting.Spec.HealthCheckNodePort
				}
			}
		}
		// Compare specs; skip if no changes
		if resourceSpecsEqual(desired, existing) {
			dfi.log.Info("no changes detected, skipping update", "resource", desired.GetName())
			continue
		}
		// Update if specs differ
		desired.SetResourceVersion(existing.GetResourceVersion())
		if err = dfi.client.Update(ctx, desired); err != nil {
			return fmt.Errorf("failed to update resource: %w", err)
		}
		dfi.log.Info("updated resource", "resource", desired.GetName())
	}
	if dfi.df.Spec.Cluster == nil && dfi.df.Spec.Replicas < 2 {
		if err = dfi.client.Delete(ctx, &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      dfi.df.Name,
				Namespace: dfi.df.Namespace,
			},
		}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete pod disruption budget: %w", err)
		}
	}
	status := dfi.getStatus()
	if status.Phase == "" {
		status.Phase = PhaseResourcesCreated
		if err = dfi.patchStatus(ctx, status); err != nil {
			return fmt.Errorf("failed to update the dragonfly object")
		}
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Created resources")
	}
	return nil
}

// Helper function to compare resource specs (add to the file)
func resourceSpecsEqual(desired, existing client.Object) bool {
	// Compare metadata labels and annotations
	if !reflect.DeepEqual(desired.GetLabels(), existing.GetLabels()) || !reflect.DeepEqual(desired.GetAnnotations(), existing.GetAnnotations()) {
		return false
	}
	// Compare only the .Spec field using reflection
	desiredV := reflect.ValueOf(desired).Elem()
	existingV := reflect.ValueOf(existing).Elem()
	desiredSpec := desiredV.FieldByName("Spec")
	existingSpec := existingV.FieldByName("Spec")
	if !desiredSpec.IsValid() || !existingSpec.IsValid() {
		return true // No spec field, consider equal
	}
	return reflect.DeepEqual(desiredSpec.Interface(), existingSpec.Interface())
}

func parseInfoToMap(info string) map[string]string {
	data := make(map[string]string)
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(strings.TrimSuffix(parts[1], "\r"))
		data[key] = value
	}
	return data
}

// retryWithBackoff retries the given function with exponential backoff.
// It returns the last error if all attempts fail.
func retryWithBackoff(ctx context.Context, maxAttempts int, baseDelay time.Duration, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := fn(); err != nil {
			lastErr = err
			if attempt < maxAttempts-1 {
				delay := baseDelay * time.Duration(1<<attempt) // exponential: baseDelay * 2^attempt
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		} else {
			return nil
		}
	}
	return lastErr
}

func (dfi *DragonflyInstance) isDatasetLoaded(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Status.PodIP == "" {
		return false, nil
	}

	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
	})
	defer redisClient.Close()

	persistenceInfo, err := redisClient.Info(ctx, "persistence").Result()
	if err != nil {
		return false, err
	}

	data := parseInfoToMap(persistenceInfo)

	if val, ok := data["loading"]; ok && val != "" && val != "0" {
		return false, nil
	}

	if val, ok := data["load_state"]; ok && val != "" && val != "done" {
		return false, nil
	}

	return true, nil
}

func (dfi *DragonflyInstance) isPodReady(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if !isRunningAndReady(pod) || isTerminating(pod) {
		return false, nil
	}

	loaded, err := dfi.isDatasetLoaded(ctx, pod)
	if err != nil {
		return false, fmt.Errorf("failed to determine dataset load status: %w", err)
	}

	return loaded, nil
}

func (dfi *DragonflyInstance) updateNonClusterPhase(ctx context.Context) (bool, error) {
	if dfi.df.Spec.Snapshot == nil {
		return false, nil
	}

	pods, err := dfi.getPods(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get dragonfly pods: %w", err)
	}

	allReady := true
	needsLoadingGate := false
	for _, pod := range pods.Items {
		if !roleExists(&pod) {
			allReady = false
		}

		if !isRunningAndReady(&pod) || isTerminating(&pod) {
			allReady = false
			continue
		}

		loaded, readyErr := dfi.isDatasetLoaded(ctx, &pod)
		if readyErr != nil {
			return false, fmt.Errorf("failed to verify pod readiness: %w", readyErr)
		}
		if !loaded {
			allReady = false
			needsLoadingGate = true
		}
	}

	status := dfi.getStatus()
	if allReady {
		if status.Phase != PhaseReady && status.Phase != PhaseReadyOld {
			status.Phase = PhaseReady
			if err := dfi.patchStatus(ctx, status); err != nil {
				return false, fmt.Errorf("failed to update status: %w", err)
			}
		}
		return false, nil
	}

	if needsLoadingGate && status.Phase == PhaseReady {
		status.Phase = PhaseConfiguring
		if err := dfi.patchStatus(ctx, status); err != nil {
			return false, fmt.Errorf("failed to update status: %w", err)
		}
	}

	return needsLoadingGate, nil
}

// detectRollingUpdate checks whether the pod spec has changed and performs a rolling update if needed
func (dfi *DragonflyInstance) detectRollingUpdate(ctx context.Context) (dfv1alpha1.DragonflyStatus, error) {
	dfi.log.Info("checking if pod spec has changed")
	status := dfi.getStatus()
	statefulSet, err := dfi.getStatefulSet(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get statefulset: %w", err)
	}

	pods, err := dfi.getPods(ctx)
	if err != nil {
		return status, fmt.Errorf("failed to get dragonfly pods: %w", err)
	}

	if needRollingUpdate(pods, statefulSet) {
		dfi.log.Info("pod spec has changed, performing a rollout")
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Starting a rollout")

		status.Phase = PhaseRollingUpdate
		if err = dfi.patchStatus(ctx, status); err != nil {
			return status, fmt.Errorf("failed to update the dragonfly status: %w", err)
		}
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Resources", "Performing a rollout")
	}
	return status, nil
}

// deleteMasterRoleLabel deletes the role label from the pods
func (dfi *DragonflyInstance) deleteMasterRoleLabel(ctx context.Context) error {
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to get dragonfly pods: %w", err)
	}

	for _, pod := range pods.Items {
		if isMaster(&pod) {
			if err = dfi.deleteRoleLabel(ctx, &pod); err != nil {
				return err
			}
		}
	}

	return nil
}

// deleteRoleLabel deletes the role label from the given pod
func (dfi *DragonflyInstance) deleteRoleLabel(ctx context.Context, pod *corev1.Pod) error {
	dfi.log.Info("deleting pod role label", "pod", pod.Name, "role", pod.Labels[resources.RoleLabelKey])

	patchFrom := client.MergeFrom(pod.DeepCopy())
	delete(pod.Labels, resources.RoleLabelKey)

	if err := dfi.client.Patch(ctx, pod, patchFrom); err != nil {
		dfi.log.Error(err, "failed to update the role label", "pod", pod.Name)
		return err
	}

	return nil
}

// allPodsHealthyAndHaveRole checks whether all pods are healthy, and deletes pods that are outdated and failed to start
func (dfi *DragonflyInstance) allPodsHealthyAndHaveRole(ctx context.Context, updateRevision string) (ctrl.Result, error) {
	pods, err := dfi.getPods(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get dragonfly pods: %w", err)
	}

	for _, pod := range pods.Items {
		if !isPodOnLatestVersion(&pod, updateRevision) && !isTerminating(&pod) && !isRunningAndReady(&pod) {
			dfi.log.Info("deleting failed to start pod", "pod", pod.Name)
			if err := dfi.client.Delete(ctx, &pod); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to delete pod: %w", err)
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		ready, readyErr := dfi.isPodReady(ctx, &pod)
		if readyErr != nil {
			return ctrl.Result{}, fmt.Errorf("failed to verify pod readiness: %w", readyErr)
		}

		if !ready {
			dfi.log.Info("waiting for pod to finish startup", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		if !roleExists(&pod) {
			dfi.log.Info("waiting for pod to be assigned a role", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// verifyUpdatedReplicas checks whether the updated replicas are in a stable state.
func (dfi *DragonflyInstance) verifyUpdatedReplicas(ctx context.Context, replicas *corev1.PodList, updateRevision string) (ctrl.Result, error) {
	for _, replica := range replicas.Items {
		if isPodOnLatestVersion(&replica, updateRevision) {
			dfi.log.Info("new replica found. checking if replica had a full sync", "pod", replica.Name)

			ok, err := dfi.isReplicaStable(ctx, &replica)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to check if replica is stable: %w", err)
			}

			if !ok {
				dfi.log.Info("not all new replicas are in stable status yet", "pod", replica.Name, "reason", err)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			dfi.log.Info("replica is in stable state", "pod", replica.Name)
		}
	}

	return ctrl.Result{}, nil
}

// updateReplicas updates the replicas to the latest version
func (dfi *DragonflyInstance) updateReplicas(ctx context.Context, replicas *corev1.PodList, updateRevision string) (ctrl.Result, error) {
	_, err := dfi.getMaster(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get master before deleting replica: %w", err)
	}
	for _, replica := range replicas.Items {
		if !isPodOnLatestVersion(&replica, updateRevision) {
			dfi.log.Info("deleting replica", "pod", replica.Name)
			dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "Rollout", "Deleting replica")
			if err := dfi.client.Delete(ctx, &replica); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to delete pod: %w", err)
			}

			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// updatedMaster updates the master to the latest version
func (dfi *DragonflyInstance) updatedMaster(ctx context.Context, oldMaster *corev1.Pod, replicas *corev1.PodList, updateRevision string) error {
	if len(replicas.Items) > 0 {
		newMaster, err := getUpdatedReplica(replicas, updateRevision)
		if err != nil {
			return fmt.Errorf("failed to get updated replica: %w", err)
		}

		if err = dfi.replTakeover(ctx, newMaster, oldMaster); err != nil {
			return fmt.Errorf("failed to update master: %w", err)
		}

		for _, replica := range replicas.Items {
			if replica.Name == newMaster.Name {
				continue
			}

			ready, readyErr := dfi.isPodReady(ctx, &replica)
			if readyErr != nil {
				return fmt.Errorf("failed to verify replica readiness: %w", readyErr)
			}

			if ready {
				dfi.log.Info("configuring pod as replica to the right master", "pod", replica.Name)
				if err = dfi.configureReplica(ctx, &replica, newMaster.Status.PodIP); err != nil {
					return fmt.Errorf("failed to configure pod as replica: %w", err)
				}
			}
		}
	} else {
		// delete the old master, so that it gets recreated with the new version
		dfi.log.Info("no replicas found to run REPLTAKEOVER on. deleting master", "pod", oldMaster.Name)
		if err := dfi.client.Delete(ctx, oldMaster); err != nil {
			return fmt.Errorf("failed to delete pod: %w", err)
		}
	}

	return nil
}

// replTakeover runs the replTakeOver on the given replica pod
func (dfi *DragonflyInstance) replTakeover(ctx context.Context, newMaster *corev1.Pod, oldMaster *corev1.Pod) error {
	dfi.log.Info("running REPLTAKEOVER on replica", "pod", newMaster.Name)

	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(newMaster.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.Do(ctx, "repltakeover", "10000").Result()
	if err != nil {
		return fmt.Errorf("error running REPLTAKEOVER command: %w", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of `REPLTAKEOVER` on replica is not OK: %s", resp)
	}

	masterIp := newMaster.Status.PodIP

	newMaster.Labels[resources.RoleLabelKey] = resources.Master
	delete(newMaster.Labels, resources.MasterIpLabelKey)

	if newMaster.Annotations == nil {
		newMaster.Annotations = make(map[string]string)
	}
	newMaster.Annotations[resources.MasterIpAnnotationKey] = masterIp

	// update the label on the pod
	if err := dfi.client.Update(ctx, newMaster); err != nil {
		return fmt.Errorf("failed to update the role label on the pod: %w", err)
	}

	// delete the old master, so that it gets recreated with the new version
	dfi.log.Info("deleting master", "pod", oldMaster.Name)
	if err := dfi.client.Delete(ctx, oldMaster); err != nil {
		return fmt.Errorf("failed to delete pod: %w", err)
	}

	return nil
}

func (dfi *DragonflyInstance) reconcileCluster(ctx context.Context) (ctrl.Result, error) {
	dfi.log.Info("reconciling multi-shard cluster")

	pods, err := dfi.getPods(ctx)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list pods: %w", err)
	}

	shardPods := make(map[string][]*corev1.Pod)
	allPods := make([]*corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		pod := &pods.Items[i]
		allPods = append(allPods, pod)
		shardName, ok := pod.Labels[resources.ShardNameLabelKey]
		if !ok {
			dfi.log.Info("pod missing shard label, waiting for sync", "pod", pod.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		shardPods[shardName] = append(shardPods[shardName], pod)
	}

	for _, pods := range shardPods {
		sortPodsByName(pods)
	}
	sortPodsByName(allPods)

	replicasPerShard := dfi.df.Spec.Cluster.ReplicasPerShard
	if replicasPerShard < 1 {
		replicasPerShard = 1
	}

	// Initialize PreviousShards on first run or when cluster status doesn't exist
	if dfi.df.Status.Cluster == nil || dfi.df.Status.Cluster.PreviousShards == 0 {
		// First-time setup: initialize previousShards to current spec
		patchFrom := client.MergeFrom(dfi.df.DeepCopy())
		if dfi.df.Status.Cluster == nil {
			dfi.df.Status.Cluster = &dfv1alpha1.ClusterStatus{}
		}
		dfi.df.Status.Cluster.PreviousShards = dfi.df.Spec.Cluster.Shards
		if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to initialize previousShards: %w", err)
		}
		dfi.log.Info("initialized previousShards", "shards", dfi.df.Spec.Cluster.Shards)
	}

	// Determine effective shard count (includes scale-down pending shards)
	effectiveShards := dfi.df.Spec.Cluster.Shards
	if dfi.df.Status.Cluster != nil {
		if dfi.df.Status.Cluster.PreviousShards > effectiveShards {
			effectiveShards = dfi.df.Status.Cluster.PreviousShards
		}
		if len(dfi.df.Status.Cluster.ScaleDownPending) > 0 {
			effectiveShards = dfi.df.Status.Cluster.PreviousShards
		}
	}

	// Check if we need to handle failover (some pods are missing, unhealthy, or split-brain)
	needsFailover := false
	for i := int32(0); i < effectiveShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		pods := shardPods[shardName]

		// Check for split-brain (multiple masters)
		masters := selectMasterPods(pods)
		if len(masters) > 1 {
			dfi.log.Info("shard has split-brain, needs resolution", "shard", shardName, "masterCount", len(masters))
			needsFailover = true
			continue
		}

		// Check if master is missing or unhealthy
		currentMaster := selectMasterPod(pods)
		if currentMaster == nil || !dfi.isShardMasterHealthy(ctx, currentMaster) {
			// Check if we have at least one ready pod that could become master
			hasReadyCandidate := false
			for _, pod := range pods {
				if pod.Status.PodIP != "" && isRunningAndReady(pod) && !isTerminating(pod) {
					hasReadyCandidate = true
					break
				}
			}
			if hasReadyCandidate {
				dfi.log.Info("shard needs failover", "shard", shardName, "hasMaster", currentMaster != nil)
				needsFailover = true
			}
		}
	}

	// If no failover needed, wait for all pods to be ready before proceeding
	if !needsFailover {
		for i := int32(0); i < effectiveShards; i++ {
			shardName := fmt.Sprintf("shard-%d", i)
			if int32(len(shardPods[shardName])) != replicasPerShard {
				dfi.log.Info("shard has unexpected replica count, waiting", "shard", shardName, "expected", replicasPerShard, "actual", len(shardPods[shardName]))
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}

		for _, pod := range allPods {
			ready, err := dfi.isPodReady(ctx, pod)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to determine readiness: %w", err)
			}
			if !ready {
				dfi.log.Info("pod not ready yet", "pod", pod.Name)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			if pod.Status.PodIP == "" {
				dfi.log.Info("pod missing IP, waiting", "pod", pod.Name)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
		}
	}

	// Build map of ready pods per shard and ensure each shard has a working master
	masters := make(map[string]*corev1.Pod)
	readyShardPods := make(map[string][]*corev1.Pod)
	var readyAllPods []*corev1.Pod

	for i := int32(0); i < effectiveShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		pods := shardPods[shardName]

		// Filter to only ready pods with IPs
		var readyPods []*corev1.Pod
		for _, pod := range pods {
			if pod.Status.PodIP != "" && isRunningAndReady(pod) && !isTerminating(pod) {
				readyPods = append(readyPods, pod)
				readyAllPods = append(readyAllPods, pod)
			}
		}
		readyShardPods[shardName] = readyPods

		if len(readyPods) == 0 {
			dfi.log.Info("no ready pods in shard, waiting", "shard", shardName)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		master, err := dfi.ensureShardReplication(ctx, shardName, readyPods)
		if err != nil {
			if errors.Is(err, errMasterGracePeriod) {
				dfi.log.Info("master within grace period, requeuing", "shard", shardName)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			return ctrl.Result{}, err
		}
		masters[shardName] = master
	}

	// Handle slot migration for scaling operations
	result, handled, err := dfi.reconcileSlotMigration(ctx, masters, readyShardPods, readyAllPods)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile slot migration: %w", err)
	}
	if handled {
		return result, nil
	}

	configJSON, hash, err := dfi.buildClusterConfig(ctx, masters, readyShardPods)
	if err != nil {
		return ctrl.Result{}, err
	}

	const configApplyCooldown = 30 * time.Second

	status := dfi.getStatus()
	if status.Cluster != nil && status.Cluster.ConfigHash == hash && status.Cluster.ObservedGeneration == dfi.df.Generation {
		allConfigured, err := dfi.allNodesConfigured(ctx, readyAllPods)
		if err != nil {
			return ctrl.Result{}, err
		}
		if allConfigured {
			if err := dfi.initializeRestoreState(ctx); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to initialize restore state: %w", err)
			}

			// Check coordinated restore status before marking ready
			restoreComplete, err := dfi.reconcileClusterRestore(ctx, masters)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to check restore status: %w", err)
			}
			if !restoreComplete {
				dfi.log.Info("waiting for coordinated restore to complete")
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			// Handle coordinated backup orchestration for cluster mode
			if result, err := dfi.reconcileClusterBackup(ctx, masters); err != nil {
				dfi.log.Error(err, "failed to reconcile cluster backup")
				// Don't fail the entire reconciliation for backup errors
			} else if result.RequeueAfter > 0 {
				return result, nil
			}

			if status.Phase != PhaseReady {
				status.Phase = PhaseReady
				if err := dfi.patchStatus(ctx, status); err != nil {
					return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
				}
			}
			return ctrl.Result{}, nil
		}

		// Check cooldown: if config was applied recently, wait before reapplying
		if status.Cluster.LastConfigAppliedAt != nil {
			elapsed := time.Since(status.Cluster.LastConfigAppliedAt.Time)
			if elapsed < configApplyCooldown {
				remaining := configApplyCooldown - elapsed
				dfi.log.Info("cluster config cooldown active, requeue", "remaining", remaining, "dragonfly", dfi.df.Name)
				return ctrl.Result{RequeueAfter: remaining}, nil
			}
		}
		dfi.log.Info("cluster config missing on one or more nodes; reapplying", "dragonfly", dfi.df.Name)
	}

	status.Phase = PhaseConfiguring
	if err := dfi.patchStatus(ctx, status); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

	if err := dfi.applyClusterConfig(ctx, configJSON, readyAllPods); err != nil {
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	// Preserve existing ClusterStatus fields when updating config-related fields
	if status.Cluster == nil {
		status.Cluster = &dfv1alpha1.ClusterStatus{}
	}
	status.Cluster.ConfigHash = hash
	status.Cluster.ObservedGeneration = dfi.df.Generation
	status.Cluster.LastConfigAppliedAt = &now
	status.Phase = PhaseReady
	if err := dfi.patchStatus(ctx, status); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "ClusterConfigured", "Applied Dragonfly cluster configuration")

	return ctrl.Result{}, nil
}

func (dfi *DragonflyInstance) ensureShardReplication(ctx context.Context, shardName string, pods []*corev1.Pod) (*corev1.Pod, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pods found for shard %s", shardName)
	}

	// Detect split-brain: multiple pods labeled as master
	masters := selectMasterPods(pods)
	if len(masters) > 1 {
		dfi.log.Info("split-brain detected: multiple masters in shard", "shard", shardName, "masterCount", len(masters))
		// Resolve by picking the first master deterministically (pods are sorted by name)
		// and demoting the rest
		master, err := dfi.resolveSplitBrain(ctx, shardName, masters, pods)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve split-brain in shard %s: %w", shardName, err)
		}
		return master, nil
	}

	// Find current master (if any)
	currentMaster := selectMasterPod(pods)

	// Check if current master is healthy
	if currentMaster != nil && dfi.isShardMasterHealthy(ctx, currentMaster) {
		dfi.log.Info("shard master is healthy", "shard", shardName, "master", currentMaster.Name)
		// Ensure all replicas are configured correctly
		for _, pod := range pods {
			if pod.Name == currentMaster.Name {
				continue
			}
			if err := dfi.configureReplica(ctx, pod, currentMaster.Status.PodIP); err != nil {
				return nil, fmt.Errorf("failed to configure replica %s in shard %s: %w", pod.Name, shardName, err)
			}
		}
		return currentMaster, nil
	}

	// Master is unhealthy or doesn't exist - need to promote a new one
	const masterStabilityGrace = 10 * time.Second

	// Check grace period from CR status first (persists across pod deletion)
	// This prevents rapid failover churn even if the master pod was deleted
	if dfi.df.Status.Cluster != nil && dfi.df.Status.Cluster.ShardMasters != nil {
		if shardInfo, ok := dfi.df.Status.Cluster.ShardMasters[shardName]; ok && shardInfo.MasterSince != nil {
			masterSince := shardInfo.MasterSince.Time
			if time.Since(masterSince) < masterStabilityGrace {
				dfi.log.Info("master was recently promoted, waiting for grace period",
					"shard", shardName, "lastMaster", shardInfo.PodName, "masterSince", masterSince)
				return nil, errMasterGracePeriod
			}
		}
	}

	// Fall back to checking pod annotation if CR status not available
	if currentMaster != nil {
		// Check grace period from pod annotation
		if masterSinceStr, ok := currentMaster.Annotations[resources.MasterSinceAnnotationKey]; ok {
			masterSince, err := time.Parse(time.RFC3339, masterSinceStr)
			if err == nil && time.Since(masterSince) < masterStabilityGrace {
				dfi.log.Info("master may be transiently unhealthy, waiting for grace period",
					"shard", shardName, "master", currentMaster.Name, "masterSince", masterSince)
				return nil, errMasterGracePeriod
			}
		}
		dfi.log.Info("shard master is unhealthy, initiating failover", "shard", shardName, "master", currentMaster.Name)
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "MasterUnhealthy", fmt.Sprintf("Shard %s master %s is unhealthy, initiating failover", shardName, currentMaster.Name))
	} else {
		dfi.log.Info("no master found for shard, selecting new master", "shard", shardName)
	}

	// Select best replica candidate for promotion
	newMaster := dfi.selectBestReplicaCandidate(ctx, pods, currentMaster)
	if newMaster == nil {
		// No stable replica found - fall back to first ready pod
		for _, pod := range pods {
			if currentMaster != nil && pod.Name == currentMaster.Name {
				continue
			}
			ready, err := dfi.isPodReady(ctx, pod)
			if err == nil && ready && pod.Status.PodIP != "" {
				newMaster = pod
				dfi.log.Info("no stable replica found, falling back to first ready pod", "shard", shardName, "pod", pod.Name)
				break
			}
		}
	}

	if newMaster == nil {
		// If still no candidate, try the current master if it has an IP (might just be temporarily unreachable)
		if currentMaster != nil && currentMaster.Status.PodIP != "" {
			newMaster = currentMaster
			dfi.log.Info("no healthy replica available, keeping current master", "shard", shardName, "master", currentMaster.Name)
		} else if len(pods) > 0 && pods[0].Status.PodIP != "" {
			// Last resort: use first pod
			newMaster = pods[0]
			dfi.log.Info("no healthy candidate, using first pod as master", "shard", shardName, "pod", pods[0].Name)
		} else {
			return nil, fmt.Errorf("no suitable master candidate found for shard %s", shardName)
		}
	}

	// Attempt graceful promotion if old master is reachable
	if currentMaster != nil && currentMaster.Name != newMaster.Name && dfi.isMasterReachable(ctx, currentMaster) {
		dfi.log.Info("attempting graceful failover with REPLTAKEOVER", "shard", shardName, "oldMaster", currentMaster.Name, "newMaster", newMaster.Name)
		err := dfi.replTakeoverForShard(ctx, newMaster, currentMaster)
		if err != nil {
			dfi.log.Info("REPLTAKEOVER failed, falling back to SLAVEOF NO ONE", "shard", shardName, "err", err)
			// Fall through to force promotion
		} else {
			// Persist master promotion time to CR status for grace period tracking
			if err := dfi.updateShardMasterStatus(ctx, shardName, newMaster.Name); err != nil {
				dfi.log.Error(err, "failed to update shard master status, grace period tracking may be incomplete", "shard", shardName)
			}
			dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "MasterPromoted", fmt.Sprintf("Shard %s: promoted %s to master via REPLTAKEOVER", shardName, newMaster.Name))
			// Configure remaining pods as replicas
			for _, pod := range pods {
				if pod.Name == newMaster.Name {
					continue
				}
				// Skip the old master - it will be reconfigured when it comes back
				if pod.Name == currentMaster.Name {
					continue
				}
				if err := dfi.configureReplica(ctx, pod, newMaster.Status.PodIP); err != nil {
					return nil, fmt.Errorf("failed to configure replica %s in shard %s: %w", pod.Name, shardName, err)
				}
			}
			return newMaster, nil
		}
	}

	// Force promotion via SLAVEOF NO ONE
	dfi.log.Info("promoting new master via SLAVEOF NO ONE", "shard", shardName, "newMaster", newMaster.Name)
	if err := dfi.replicaOfNoOne(ctx, newMaster); err != nil {
		return nil, fmt.Errorf("failed to promote master in shard %s: %w", shardName, err)
	}

	// Persist master promotion time to CR status for grace period tracking
	if err := dfi.updateShardMasterStatus(ctx, shardName, newMaster.Name); err != nil {
		dfi.log.Error(err, "failed to update shard master status, grace period tracking may be incomplete", "shard", shardName)
		// Don't fail the operation, just log the error
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "MasterPromoted", fmt.Sprintf("Shard %s: promoted %s to master", shardName, newMaster.Name))

	// Configure all other pods as replicas
	for _, pod := range pods {
		if pod.Name == newMaster.Name {
			continue
		}
		if err := dfi.configureReplica(ctx, pod, newMaster.Status.PodIP); err != nil {
			return nil, fmt.Errorf("failed to configure replica %s in shard %s: %w", pod.Name, shardName, err)
		}
	}

	return newMaster, nil
}

// resolveSplitBrain handles the case where multiple pods are labeled as master.
// It picks the first healthy master deterministically and demotes the rest.
func (dfi *DragonflyInstance) resolveSplitBrain(ctx context.Context, shardName string, masters []*corev1.Pod, allPods []*corev1.Pod) (*corev1.Pod, error) {
	// Sort masters by name for deterministic selection
	sortPodsByName(masters)

	// Find first healthy master
	var electedMaster *corev1.Pod
	for _, master := range masters {
		if dfi.isShardMasterHealthy(ctx, master) {
			electedMaster = master
			break
		}
	}

	// If no healthy master, pick the first one with an IP
	if electedMaster == nil {
		for _, master := range masters {
			if master.Status.PodIP != "" {
				electedMaster = master
				break
			}
		}
	}

	if electedMaster == nil {
		return nil, fmt.Errorf("no viable master found during split-brain resolution for shard %s", shardName)
	}

	dfi.log.Info("resolved split-brain, elected master", "shard", shardName, "electedMaster", electedMaster.Name)
	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "SplitBrainResolved", fmt.Sprintf("Shard %s: resolved split-brain, elected %s as master", shardName, electedMaster.Name))

	// Ensure elected master is actually master
	if err := dfi.replicaOfNoOne(ctx, electedMaster); err != nil {
		return nil, fmt.Errorf("failed to confirm master in shard %s: %w", shardName, err)
	}

	// Persist master promotion time to CR status for grace period tracking
	if err := dfi.updateShardMasterStatus(ctx, shardName, electedMaster.Name); err != nil {
		dfi.log.Error(err, "failed to update shard master status, grace period tracking may be incomplete", "shard", shardName)
	}

	// Demote all other pods to replicas
	for _, pod := range allPods {
		if pod.Name == electedMaster.Name {
			continue
		}
		if err := dfi.configureReplica(ctx, pod, electedMaster.Status.PodIP); err != nil {
			return nil, fmt.Errorf("failed to demote %s to replica in shard %s: %w", pod.Name, shardName, err)
		}
	}

	return electedMaster, nil
}

// replTakeoverForShard performs a REPLTAKEOVER for shard failover.
// Unlike the rolling update version, this doesn't delete the old master pod.
func (dfi *DragonflyInstance) replTakeoverForShard(ctx context.Context, newMaster *corev1.Pod, oldMaster *corev1.Pod) error {
	dfi.log.Info("running REPLTAKEOVER for shard failover", "newMaster", newMaster.Name, "oldMaster", oldMaster.Name)

	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(newMaster.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.Do(ctx, "repltakeover", "10000").Result()
	if err != nil {
		return fmt.Errorf("error running REPLTAKEOVER command: %w", err)
	}

	if resp != "OK" {
		return fmt.Errorf("response of REPLTAKEOVER is not OK: %s", resp)
	}

	// Update labels on new master
	masterIp := newMaster.Status.PodIP
	patchFrom := client.MergeFrom(newMaster.DeepCopy())
	newMaster.Labels[resources.RoleLabelKey] = resources.Master
	delete(newMaster.Labels, resources.MasterIpLabelKey)
	if newMaster.Annotations == nil {
		newMaster.Annotations = make(map[string]string)
	}
	newMaster.Annotations[resources.MasterIpAnnotationKey] = masterIp

	if err := dfi.client.Patch(ctx, newMaster, patchFrom); err != nil {
		return fmt.Errorf("failed to update labels on new master: %w", err)
	}

	// Update labels on old master to replica
	patchFromOld := client.MergeFrom(oldMaster.DeepCopy())
	oldMaster.Labels[resources.RoleLabelKey] = resources.Replica
	oldMaster.Labels[resources.MasterIpLabelKey] = masterIp
	if oldMaster.Annotations == nil {
		oldMaster.Annotations = make(map[string]string)
	}
	oldMaster.Annotations[resources.MasterIpAnnotationKey] = masterIp

	if err := dfi.client.Patch(ctx, oldMaster, patchFromOld); err != nil {
		dfi.log.Info("failed to update labels on old master, will be fixed on next reconcile", "pod", oldMaster.Name, "err", err)
	}

	return nil
}

func selectMasterPod(pods []*corev1.Pod) *corev1.Pod {
	for _, pod := range pods {
		if role, ok := pod.Labels[resources.RoleLabelKey]; ok && role == resources.Master {
			return pod
		}
	}
	return nil
}

// selectMasterPods returns all pods labeled as master in the given slice.
// Used to detect split-brain scenarios.
func selectMasterPods(pods []*corev1.Pod) []*corev1.Pod {
	var masters []*corev1.Pod
	for _, pod := range pods {
		if role, ok := pod.Labels[resources.RoleLabelKey]; ok && role == resources.Master {
			masters = append(masters, pod)
		}
	}
	return masters
}

func sortPodsByName(pods []*corev1.Pod) {
	sort.Slice(pods, func(i, j int) bool {
		return pods[i].Name < pods[j].Name
	})
}

// isShardMasterHealthy checks if the given master pod is healthy:
// - Kubernetes ready
// - Admin port reachable
// - Actually has master role in Redis
func (dfi *DragonflyInstance) isShardMasterHealthy(ctx context.Context, master *corev1.Pod) bool {
	if master == nil || master.Status.PodIP == "" {
		return false
	}

	// Check Kubernetes readiness
	ready, err := dfi.isPodReady(ctx, master)
	if err != nil || !ready {
		dfi.log.Info("shard master not ready", "pod", master.Name, "err", err)
		return false
	}

	// Check admin port reachability and Redis role
	client := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(master.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	// Ping to verify connectivity
	if _, err := client.Ping(ctx).Result(); err != nil {
		dfi.log.Info("shard master unreachable", "pod", master.Name, "err", err)
		return false
	}

	// Verify Redis role is master
	info, err := client.Info(ctx, "replication").Result()
	if err != nil {
		dfi.log.Info("failed to get replication info from master", "pod", master.Name, "err", err)
		return false
	}

	replicationData := parseInfoToMap(info)
	role, ok := replicationData["role"]
	if !ok || role != "master" {
		dfi.log.Info("shard master has incorrect Redis role", "pod", master.Name, "role", role)
		return false
	}

	return true
}

// isMasterReachable checks if the master pod is reachable via admin port.
func (dfi *DragonflyInstance) isMasterReachable(ctx context.Context, master *corev1.Pod) bool {
	if master == nil || master.Status.PodIP == "" {
		return false
	}

	client := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(master.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	_, err := client.Ping(ctx).Result()
	return err == nil
}

// selectBestReplicaCandidate selects the best replica for promotion from a list of pods.
// Selection criteria:
// 1. Must be Kubernetes ready
// 2. Must have stable replication state (isReplicaStable)
// 3. Deterministic ordering by pod name (first ready+stable wins)
// Returns nil if no suitable candidate found.
func (dfi *DragonflyInstance) selectBestReplicaCandidate(ctx context.Context, pods []*corev1.Pod, excludeMaster *corev1.Pod) *corev1.Pod {
	// Pods should already be sorted by name for deterministic selection
	for _, pod := range pods {
		// Skip current master
		if excludeMaster != nil && pod.Name == excludeMaster.Name {
			continue
		}

		// Check Kubernetes readiness
		ready, err := dfi.isPodReady(ctx, pod)
		if err != nil || !ready {
			dfi.log.Info("replica candidate not ready", "pod", pod.Name, "err", err)
			continue
		}

		// Check if pod has IP
		if pod.Status.PodIP == "" {
			dfi.log.Info("replica candidate has no IP", "pod", pod.Name)
			continue
		}

		// Check replication stability
		stable, err := dfi.isReplicaStable(ctx, pod)
		if err != nil {
			dfi.log.Info("failed to check replica stability", "pod", pod.Name, "err", err)
			continue
		}
		if !stable {
			dfi.log.Info("replica candidate not stable", "pod", pod.Name)
			continue
		}

		dfi.log.Info("selected replica candidate for promotion", "pod", pod.Name)
		return pod
	}

	return nil
}

// clusterMigrationTarget specifies where slots should migrate to.
// This is used in the DFLYCLUSTER CONFIG payload to trigger slot migration.
type clusterMigrationTarget struct {
	SlotRanges []dfv1alpha1.SlotRange `json:"slot_ranges"`
	NodeID     string                 `json:"node_id"`
	IP         string                 `json:"ip"`
	Port       int                    `json:"port"`
}

type clusterNodeAddress struct {
	SlotRanges []dfv1alpha1.SlotRange `json:"slot_ranges"`
	Master     *clusterNode           `json:"master"`
	Replicas   []clusterNode          `json:"replicas"`
	// Migrations specifies slots to migrate OUT from this node.
	// When set, Dragonfly will initiate slot transfer to the specified targets.
	Migrations []clusterMigrationTarget `json:"migrations,omitempty"`
}

type clusterNode struct {
	ID   string `json:"id"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type clusterConfig struct {
	Ver   int                  `json:"ver"`
	Nodes []clusterNodeAddress `json:"nodes"`
}

const totalSlots = 16384

// computeSlotRanges distributes the 16384 hash slots evenly across all shards.
// Returns a map from shard name (shard-0, shard-1, etc.) to the computed slot range.
func computeSlotRanges(numShards int32) map[string][]dfv1alpha1.SlotRange {
	result := make(map[string][]dfv1alpha1.SlotRange)
	if numShards == 0 {
		return result
	}

	slotsPerShard := int32(totalSlots) / numShards
	extraSlots := int32(totalSlots) % numShards

	var currentSlot int32 = 0
	for i := int32(0); i < numShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		slotCount := slotsPerShard
		if i < extraSlots {
			slotCount++
		}
		endSlot := currentSlot + slotCount - 1
		result[shardName] = []dfv1alpha1.SlotRange{{Start: currentSlot, End: endSlot}}
		currentSlot = endSlot + 1
	}

	return result
}

func (dfi *DragonflyInstance) buildClusterConfig(ctx context.Context, masters map[string]*corev1.Pod, shardPods map[string][]*corev1.Pod) (string, string, error) {
	var nodes []clusterNodeAddress

	// Compute slot ranges for all shards
	slotRanges := computeSlotRanges(dfi.df.Spec.Cluster.Shards)

	for i := int32(0); i < dfi.df.Spec.Cluster.Shards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		master := masters[shardName]
		if master == nil {
			return "", "", fmt.Errorf("shard %s has no master", shardName)
		}

		masterNode, err := dfi.getClusterNodeInfo(ctx, master)
		if err != nil {
			return "", "", fmt.Errorf("failed to fetch master id for shard %s: %w", shardName, err)
		}

		replicas := []clusterNode{}
		for _, pod := range shardPods[shardName] {
			if pod.Name == master.Name {
				continue
			}

			node, err := dfi.getClusterNodeInfo(ctx, pod)
			if err != nil {
				return "", "", fmt.Errorf("failed to fetch replica id for shard %s: %w", shardName, err)
			}
			replicas = append(replicas, node)
		}

		nodes = append(nodes, clusterNodeAddress{
			SlotRanges: slotRanges[shardName],
			Master:     &masterNode,
			Replicas:   replicas,
		})
	}

	payload, err := json.Marshal(nodes)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal cluster config: %w", err)
	}

	hash := sha256.Sum256(payload)
	dfi.log.Info("generated cluster config", "config", string(payload))
	return string(payload), hex.EncodeToString(hash[:]), nil
}

func (dfi *DragonflyInstance) applyClusterConfig(ctx context.Context, config string, pods []*corev1.Pod) error {
	for _, pod := range pods {
		podName := pod.Name
		podIP := pod.Status.PodIP
		err := retryWithBackoff(ctx, 3, time.Second, func() error {
			redisClient := redis.NewClient(&redis.Options{
				ClientName: resources.DragonflyOperatorName,
				Addr:       net.JoinHostPort(podIP, strconv.Itoa(int(dfi.adminPort()))),
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode: maintnotifications.ModeDisabled,
				},
			})
			defer redisClient.Close()

			if _, err := redisClient.Do(ctx, "DFLYCLUSTER", "CONFIG", config).Result(); err != nil {
				dfi.log.Info("retrying DFLYCLUSTER CONFIG", "pod", podName, "err", err)
				return err
			}
			return nil
		})
		if err != nil {
			dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "FailedToApplyClusterConfig",
				fmt.Sprintf("Failed to apply cluster config on pod %s after retries: %v", podName, err))
			return fmt.Errorf("failed to apply cluster config on pod %s after retries: %w", podName, err)
		}
	}

	return nil
}

func (dfi *DragonflyInstance) allNodesConfigured(ctx context.Context, pods []*corev1.Pod) (bool, error) {
	for _, pod := range pods {
		configured, err := dfi.isNodeConfigured(ctx, pod)
		if err != nil {
			return false, fmt.Errorf("failed to check cluster config on pod %s: %w", pod.Name, err)
		}
		if !configured {
			return false, nil
		}
	}
	return true, nil
}

func (dfi *DragonflyInstance) isNodeConfigured(ctx context.Context, pod *corev1.Pod) (bool, error) {
	client := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	if _, err := client.Do(ctx, "CLUSTER", "SHARDS").Result(); err != nil {
		if strings.Contains(err.Error(), "Cluster is not yet configured") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (dfi *DragonflyInstance) getClusterNodeInfo(ctx context.Context, pod *corev1.Pod) (clusterNode, error) {
	podName := pod.Name
	podIP := pod.Status.PodIP
	var nodeID string

	err := retryWithBackoff(ctx, 2, 500*time.Millisecond, func() error {
		client := redis.NewClient(&redis.Options{
			ClientName: resources.DragonflyOperatorName,
			Addr:       net.JoinHostPort(podIP, strconv.Itoa(int(dfi.adminPort()))),
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		})
		defer client.Close()

		id, err := client.Do(ctx, "CLUSTER", "MYID").Text()
		if err != nil {
			dfi.log.Info("retrying CLUSTER MYID", "pod", podName, "err", err)
			return err
		}
		nodeID = id
		return nil
	})
	if err != nil {
		return clusterNode{}, fmt.Errorf("failed to get cluster node info after retries: %w", err)
	}

	return clusterNode{
		ID:   nodeID,
		IP:   podIP,
		Port: resources.DragonflyPort,
	}, nil
}

// detectScaleChange compares spec.cluster.shards with status.cluster.previousShards
// to detect if scaling is needed. Returns the scale direction and delta.
func (dfi *DragonflyInstance) detectScaleChange() (scaleUp bool, scaleDown bool, delta int32) {
	if dfi.df.Spec.Cluster == nil {
		return false, false, 0
	}

	specShards := dfi.df.Spec.Cluster.Shards
	previousShards := int32(0)

	if dfi.df.Status.Cluster != nil && dfi.df.Status.Cluster.PreviousShards > 0 {
		previousShards = dfi.df.Status.Cluster.PreviousShards
	}

	// If previousShards is 0, this is likely initial deployment - no scaling needed
	if previousShards == 0 {
		return false, false, 0
	}

	if specShards > previousShards {
		return true, false, specShards - previousShards
	}
	if specShards < previousShards {
		return false, true, previousShards - specShards
	}
	return false, false, 0
}

// MigrationPlan represents a computed slot migration plan.
type MigrationPlan struct {
	// Migrations is a list of slot migrations to perform.
	Migrations []dfv1alpha1.SlotMigration
	// IsScaleUp indicates if this is a scale-up operation.
	IsScaleUp bool
	// IsScaleDown indicates if this is a scale-down operation.
	IsScaleDown bool
}

// computeMigrationPlan calculates which slots need to move between which shards
// when scaling from previousShards to targetShards.
// For scale-up: slots are redistributed from existing shards to ALL shards (including existing ones).
// For scale-down: slots from removed shards are redistributed to remaining shards.
func computeMigrationPlan(previousShards, targetShards int32) MigrationPlan {
	plan := MigrationPlan{}

	if previousShards == targetShards || previousShards == 0 {
		return plan
	}

	// Compute current slot distribution (before scaling)
	currentSlots := computeSlotRanges(previousShards)
	// Compute target slot distribution (after scaling)
	targetSlots := computeSlotRanges(targetShards)

	if targetShards > previousShards {
		plan.IsScaleUp = true
	} else if targetShards < previousShards {
		plan.IsScaleDown = true
	}

	// For each current shard, check which slots it currently owns
	// that need to move to a different shard in the target distribution.
	for i := int32(0); i < previousShards; i++ {
		sourceShard := fmt.Sprintf("shard-%d", i)
		currentRange := currentSlots[sourceShard][0]

		// Check against ALL target shards to see if any need slots from this source
		for j := int32(0); j < targetShards; j++ {
			if i == j {
				// Skip self - no migration needed to the same shard
				continue
			}

			targetShard := fmt.Sprintf("shard-%d", j)
			targetShardRange := targetSlots[targetShard][0]

			// Check if this source currently owns slots that the target should own
			if targetShardRange.Start > currentRange.End || targetShardRange.End < currentRange.Start {
				continue
			}

			// Calculate the overlap - these are slots source has that target needs
			migrateStart := targetShardRange.Start
			migrateEnd := targetShardRange.End

			// Clamp to what source actually owns
			if migrateStart < currentRange.Start {
				migrateStart = currentRange.Start
			}
			if migrateEnd > currentRange.End {
				migrateEnd = currentRange.End
			}

			if migrateStart <= migrateEnd {
				plan.Migrations = append(plan.Migrations, dfv1alpha1.SlotMigration{
					SourceShard: sourceShard,
					TargetShard: targetShard,
					SlotRanges:  []dfv1alpha1.SlotRange{{Start: migrateStart, End: migrateEnd}},
					Status:      dfv1alpha1.MigrationStatePending,
				})
			}
		}
	}

	return plan
}

func validateScaleDownMasters(masters map[string]*corev1.Pod, previousShards, targetShards int32) error {
	if targetShards >= previousShards {
		return nil
	}
	for i := targetShards; i < previousShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		if masters[shardName] == nil {
			return fmt.Errorf("missing master for draining shard %s", shardName)
		}
	}
	return nil
}

// buildClusterConfigWithMigrations builds a cluster config JSON payload that includes
// migration directives to trigger slot transfers between nodes.
func (dfi *DragonflyInstance) buildClusterConfigWithMigrations(
	ctx context.Context,
	masters map[string]*corev1.Pod,
	shardPods map[string][]*corev1.Pod,
	migrations []dfv1alpha1.SlotMigration,
	targetShards int32,
) (string, string, error) {
	var nodes []clusterNodeAddress

	// Build a map of target shard -> master node info for migration targets
	targetNodes := make(map[string]clusterNode)
	for shardName, master := range masters {
		if master != nil {
			nodeInfo, err := dfi.getClusterNodeInfo(ctx, master)
			if err != nil {
				dfi.log.Info("failed to get node info for migration target", "shard", shardName, "err", err)
				continue
			}
			targetNodes[shardName] = nodeInfo
		}
	}

	// Build migration lookup: source shard -> list of migration targets
	migrationsBySource := make(map[string][]clusterMigrationTarget)
	for _, mig := range migrations {
		if mig.Status != dfv1alpha1.MigrationStatePending && mig.Status != dfv1alpha1.MigrationStateSyncing {
			continue // Skip finished or failed migrations
		}
		targetNode, ok := targetNodes[mig.TargetShard]
		if !ok {
			dfi.log.Info("migration target shard not found", "targetShard", mig.TargetShard)
			continue
		}
		migrationsBySource[mig.SourceShard] = append(migrationsBySource[mig.SourceShard], clusterMigrationTarget{
			SlotRanges: mig.SlotRanges,
			NodeID:     targetNode.ID,
			IP:         targetNode.IP,
			Port:       resources.DragonflyPort, // Must use data port, not admin port
		})
	}

	// Compute current slot ranges (use max of current and target to include all shards)
	currentShards := dfi.df.Status.Cluster.PreviousShards
	if currentShards == 0 {
		currentShards = dfi.df.Spec.Cluster.Shards
	}
	maxShards := currentShards
	if targetShards > maxShards {
		maxShards = targetShards
	}
	slotRanges := computeSlotRanges(currentShards)

	for i := int32(0); i < maxShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		master := masters[shardName]
		if master == nil {
			if i < currentShards {
				return "", "", fmt.Errorf("shard %s has no master", shardName)
			}
			// For new shards during scale-up, they start with empty slot ranges
			if i >= currentShards {
				// New shard - include in config but with empty slots (will receive via migration)
				if masterPod, exists := masters[shardName]; exists && masterPod != nil {
					masterNode, err := dfi.getClusterNodeInfo(ctx, masterPod)
					if err != nil {
						return "", "", fmt.Errorf("failed to fetch master id for new shard %s: %w", shardName, err)
					}
					nodes = append(nodes, clusterNodeAddress{
						SlotRanges: []dfv1alpha1.SlotRange{}, // Empty - will receive slots via migration
						Master:     &masterNode,
						Replicas:   []clusterNode{},
					})
				}
			}
			continue
		}

		masterNode, err := dfi.getClusterNodeInfo(ctx, master)
		if err != nil {
			return "", "", fmt.Errorf("failed to fetch master id for shard %s: %w", shardName, err)
		}

		replicas := []clusterNode{}
		for _, pod := range shardPods[shardName] {
			if pod.Name == master.Name {
				continue
			}
			node, err := dfi.getClusterNodeInfo(ctx, pod)
			if err != nil {
				return "", "", fmt.Errorf("failed to fetch replica id for shard %s: %w", shardName, err)
			}
			replicas = append(replicas, node)
		}

		ranges := slotRanges[shardName]
		if ranges == nil {
			ranges = []dfv1alpha1.SlotRange{}
		}

		nodeAddr := clusterNodeAddress{
			SlotRanges: ranges,
			Master:     &masterNode,
			Replicas:   replicas,
		}

		// Add migrations if this shard is a source
		if migs, ok := migrationsBySource[shardName]; ok {
			nodeAddr.Migrations = migs
		}

		nodes = append(nodes, nodeAddr)
	}

	payload, err := json.Marshal(nodes)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal cluster config with migrations: %w", err)
	}

	hash := sha256.Sum256(payload)
	dfi.log.Info("generated cluster config with migrations", "config", string(payload))
	return string(payload), hex.EncodeToString(hash[:]), nil
}

// checkMigrationStatus queries DFLYCLUSTER SLOT-MIGRATION-STATUS on a pod to get
// the current status of active migrations.
// The response is an array of status strings, one per migration.
func (dfi *DragonflyInstance) checkMigrationStatus(ctx context.Context, pod *corev1.Pod) (string, error) {
	podIP := pod.Status.PodIP
	var status string

	err := retryWithBackoff(ctx, 2, 500*time.Millisecond, func() error {
		client := redis.NewClient(&redis.Options{
			ClientName: resources.DragonflyOperatorName,
			Addr:       net.JoinHostPort(podIP, strconv.Itoa(int(dfi.adminPort()))),
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
		})
		defer client.Close()

		result, err := client.Do(ctx, "DFLYCLUSTER", "SLOT-MIGRATION-STATUS").Result()
		if err != nil {
			dfi.log.Info("retrying DFLYCLUSTER SLOT-MIGRATION-STATUS", "pod", pod.Name, "err", err)
			return err
		}

		status = parseMigrationStatus(result)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to get migration status after retries: %w", err)
	}

	return status, nil
}

func parseMigrationStatus(result interface{}) string {
	if result == nil {
		return ""
	}

	switch v := result.(type) {
	case []interface{}:
		if len(v) == 0 {
			return ""
		}
		statuses := make([]string, 0, len(v))
		for _, item := range v {
			switch t := item.(type) {
			case string:
				statuses = append(statuses, t)
			case []byte:
				statuses = append(statuses, string(t))
			case fmt.Stringer:
				statuses = append(statuses, t.String())
			default:
				statuses = append(statuses, fmt.Sprintf("%v", item))
			}
		}
		return strings.Join(statuses, "; ")
	case string:
		return v
	case []byte:
		return string(v)
	case fmt.Stringer:
		return v.String()
	default:
		return fmt.Sprintf("%v", result)
	}
}

// checkAllMigrationsFinished checks if all active migrations have completed.
// It queries the migration status on TARGET shards (the ones receiving slots).
// According to Dragonfly's design, SLOT-MIGRATION-STATUS is queried on the target node.
func (dfi *DragonflyInstance) checkAllMigrationsFinished(ctx context.Context, masters map[string]*corev1.Pod) (bool, error) {
	if dfi.df.Status.Cluster == nil || len(dfi.df.Status.Cluster.ActiveMigrations) == 0 {
		return true, nil
	}

	// Get unique target shards - we check migration status on the TARGET nodes
	targetShards := make(map[string]bool)
	for _, mig := range dfi.df.Status.Cluster.ActiveMigrations {
		if mig.Status == dfv1alpha1.MigrationStateSyncing || mig.Status == dfv1alpha1.MigrationStatePending {
			targetShards[mig.TargetShard] = true
		}
	}

	for shardName := range targetShards {
		master := masters[shardName]
		if master == nil {
			dfi.log.Info("target shard master not found for migration check", "shard", shardName)
			// For scale-up, target might be a new shard that just came up
			return false, nil // Not finished yet
		}

		status, err := dfi.checkMigrationStatus(ctx, master)
		if err != nil {
			dfi.log.Info("failed to check migration status on target", "shard", shardName, "err", err)
			return false, nil // Assume not finished if we can't check
		}

		dfi.log.Info("migration status on target", "shard", shardName, "status", status)

		// Parse the status - we check the TARGET node
		// If status is empty or contains NO_MIGRATIONS, the migration hasn't started on this target
		// If it contains FINISHED, this target's migration is complete
		// Otherwise, migration is still in progress
		if status == "" {
			// No status means migration hasn't started yet - not finished
			return false, nil
		}
		if strings.Contains(status, "FATAL") {
			dfi.log.Error(nil, "migration failed on target", "shard", shardName, "status", status)
			return false, fmt.Errorf("migration failed on target %s: %s", shardName, status)
		}
		if !strings.Contains(status, "FINISHED") {
			// Migration in progress
			return false, nil
		}
	}

	return true, nil
}

// updateMigrationStatuses updates the status of active migrations based on the
// current migration status from Dragonfly TARGET nodes.
func (dfi *DragonflyInstance) updateMigrationStatuses(ctx context.Context, masters map[string]*corev1.Pod) error {
	if dfi.df.Status.Cluster == nil || len(dfi.df.Status.Cluster.ActiveMigrations) == 0 {
		return nil
	}

	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	updated := false

	for i := range dfi.df.Status.Cluster.ActiveMigrations {
		mig := &dfi.df.Status.Cluster.ActiveMigrations[i]
		if mig.Status == dfv1alpha1.MigrationStateFinished || mig.Status == dfv1alpha1.MigrationStateFailed {
			continue
		}

		// Check migration status on the TARGET shard (the one receiving slots)
		targetMaster := masters[mig.TargetShard]
		if targetMaster == nil {
			dfi.log.Info("target shard master not found for status update", "shard", mig.TargetShard)
			continue
		}

		status, err := dfi.checkMigrationStatus(ctx, targetMaster)
		if err != nil {
			dfi.log.Info("failed to check migration status on target for update", "shard", mig.TargetShard, "err", err)
			continue
		}

		// Update status based on response from TARGET node
		if strings.Contains(status, "FATAL") {
			mig.Status = dfv1alpha1.MigrationStateFailed
			updated = true
		} else if strings.Contains(status, "FINISHED") {
			mig.Status = dfv1alpha1.MigrationStateFinished
			updated = true
		} else if status != "" && mig.Status == dfv1alpha1.MigrationStatePending {
			// Migration has started (non-empty status that's not FINISHED)
			mig.Status = dfv1alpha1.MigrationStateSyncing
			updated = true
		}
	}

	if updated {
		if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
			return fmt.Errorf("failed to update migration statuses: %w", err)
		}
	}

	return nil
}

// cleanupDrainedShards deletes StatefulSets and Services for shards that have been
// fully drained during scale-down.
func (dfi *DragonflyInstance) cleanupDrainedShards(ctx context.Context, shardNames []string) error {
	for _, shardName := range shardNames {
		statefulSetName := fmt.Sprintf("%s-%s", dfi.df.Name, shardName)
		serviceName := fmt.Sprintf("%s-%s-headless", dfi.df.Name, shardName)
		pdbName := fmt.Sprintf("%s-%s", dfi.df.Name, shardName)

		// Delete StatefulSet
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      statefulSetName,
				Namespace: dfi.df.Namespace,
			},
		}
		if err := dfi.client.Delete(ctx, sts); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete StatefulSet %s: %w", statefulSetName, err)
		}
		dfi.log.Info("deleted StatefulSet for drained shard", "statefulSet", statefulSetName)

		// Delete headless Service
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      serviceName,
				Namespace: dfi.df.Namespace,
			},
		}
		if err := dfi.client.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Service %s: %w", serviceName, err)
		}
		dfi.log.Info("deleted Service for drained shard", "service", serviceName)

		// Delete PDB if exists
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pdbName,
				Namespace: dfi.df.Namespace,
			},
		}
		if err := dfi.client.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
			dfi.log.Info("failed to delete PDB (may not exist)", "pdb", pdbName, "err", err)
		}
	}

	return nil
}

// reconcileSlotMigration handles slot migration during cluster scaling.
// It implements the migration state machine:
// 1. Detect scale change
// 2. Compute migration plan
// 3. Apply config with migrations
// 4. Monitor migration progress
// 5. Apply final config (scale-up) or cleanup resources (scale-down)
func (dfi *DragonflyInstance) reconcileSlotMigration(ctx context.Context, masters map[string]*corev1.Pod, shardPods map[string][]*corev1.Pod, allPods []*corev1.Pod) (ctrl.Result, bool, error) {
	status := dfi.getStatus()

	// Check if there are active migrations in progress
	if status.Cluster != nil && len(status.Cluster.ActiveMigrations) > 0 {
		return dfi.handleActiveMigrations(ctx, masters, shardPods, allPods)
	}

	// Check if there are pending scale-down cleanups
	if status.Cluster != nil && len(status.Cluster.ScaleDownPending) > 0 {
		return dfi.handleScaleDownCleanup(ctx, masters, shardPods, allPods)
	}

	// Detect if scaling is needed
	scaleUp, scaleDown, delta := dfi.detectScaleChange()
	if !scaleUp && !scaleDown {
		return ctrl.Result{}, false, nil // No scaling needed
	}

	dfi.log.Info("scale change detected", "scaleUp", scaleUp, "scaleDown", scaleDown, "delta", delta)

	previousShards := status.Cluster.PreviousShards
	targetShards := dfi.df.Spec.Cluster.Shards

	// Compute migration plan
	plan := computeMigrationPlan(previousShards, targetShards)
	if len(plan.Migrations) == 0 {
		dfi.log.Info("no migrations needed for scaling")
		return ctrl.Result{}, false, nil
	}

	dfi.log.Info("computed migration plan", "migrations", len(plan.Migrations))

	// For scale-up: wait for new shard pods to be ready before starting migration
	if scaleUp {
		for i := previousShards; i < targetShards; i++ {
			shardName := fmt.Sprintf("shard-%d", i)
			pods := shardPods[shardName]
			if len(pods) == 0 {
				dfi.log.Info("waiting for new shard pods to be created", "shard", shardName)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
			}
			for _, pod := range pods {
				ready, err := dfi.isPodReady(ctx, pod)
				if err != nil || !ready {
					dfi.log.Info("waiting for new shard pod to be ready", "shard", shardName, "pod", pod.Name)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
				}
			}
		}
	}

	// For scale-down: mark shards as pending deletion
	if scaleDown {
		pendingShards := make([]string, 0, delta)
		for i := targetShards; i < previousShards; i++ {
			pendingShards = append(pendingShards, fmt.Sprintf("shard-%d", i))
		}

		patchFrom := client.MergeFrom(dfi.df.DeepCopy())
		if dfi.df.Status.Cluster == nil {
			dfi.df.Status.Cluster = &dfv1alpha1.ClusterStatus{}
		}
		dfi.df.Status.Cluster.ScaleDownPending = pendingShards
		if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("failed to set scale-down pending shards: %w", err)
		}
		dfi.log.Info("marked shards as scale-down pending", "shards", pendingShards)
	}

	if scaleDown {
		if err := validateScaleDownMasters(masters, previousShards, targetShards); err != nil {
			dfi.log.Info("waiting for draining shard masters", "err", err)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
		}
	}

	// Build and apply config with migrations
	configJSON, _, err := dfi.buildClusterConfigWithMigrations(ctx, masters, shardPods, plan.Migrations, targetShards)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to build cluster config with migrations: %w", err)
	}

	if err := dfi.applyClusterConfig(ctx, configJSON, allPods); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to apply cluster config with migrations: %w", err)
	}

	dfi.log.Info("applied cluster config with migrations")

	// Set migration timestamps and update status only after config is applied
	now := metav1.Now()
	for i := range plan.Migrations {
		plan.Migrations[i].StartedAt = &now
	}

	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	if dfi.df.Status.Cluster == nil {
		dfi.df.Status.Cluster = &dfv1alpha1.ClusterStatus{}
	}
	dfi.df.Status.Cluster.ActiveMigrations = plan.Migrations
	dfi.df.Status.Phase = PhaseConfiguring
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to set active migrations: %w", err)
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "SlotMigrationStarted",
		fmt.Sprintf("Starting slot migration: %d migrations planned", len(plan.Migrations)))

	// Requeue to monitor migration progress
	return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
}

// handleActiveMigrations monitors and handles active migrations.
func (dfi *DragonflyInstance) handleActiveMigrations(ctx context.Context, masters map[string]*corev1.Pod, shardPods map[string][]*corev1.Pod, allPods []*corev1.Pod) (ctrl.Result, bool, error) {
	// Update migration statuses
	if err := dfi.updateMigrationStatuses(ctx, masters); err != nil {
		dfi.log.Info("failed to update migration statuses", "err", err)
	}

	// Check if all migrations are finished
	finished, err := dfi.checkAllMigrationsFinished(ctx, masters)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to check migration status: %w", err)
	}

	if !finished {
		dfi.log.Info("migrations still in progress")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, true, nil
	}

	dfi.log.Info("all migrations finished")
	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "SlotMigrationFinished", "All slot migrations completed successfully")

	// Clear active migrations and apply final config
	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	dfi.df.Status.Cluster.ActiveMigrations = nil

	// Check if this was a scale-down (we have pending shards to clean up)
	if len(dfi.df.Status.Cluster.ScaleDownPending) > 0 {
		// Don't update PreviousShards yet - we still need to clean up
		if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("failed to clear active migrations: %w", err)
		}
		// Continue to cleanup phase
		return ctrl.Result{RequeueAfter: 1 * time.Second}, true, nil
	}

	// Scale-up: update previous shards and apply final config
	dfi.df.Status.Cluster.PreviousShards = dfi.df.Spec.Cluster.Shards
	dfi.df.Status.Phase = PhaseReady
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to update status after migration: %w", err)
	}

	// Build and apply final config without migrations
	configJSON, hash, err := dfi.buildClusterConfig(ctx, masters, shardPods)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to build final cluster config: %w", err)
	}

	if err := dfi.applyClusterConfig(ctx, configJSON, allPods); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to apply final cluster config: %w", err)
	}

	// Update config hash
	patchFrom = client.MergeFrom(dfi.df.DeepCopy())
	now := metav1.Now()
	dfi.df.Status.Cluster.ConfigHash = hash
	dfi.df.Status.Cluster.ObservedGeneration = dfi.df.Generation
	dfi.df.Status.Cluster.LastConfigAppliedAt = &now
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to update config hash: %w", err)
	}

	return ctrl.Result{}, false, nil
}

// handleScaleDownCleanup handles the final cleanup phase of scale-down operations.
func (dfi *DragonflyInstance) handleScaleDownCleanup(ctx context.Context, masters map[string]*corev1.Pod, shardPods map[string][]*corev1.Pod, allPods []*corev1.Pod) (ctrl.Result, bool, error) {
	pendingShards := dfi.df.Status.Cluster.ScaleDownPending
	dfi.log.Info("cleaning up drained shards", "shards", pendingShards)

	// First, apply the final config with only the remaining shards
	// This removes the drained shards from the cluster configuration
	targetShards := dfi.df.Spec.Cluster.Shards

	// Filter masters and shardPods to only include remaining shards
	remainingMasters := make(map[string]*corev1.Pod)
	remainingShardPods := make(map[string][]*corev1.Pod)
	var remainingAllPods []*corev1.Pod

	for i := int32(0); i < targetShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		if master, ok := masters[shardName]; ok {
			remainingMasters[shardName] = master
		}
		if pods, ok := shardPods[shardName]; ok {
			remainingShardPods[shardName] = pods
			remainingAllPods = append(remainingAllPods, pods...)
		}
	}

	// Build config for remaining shards only
	configJSON, hash, err := dfi.buildClusterConfigForShards(ctx, remainingMasters, remainingShardPods, targetShards)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to build final cluster config: %w", err)
	}

	// Apply to remaining pods only
	if err := dfi.applyClusterConfig(ctx, configJSON, remainingAllPods); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to apply final cluster config: %w", err)
	}

	// Now safe to delete the orphaned resources
	if err := dfi.cleanupDrainedShards(ctx, pendingShards); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to cleanup drained shards: %w", err)
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "ScaleDownComplete",
		fmt.Sprintf("Successfully scaled down cluster and removed shards: %v", pendingShards))

	// Update status: clear pending shards, update previousShards
	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	dfi.df.Status.Cluster.ScaleDownPending = nil
	dfi.df.Status.Cluster.PreviousShards = targetShards
	dfi.df.Status.Cluster.ConfigHash = hash
	dfi.df.Status.Cluster.ObservedGeneration = dfi.df.Generation
	now := metav1.Now()
	dfi.df.Status.Cluster.LastConfigAppliedAt = &now
	dfi.df.Status.Phase = PhaseReady
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("failed to update status after cleanup: %w", err)
	}

	return ctrl.Result{}, false, nil
}

// buildClusterConfigForShards builds a cluster config JSON for a specific set of shards.
// Used when we need to exclude shards being removed during scale-down.
func (dfi *DragonflyInstance) buildClusterConfigForShards(ctx context.Context, masters map[string]*corev1.Pod, shardPods map[string][]*corev1.Pod, numShards int32) (string, string, error) {
	var nodes []clusterNodeAddress

	// Compute slot ranges for the target number of shards
	slotRanges := computeSlotRanges(numShards)

	for i := int32(0); i < numShards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		master := masters[shardName]
		if master == nil {
			return "", "", fmt.Errorf("shard %s has no master", shardName)
		}

		masterNode, err := dfi.getClusterNodeInfo(ctx, master)
		if err != nil {
			return "", "", fmt.Errorf("failed to fetch master id for shard %s: %w", shardName, err)
		}

		replicas := []clusterNode{}
		for _, pod := range shardPods[shardName] {
			if pod.Name == master.Name {
				continue
			}
			node, err := dfi.getClusterNodeInfo(ctx, pod)
			if err != nil {
				return "", "", fmt.Errorf("failed to fetch replica id for shard %s: %w", shardName, err)
			}
			replicas = append(replicas, node)
		}

		nodes = append(nodes, clusterNodeAddress{
			SlotRanges: slotRanges[shardName],
			Master:     &masterNode,
			Replicas:   replicas,
		})
	}

	payload, err := json.Marshal(nodes)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal cluster config: %w", err)
	}

	hash := sha256.Sum256(payload)
	dfi.log.Info("generated cluster config for shards", "numShards", numShards, "config", string(payload))
	return string(payload), hex.EncodeToString(hash[:]), nil
}
