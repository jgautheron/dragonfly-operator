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
	return dfi.df.Spec.Cluster != nil && dfi.df.Spec.Cluster.Mode == dfv1alpha1.ClusterModeMultiShard
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

	// Check if we need to handle failover (some pods are missing, unhealthy, or split-brain)
	needsFailover := false
	for i := int32(0); i < dfi.df.Spec.Cluster.Shards; i++ {
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
		for i := int32(0); i < dfi.df.Spec.Cluster.Shards; i++ {
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

	for i := int32(0); i < dfi.df.Spec.Cluster.Shards; i++ {
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

type clusterNodeAddress struct {
	SlotRanges []dfv1alpha1.SlotRange `json:"slot_ranges"`
	Master     *clusterNode           `json:"master"`
	Replicas   []clusterNode          `json:"replicas"`
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
