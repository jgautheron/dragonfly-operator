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
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// BackupSetIDFormat is the timestamp format for backup set IDs.
const BackupSetIDFormat = "2006-01-24T15-04-05Z"

// GenerateBackupSetID creates a new backup set ID based on the current time.
func GenerateBackupSetID() string {
	return time.Now().UTC().Format(BackupSetIDFormat)
}

// shouldTriggerBackup determines if a new coordinated backup should be triggered
// based on the cron schedule and last completed backup time.
func (dfi *DragonflyInstance) shouldTriggerBackup() (bool, error) {
	if dfi.df.Spec.Snapshot == nil || dfi.df.Spec.Snapshot.Cron == "" {
		return false, nil
	}

	// Only manage backups in cluster mode
	if !dfi.isClusterMode() {
		return false, nil
	}

	cronExpr := dfi.df.Spec.Snapshot.Cron
	schedule, err := cron.ParseStandard(cronExpr)
	if err != nil {
		return false, fmt.Errorf("failed to parse cron expression %q: %w", cronExpr, err)
	}

	// Determine the reference time for calculating next run
	var lastRun time.Time
	if dfi.df.Status.Cluster != nil && dfi.df.Status.Cluster.LastScheduledBackup != nil {
		lastRun = dfi.df.Status.Cluster.LastScheduledBackup.Time
	} else {
		// If no previous backup, use a time far in the past to trigger immediately
		lastRun = time.Now().Add(-365 * 24 * time.Hour)
	}

	// Check if there's a backup currently running
	if dfi.df.Status.Cluster != nil && dfi.df.Status.Cluster.LastBackupSet != nil {
		if dfi.df.Status.Cluster.LastBackupSet.Status == dfv1alpha1.BackupSetStatusRunning {
			// Backup already in progress
			return false, nil
		}
	}

	nextRun := schedule.Next(lastRun)
	return time.Now().After(nextRun), nil
}

// triggerCoordinatedBackup initiates a coordinated BGSAVE across all shard masters.
func (dfi *DragonflyInstance) triggerCoordinatedBackup(ctx context.Context, masters map[string]*corev1.Pod) (ctrl.Result, error) {
	dfi.log.Info("initiating coordinated backup across all shards")

	backupSetID := GenerateBackupSetID()
	now := metav1.Now()

	// Initialize the backup set in status
	shards := make([]dfv1alpha1.ShardBackupInfo, 0, len(masters))
	for shardName := range masters {
		shards = append(shards, dfv1alpha1.ShardBackupInfo{
			ShardName: shardName,
			Status:    dfv1alpha1.BackupSetStatusRunning,
		})
	}

	backupSet := &dfv1alpha1.BackupSet{
		ID:        backupSetID,
		StartedAt: now,
		Status:    dfv1alpha1.BackupSetStatusRunning,
		Shards:    shards,
	}

	// Update status with new backup set
	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	if dfi.df.Status.Cluster == nil {
		dfi.df.Status.Cluster = &dfv1alpha1.ClusterStatus{}
	}
	dfi.df.Status.Cluster.LastBackupSet = backupSet
	dfi.df.Status.Cluster.LastScheduledBackup = &now

	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update backup set status: %w", err)
	}

	// Trigger BGSAVE on all masters concurrently
	var wg sync.WaitGroup
	results := make(map[string]error)
	var resultsMu sync.Mutex

	for shardName, master := range masters {
		wg.Add(1)
		go func(shard string, pod *corev1.Pod) {
			defer wg.Done()
			err := dfi.triggerBGSAVE(ctx, pod)
			resultsMu.Lock()
			results[shard] = err
			resultsMu.Unlock()
		}(shardName, master)
	}

	wg.Wait()

	// Check results and update status
	allSucceeded := true
	for shardName, err := range results {
		if err != nil {
			dfi.log.Error(err, "BGSAVE failed on shard", "shard", shardName)
			allSucceeded = false
		} else {
			dfi.log.Info("BGSAVE triggered successfully", "shard", shardName)
		}
	}

	if !allSucceeded {
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "BackupFailed",
			fmt.Sprintf("Coordinated backup %s failed on one or more shards", backupSetID))
	} else {
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "BackupStarted",
			fmt.Sprintf("Coordinated backup %s started on all shards", backupSetID))
	}

	// Requeue to check completion status
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// triggerBGSAVE sends a BGSAVE command to a single pod.
func (dfi *DragonflyInstance) triggerBGSAVE(ctx context.Context, pod *corev1.Pod) error {
	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.BgSave(ctx).Result()
	if err != nil {
		// "Background save already in progress" is not an error for our purposes
		if strings.Contains(err.Error(), "Background save already in progress") {
			return nil
		}
		return fmt.Errorf("BGSAVE failed: %w", err)
	}

	if resp != "Background saving started" && resp != "OK" {
		return fmt.Errorf("unexpected BGSAVE response: %s", resp)
	}

	return nil
}

// checkBackupCompletion verifies if a running backup set has completed on all shards.
func (dfi *DragonflyInstance) checkBackupCompletion(ctx context.Context, masters map[string]*corev1.Pod) (ctrl.Result, error) {
	if dfi.df.Status.Cluster == nil || dfi.df.Status.Cluster.LastBackupSet == nil {
		return ctrl.Result{}, nil
	}

	backupSet := dfi.df.Status.Cluster.LastBackupSet
	if backupSet.Status != dfv1alpha1.BackupSetStatusRunning {
		return ctrl.Result{}, nil
	}

	dfi.log.Info("checking backup completion status", "backupSetID", backupSet.ID)

	// Check persistence status on all masters
	var wg sync.WaitGroup
	results := make(map[string]*persistenceStatus)
	var resultsMu sync.Mutex

	for shardName, master := range masters {
		wg.Add(1)
		go func(shard string, pod *corev1.Pod) {
			defer wg.Done()
			status, err := dfi.getPersistenceStatus(ctx, pod)
			resultsMu.Lock()
			if err != nil {
				dfi.log.Error(err, "failed to get persistence status", "shard", shard)
				results[shard] = &persistenceStatus{isError: true}
			} else {
				results[shard] = status
			}
			resultsMu.Unlock()
		}(shardName, master)
	}

	wg.Wait()

	// Analyze results
	allCompleted := true
	anyFailed := false
	startedAt := backupSet.StartedAt.Time
	for _, status := range results {
		completed, failed := evaluateBackupCompletion(startedAt, status)
		if !completed {
			allCompleted = false
			continue
		}
		if failed {
			anyFailed = true
		}
	}

	if !allCompleted {
		// Still running or missing sufficient info, requeue
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Update backup set status
	now := metav1.Now()
	patchFrom := client.MergeFrom(dfi.df.DeepCopy())

	backupSet.CompletedAt = &now
	if anyFailed {
		backupSet.Status = dfv1alpha1.BackupSetStatusFailed
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeWarning, "BackupFailed",
			fmt.Sprintf("Coordinated backup %s failed", backupSet.ID))
	} else {
		backupSet.Status = dfv1alpha1.BackupSetStatusSucceeded
		// Set active restore set for future pod restarts
		dfi.df.Status.Cluster.ActiveRestoreSet = backupSet.ID
		dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "BackupCompleted",
			fmt.Sprintf("Coordinated backup %s completed successfully", backupSet.ID))
	}

	// Update per-shard status
	for i := range backupSet.Shards {
		shardName := backupSet.Shards[i].ShardName
		if status, ok := results[shardName]; ok {
			completed, failed := evaluateBackupCompletion(startedAt, status)
			if !completed || failed {
				backupSet.Shards[i].Status = dfv1alpha1.BackupSetStatusFailed
			} else {
				backupSet.Shards[i].Status = dfv1alpha1.BackupSetStatusSucceeded
				backupSet.Shards[i].CompletedAt = &now
				if master, ok := masters[shardName]; ok {
					backupSet.Shards[i].SnapshotPath = dfi.computeShardSnapshotPath(shardName, master)
				}
			}
		}
	}

	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update backup completion status: %w", err)
	}

	return ctrl.Result{}, nil
}

type persistenceStatus struct {
	isSaving      bool
	lastSaveTime  time.Time
	hasLastSave   bool
	lastSaveOK    bool
	isError       bool
	loadingStatus string
	loadState     string
}

func parsePersistenceStatus(info string) *persistenceStatus {
	data := parseInfoToMap(info)

	status := &persistenceStatus{}

	// Check if save is in progress
	if val, ok := data["rdb_bgsave_in_progress"]; ok && val == "1" {
		status.isSaving = true
	}

	// Check last save time and status
	if val, ok := data["rdb_last_bgsave_status"]; ok {
		status.lastSaveOK = (val == "ok")
	}

	if val, ok := data["rdb_last_save_time"]; ok {
		if ts, err := strconv.ParseInt(val, 10, 64); err == nil && ts > 0 {
			status.lastSaveTime = time.Unix(ts, 0).UTC()
			status.hasLastSave = true
		}
	}

	// Check loading status
	if val, ok := data["loading"]; ok {
		status.loadingStatus = val
	}
	if val, ok := data["load_state"]; ok {
		status.loadState = val
	}

	return status
}

// getPersistenceStatus retrieves the persistence info from a pod.
func (dfi *DragonflyInstance) getPersistenceStatus(ctx context.Context, pod *corev1.Pod) (*persistenceStatus, error) {
	redisClient := redis.NewClient(&redis.Options{
		ClientName: resources.DragonflyOperatorName,
		Addr:       net.JoinHostPort(pod.Status.PodIP, strconv.Itoa(int(dfi.adminPort()))),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	info, err := redisClient.Info(ctx, "persistence").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get INFO persistence: %w", err)
	}

	return parsePersistenceStatus(info), nil
}

func evaluateBackupCompletion(startedAt time.Time, status *persistenceStatus) (completed bool, failed bool) {
	if status == nil || status.isError {
		return false, false
	}
	if status.isSaving {
		return false, false
	}
	if !status.hasLastSave {
		return false, false
	}
	if status.lastSaveTime.Before(startedAt) {
		return false, false
	}
	if !status.lastSaveOK {
		return true, true
	}
	return true, false
}

func restoredFromBackupSet(restoreSetID string, status *persistenceStatus) (bool, error) {
	if status == nil {
		return false, fmt.Errorf("missing persistence status")
	}
	if status.loadingStatus != "" && status.loadingStatus != "0" {
		return false, nil
	}
	if status.loadState != "" && status.loadState != "done" {
		return false, nil
	}
	if restoreSetID == "" {
		return true, nil
	}

	restoreTime, err := time.Parse(BackupSetIDFormat, restoreSetID)
	if err != nil {
		return false, fmt.Errorf("invalid restore set ID %q: %w", restoreSetID, err)
	}

	if !status.hasLastSave {
		return false, nil
	}

	if status.lastSaveTime.Before(restoreTime) {
		return false, nil
	}

	return true, nil
}

// computeShardSnapshotPath returns the expected snapshot path for a shard.
func (dfi *DragonflyInstance) computeShardSnapshotPath(shardName string, master *corev1.Pod) string {
	if dfi.df.Spec.Snapshot == nil {
		return ""
	}

	baseDir := dfi.df.Spec.Snapshot.Dir
	if baseDir == "" {
		baseDir = resources.SnapshotsDir
	}

	return resources.ComputeShardSnapshotDir(baseDir, dfi.df.Name, shardName)
}

// reconcileClusterBackup handles the backup orchestration for cluster mode.
// It should be called as part of the cluster reconciliation loop.
func (dfi *DragonflyInstance) reconcileClusterBackup(ctx context.Context, masters map[string]*corev1.Pod) (ctrl.Result, error) {
	// Skip if snapshots not configured
	if dfi.df.Spec.Snapshot == nil || dfi.df.Spec.Snapshot.Cron == "" {
		return ctrl.Result{}, nil
	}

	// Check if there's a running backup to monitor
	if dfi.df.Status.Cluster != nil && dfi.df.Status.Cluster.LastBackupSet != nil {
		if dfi.df.Status.Cluster.LastBackupSet.Status == dfv1alpha1.BackupSetStatusRunning {
			return dfi.checkBackupCompletion(ctx, masters)
		}
	}

	// Check if a new backup should be triggered
	shouldBackup, err := dfi.shouldTriggerBackup()
	if err != nil {
		dfi.log.Error(err, "failed to check backup schedule")
		return ctrl.Result{}, nil // Don't fail reconciliation for backup errors
	}

	if shouldBackup {
		return dfi.triggerCoordinatedBackup(ctx, masters)
	}

	return ctrl.Result{}, nil
}

// reconcileClusterRestore handles the coordinated restore gating for cluster mode.
// It ensures all shards have completed restoring before marking the cluster as ready.
// Returns true if restore is complete (or not applicable), false if still restoring.
func (dfi *DragonflyInstance) reconcileClusterRestore(ctx context.Context, masters map[string]*corev1.Pod) (bool, error) {
	// Skip if snapshots not configured or no active restore set
	if dfi.df.Spec.Snapshot == nil {
		return true, nil
	}

	status := dfi.df.Status.Cluster
	if status == nil || status.ActiveRestoreSet == "" {
		return true, nil
	}

	// Check if restore state already indicates ready
	if status.RestoreState == dfv1alpha1.RestoreStateReady {
		return true, nil
	}

	dfi.log.Info("checking coordinated restore status", "activeRestoreSet", status.ActiveRestoreSet)

	// Check if all masters have completed loading and restored from the same set
	allLoaded := true
	for shardName, master := range masters {
		persistence, err := dfi.getPersistenceStatus(ctx, master)
		if err != nil {
			dfi.log.Error(err, "failed to check dataset loaded status", "shard", shardName)
			return false, err
		}

		restored, err := restoredFromBackupSet(status.ActiveRestoreSet, persistence)
		if err != nil {
			dfi.log.Error(err, "failed to validate restore set", "shard", shardName, "restoreSet", status.ActiveRestoreSet)
			return false, err
		}

		if !restored {
			dfi.log.Info("shard not restored from active set yet", "shard", shardName, "pod", master.Name)
			allLoaded = false
		}
	}

	if !allLoaded {
		// Update status to indicate restoring
		if status.RestoreState != dfv1alpha1.RestoreStateRestoring {
			patchFrom := client.MergeFrom(dfi.df.DeepCopy())
			dfi.df.Status.Cluster.RestoreState = dfv1alpha1.RestoreStateRestoring
			if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
				return false, fmt.Errorf("failed to update restore state: %w", err)
			}
		}
		return false, nil
	}

	// All shards have completed loading - mark restore as ready
	dfi.log.Info("all shards have completed restore", "activeRestoreSet", status.ActiveRestoreSet)

	patchFrom := client.MergeFrom(dfi.df.DeepCopy())
	dfi.df.Status.Cluster.RestoreState = dfv1alpha1.RestoreStateReady
	if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
		return false, fmt.Errorf("failed to update restore state to ready: %w", err)
	}

	dfi.eventRecorder.Event(dfi.df, corev1.EventTypeNormal, "RestoreCompleted",
		fmt.Sprintf("All shards restored from backup set %s", status.ActiveRestoreSet))

	return true, nil
}

// initializeRestoreState sets up the initial restore state when pods are starting
// and there's an available backup set to restore from.
func (dfi *DragonflyInstance) initializeRestoreState(ctx context.Context) error {
	if dfi.df.Spec.Snapshot == nil {
		return nil
	}

	status := dfi.df.Status.Cluster
	if status == nil {
		return nil
	}

	// If there's a completed backup set but no active restore set, set it up
	if status.LastBackupSet != nil &&
		status.LastBackupSet.Status == dfv1alpha1.BackupSetStatusSucceeded &&
		status.ActiveRestoreSet == "" {

		patchFrom := client.MergeFrom(dfi.df.DeepCopy())
		dfi.df.Status.Cluster.ActiveRestoreSet = status.LastBackupSet.ID
		dfi.df.Status.Cluster.RestoreState = dfv1alpha1.RestoreStatePending

		if err := dfi.client.Status().Patch(ctx, dfi.df, patchFrom); err != nil {
			return fmt.Errorf("failed to initialize restore state: %w", err)
		}

		dfi.log.Info("initialized restore state", "backupSetID", status.LastBackupSet.ID)
	}

	return nil
}
