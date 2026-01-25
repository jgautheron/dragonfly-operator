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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DragonflySpec defines the desired state of Dragonfly
type DragonflySpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Replicas is the total number of Dragonfly instances including the master
	Replicas int32 `json:"replicas,omitempty"`

	// (Optional) Cluster configuration. When provided, the operator deploys Dragonfly
	// in real cluster mode and ignores the global Replicas count.
	// +optional
	// +kubebuilder:validation:Optional
	Cluster *ClusterSpec `json:"cluster,omitempty"`

	// Image is the Dragonfly image to use
	Image string `json:"image,omitempty"`

	// (Optional) imagePullPolicy to set to Dragonfly, default is Always
	// +optional
	// +kubebuilder:validation:Optional
	// +kubebuilder:default:="Always"
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// (Optional) imagePullSecrets to set to Dragonfly
	// +optional
	// +kubebuilder:validation:Optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// (Optional) Dragonfly container args to pass to the container
	// Refer to the Dragonfly documentation for the list of supported args
	// +optional
	// +kubebuilder:validation:Optional
	Args []string `json:"args,omitempty"`

	// (Optional) Acl file Secret to pass to the container
	// +optional
	// +kubebuilder:validation:Optional
	AclFromSecret *corev1.SecretKeySelector `json:"aclFromSecret,omitempty"`

	// (Optional) Annotations to add to the Dragonfly pods.
	// +optional
	// +kubebuilder:validation:Optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// (Optional) Labels to add to the Dragonfly pods.
	// +optional
	// +kubebuilder:validation:Optional
	Labels map[string]string `json:"labels,omitempty"`

	// (Optional) Env variables to add to the Dragonfly pods.
	// +optional
	// +kubebuilder:validation:Optional
	Env []corev1.EnvVar `json:"env,omitempty"`

	// (Optional) Additional containers to add to dragonflycluster. Replace container on name collision.
	// +optional
	// +kubebuilder:validation:Optional
	AdditionalContainers []corev1.Container `json:"additionalContainers,omitempty"`

	// (Optional) Additional volumes to add to dragonflycluster. Replace volume on name collision.
	// +optional
	// +kubebuilder:validation:Optional
	AdditionalVolumes []corev1.Volume `json:"additionalVolumes,omitempty"`

	// (Optional) Dragonfly container resource limits. Any container limits
	// can be specified.
	// +optional
	// +kubebuilder:validation:Optional
	Resources *corev1.ResourceRequirements `json:"resources,omitempty"`

	// (Optional) Dragonfly pod affinity
	// +optional
	// +kubebuilder:validation:Optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// (Optional) Dragonfly pod node selector
	// +optional
	// +kubebuilder:validation:Optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// (Optional) Dragonfly memcached port
	// +optional
	// +kubebuilder:validation:Optional
	MemcachedPort int32 `json:"memcachedPort,omitempty"`

	// (Optional) Dragonfly pod tolerations
	// +optional
	// +kubebuilder:validation:Optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// (Optional) Dragonfly pod topologySpreadConstraints
	// +optional
	// +kubebuilder:validation:Optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// (Optional) Dragonfly Authentication mechanism
	// +optional
	// +kubebuilder:validation:Optional
	Authentication *Authentication `json:"authentication,omitempty"`

	// (Optional) Dragonfly container security context
	// +optional
	// +kubebuilder:validation:Optional
	ContainerSecurityContext *corev1.SecurityContext `json:"containerSecurityContext,omitempty"`

	// (Optional) Dragonfly pod security context
	// +optional
	// +kubebuilder:validation:Optional
	PodSecurityContext *corev1.PodSecurityContext `json:"podSecurityContext,omitempty"`

	// (Optional) Dragonfly pod service account name
	// +optional
	// +kubebuilder:validation:Optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`

	// (Optional) Dragonfly pod priority class name
	// +optional
	// +kubebuilder:validation:Optional
	PriorityClassName string `json:"priorityClassName,omitempty"`

	// (Optional) Dragonfly TLS secret to used for TLS
	// Connections to Dragonfly. Dragonfly instance  must
	// have access to this secret and be in the same namespace
	// +optional
	// +kubebuilder:validation:Optional
	TLSSecretRef *corev1.SecretReference `json:"tlsSecretRef,omitempty"`

	// (Optional) Dragonfly SSD Tiering configuration
	// +optional
	// +kubebuilder:validation:Optional
	Tiering *Tiering `json:"tiering,omitempty"`

	// (Optional) Dragonfly Snapshot configuration
	// +optional
	// +kubebuilder:validation:Optional
	Snapshot *Snapshot `json:"snapshot,omitempty"`

	// (Optional) Skip Assigning FileSystem Group. Required for platforms such as Openshift that require IDs to not be set, as it injects a fixed randomized ID per namespace into all pods.
	// +optional
	// +kubebuilder:validation:Optional
	SkipFSGroup bool `json:"skipFSGroup,omitempty"`

	// (Optional) Dragonfly Service configuration
	// +optional
	// +kubebuilder:validation:Optional
	ServiceSpec *ServiceSpec `json:"serviceSpec,omitempty"`

	// (Optional) Dragonfly pod init containers
	// +optional
	// +kubebuilder:validation:Optional
	InitContainers []corev1.Container `json:"initContainers,omitempty"`

	// (Optional) Dragonfly direct child resources additional annotations and labels
	// +optional
	// +kubebuilder:validation:Optional
	OwnedObjectsMetadata *OwnedObjectsMetadata `json:"ownedObjectsMetadata,omitempty"`
}

// ClusterSpec describes the desired cluster topology.
// When spec.cluster is set, the operator automatically enables Dragonfly's
// cluster mode (--cluster_mode=yes) and manages the cluster configuration.
type ClusterSpec struct {
	// AdminPort overrides the admin port used for cluster commands.
	// Defaults to 9999 when unset.
	// +optional
	// +kubebuilder:default:=9999
	AdminPort int32 `json:"adminPort,omitempty"`

	// Shards is the number of shards to create. Hash slots are automatically
	// distributed evenly across all shards.
	// +kubebuilder:validation:Minimum=1
	Shards int32 `json:"shards"`

	// ReplicasPerShard is the number of replicas per shard (1 = primary only).
	// Defaults to 1 when unset.
	// +optional
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum=1
	ReplicasPerShard int32 `json:"replicasPerShard,omitempty"`

	// MasterAntiAffinity enables automatic pod anti-affinity to spread shard pods
	// across different nodes. This helps ensure masters from different shards
	// run on different nodes for high availability. Defaults to true.
	// +optional
	// +kubebuilder:default:=true
	MasterAntiAffinity *bool `json:"masterAntiAffinity,omitempty"`
}

// SlotRange describes a hash slot interval owned by a shard (used internally).
// Both Start and End are inclusive (e.g., Start=0, End=8191 covers 8192 slots).
type SlotRange struct {
	Start int32 `json:"start"`
	End   int32 `json:"end"`
}

type OwnedObjectsMetadata struct {
	Annotations map[string]string `json:"annotations,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

type ServiceSpec struct {
	// (Optional) Dragonfly Service type
	// +optional
	// +kubebuilder:validation:Optional
	Type corev1.ServiceType `json:"type,omitempty"`

	// (Optional) Dragonfly Service name
	// +optional
	// +kubebuilder:validation:Optional
	Name string `json:"name,omitempty"`

	// (Optional) Dragonfly Service nodePort
	// +optional
	// +kubebuilder:validation:Optional
	NodePort int32 `json:"nodePort,omitempty"`

	// (Optional) Dragonfly Service Annotations
	// +optional
	// +kubebuilder:validation:Optional
	Annotations map[string]string `json:"annotations,omitempty"`

	// (Optional) Dragonfly Service Labels
	// +optional
	// +kubebuilder:validation:Optional
	Labels map[string]string `json:"labels,omitempty"`
}

type Tiering struct {
	// (Optional) Dragonfly PVC spec for cache tiering configuration
	// +optional
	// +kubebuilder:validation:Optional
	PersistentVolumeClaimSpec *corev1.PersistentVolumeClaimSpec `json:"persistentVolumeClaimSpec,omitempty"`
}

type Snapshot struct {
	// (Optional) The path to the snapshot directory
	// This can also be an S3 URI with the prefix `s3://` when
	// using S3 as the snapshot backend
	// +optional
	// +kubebuilder:validation:Optional
	Dir string `json:"dir,omitempty"`

	// (Optional) Dragonfly snapshot schedule
	// +optional
	// +kubebuilder:validation:Optional
	Cron string `json:"cron,omitempty"`

	// (Optional) Enable snapshot on master only
	// +optional
	// +kubebuilder:validation:Optional
	EnableOnMasterOnly bool `json:"enableOnMasterOnly,omitempty"`

	// (Optional) Dragonfly PVC spec
	// +optional
	// +kubebuilder:validation:Optional
	PersistentVolumeClaimSpec *corev1.PersistentVolumeClaimSpec `json:"persistentVolumeClaimSpec,omitempty"`

	// (Optional) Name of an existing PVC to use for Dragonfly snapshots
	// +optional
	// +kubebuilder:validation:Optional
	ExistingPersistentVolumeClaimName string `json:"existingPersistentVolumeClaimName,omitempty"`
}

type Authentication struct {
	// (Optional) Dragonfly Password from Secret as a reference to a specific key
	// +optional
	PasswordFromSecret *corev1.SecretKeySelector `json:"passwordFromSecret,omitempty"`

	// (Optional) If specified, the Dragonfly instance will check if the
	// client certificate is signed by this CA. Server TLS must be enabled for this.
	// +optional
	ClientCaCertSecret *corev1.SecretKeySelector `json:"clientCaCertSecret,omitempty"`
}

// DragonflyStatus defines the observed state of Dragonfly
type DragonflyStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// Status of the Dragonfly Instance
	// It can be one of the following:
	// - "ready": The Dragonfly instance is ready to serve requests
	// - "configuring-replication": The controller is updating the master of the Dragonfly instance
	// - "resources-created": The Dragonfly instance resources were created but not yet configured
	Phase string `json:"phase,omitempty"`

	// TODO: remove this in a future release.
	// IsRollingUpdate is true if the Dragonfly instance is being updated
	IsRollingUpdate bool `json:"isRollingUpdate,omitempty"`

	// Cluster holds details about the current cluster configuration state.
	// +optional
	Cluster *ClusterStatus `json:"cluster,omitempty"`

	// Conditions provides high-level state tracking.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// ClusterStatus tracks the multi-shard cluster state.
// ShardMasterInfo tracks the current master for a shard and when it was promoted.
type ShardMasterInfo struct {
	// PodName is the name of the current master pod for this shard.
	PodName string `json:"podName,omitempty"`
	// MasterSince is when this pod became master. Used for grace period tracking.
	// +optional
	MasterSince *metav1.Time `json:"masterSince,omitempty"`
}

// BackupSetStatus represents the state of a coordinated backup set.
// +kubebuilder:validation:Enum=Running;Succeeded;Failed
type BackupSetStatus string

const (
	// BackupSetStatusRunning indicates the backup is in progress.
	BackupSetStatusRunning BackupSetStatus = "Running"
	// BackupSetStatusSucceeded indicates all shards completed successfully.
	BackupSetStatusSucceeded BackupSetStatus = "Succeeded"
	// BackupSetStatusFailed indicates one or more shards failed.
	BackupSetStatusFailed BackupSetStatus = "Failed"
)

// RestoreState represents the state of a coordinated restore operation.
// +kubebuilder:validation:Enum=Pending;Restoring;Ready
type RestoreState string

const (
	// RestoreStatePending indicates the restore has not started yet.
	RestoreStatePending RestoreState = "Pending"
	// RestoreStateRestoring indicates shards are actively restoring.
	RestoreStateRestoring RestoreState = "Restoring"
	// RestoreStateReady indicates all shards have restored from the same backup set.
	RestoreStateReady RestoreState = "Ready"
)

// ShardBackupInfo tracks per-shard snapshot information within a backup set.
type ShardBackupInfo struct {
	// ShardName is the name of the shard (e.g., "shard-0").
	ShardName string `json:"shardName"`
	// SnapshotPath is the full path or S3 URI to the snapshot file.
	// +optional
	SnapshotPath string `json:"snapshotPath,omitempty"`
	// Status indicates if this shard's backup succeeded or failed.
	// +optional
	Status BackupSetStatus `json:"status,omitempty"`
	// CompletedAt is when this shard's snapshot completed.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// BackupSet represents a coordinated backup across all shards.
type BackupSet struct {
	// ID is a unique identifier for this backup set (timestamp string, e.g., "2026-01-24T16-30-00Z").
	ID string `json:"id"`
	// StartedAt is when the backup set was initiated.
	StartedAt metav1.Time `json:"startedAt"`
	// CompletedAt is when the backup set finished (success or failure).
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
	// Status is the overall status of the backup set.
	Status BackupSetStatus `json:"status"`
	// Shards contains per-shard backup information.
	// +optional
	Shards []ShardBackupInfo `json:"shards,omitempty"`
}

type ClusterStatus struct {
	// ConfigHash is the hash of the last applied DFLYCLUSTER CONFIG payload.
	ConfigHash string `json:"configHash,omitempty"`
	// ObservedGeneration indicates which spec revision the status represents.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// LastConfigAppliedAt indicates when the cluster config was last applied.
	// +optional
	LastConfigAppliedAt *metav1.Time `json:"lastConfigAppliedAt,omitempty"`
	// ShardMasters tracks the current master for each shard and when it was promoted.
	// Key is the shard name (e.g., "shard-0").
	// +optional
	ShardMasters map[string]ShardMasterInfo `json:"shardMasters,omitempty"`

	// LastBackupSet contains information about the most recent coordinated backup.
	// +optional
	LastBackupSet *BackupSet `json:"lastBackupSet,omitempty"`
	// ActiveRestoreSet is the backup set ID that shards should restore from.
	// Set by the operator when a new backup set is available and pods are starting.
	// +optional
	ActiveRestoreSet string `json:"activeRestoreSet,omitempty"`
	// RestoreState tracks the progress of the coordinated restore operation.
	// +optional
	RestoreState RestoreState `json:"restoreState,omitempty"`
	// LastScheduledBackup is when the operator last checked/ran the cron schedule.
	// Used to determine when the next backup should run.
	// +optional
	LastScheduledBackup *metav1.Time `json:"lastScheduledBackup,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="The current phase of the Dragonfly cluster"
//+kubebuilder:printcolumn:name="Rolling Update",type="boolean",JSONPath=".status.isRollingUpdate",description="Indicates if a rolling update is in progress"
//+kubebuilder:printcolumn:name="Replicas",type="integer",JSONPath=".spec.replicas",description="Number of replicas"

// Dragonfly is the Schema for the dragonflies API
type Dragonfly struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DragonflySpec   `json:"spec,omitempty"`
	Status DragonflyStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// DragonflyList contains a list of Dragonfly
type DragonflyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Dragonfly `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Dragonfly{}, &DragonflyList{})
}
