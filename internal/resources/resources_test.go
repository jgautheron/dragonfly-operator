package resources

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	resourcesv1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestMergeNamedSlices_Containers(t *testing.T) {
	base := []corev1.Container{
		{Name: "main", Image: "main:1"},
		{Name: "sidecar", Image: "sidecar:1"},
	}
	override := []corev1.Container{
		{Name: "main", Image: "main:custom"},
		{Name: "metrics", Image: "metrics:1"},
	}

	result := mergeNamedSlices(base, override, func(c corev1.Container) string { return c.Name })

	assert.Len(t, result, 3)
	assert.Equal(t, "main:custom", result[0].Image)
	assert.Equal(t, "metrics:1", result[1].Image)
	assert.Equal(t, "sidecar:1", result[2].Image)
}

func TestMergeNamedSlices_Volumes(t *testing.T) {
	base := []corev1.Volume{
		{Name: "config"},
		{Name: "data"},
	}
	override := []corev1.Volume{
		{Name: "config"}, // override
		{Name: "logs"},
	}

	result := mergeNamedSlices(base, override, func(v corev1.Volume) string { return v.Name })

	assert.Len(t, result, 3)
	assert.Equal(t, "config", result[0].Name)
	assert.Equal(t, "logs", result[1].Name)
	assert.Equal(t, "data", result[2].Name)
}

func TestMergeNamedSlices_EmptyOverride(t *testing.T) {
	base := []corev1.Volume{
		{Name: "default"},
	}
	override := []corev1.Volume{}

	result := mergeNamedSlices(base, override, func(v corev1.Volume) string { return v.Name })

	assert.Len(t, result, 1)
	assert.Equal(t, "default", result[0].Name)
}

func TestMergeNamedSlices_EmptyBase(t *testing.T) {
	base := []corev1.Container{}
	override := []corev1.Container{
		{Name: "user", Image: "user:1"},
	}

	result := mergeNamedSlices(base, override, func(c corev1.Container) string { return c.Name })

	assert.Len(t, result, 1)
	assert.Equal(t, "user", result[0].Name)
}

func TestGenerateClusterResources(t *testing.T) {
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:           2,
				ReplicasPerShard: 1,
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	var stsCount, serviceCount int
	for _, obj := range objs {
		switch obj.(type) {
		case *appsv1.StatefulSet:
			stsCount++
		case *corev1.Service:
			serviceCount++
		}
	}

	assert.Equal(t, 2, stsCount, "expected one statefulset per shard")
	assert.Equal(t, 3, serviceCount, "expected headless service per shard plus cluster service")
}

func TestClusterAntiAffinityEnabled(t *testing.T) {
	// Anti-affinity is enabled by default
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:           2,
				ReplicasPerShard: 2,
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	// Check that StatefulSets have anti-affinity
	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		affinity := sts.Spec.Template.Spec.Affinity
		assert.NotNil(t, affinity, "statefulset should have affinity")
		assert.NotNil(t, affinity.PodAntiAffinity, "statefulset should have pod anti-affinity")
		assert.Len(t, affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1,
			"should have one preferred anti-affinity term")

		term := affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[0]
		assert.Equal(t, int32(100), term.Weight)
		assert.Equal(t, "kubernetes.io/hostname", term.PodAffinityTerm.TopologyKey)
		assert.Equal(t, "test-cluster", term.PodAffinityTerm.LabelSelector.MatchLabels[DragonflyNameLabelKey])
	}
}

func TestClusterAntiAffinityDisabled(t *testing.T) {
	// Explicitly disable anti-affinity
	antiAffinityDisabled := false
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:             2,
				ReplicasPerShard:   2,
				MasterAntiAffinity: &antiAffinityDisabled,
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	// Check that StatefulSets do NOT have anti-affinity
	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		affinity := sts.Spec.Template.Spec.Affinity
		// Affinity should be nil since we didn't set any user-specified affinity
		// and anti-affinity is disabled
		assert.Nil(t, affinity, "statefulset should not have affinity when anti-affinity is disabled")
	}
}

func TestClusterAntiAffinityMergesWithUserAffinity(t *testing.T) {
	// User specifies their own affinity, we should merge
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:           2,
				ReplicasPerShard: 2,
			},
			Affinity: &corev1.Affinity{
				NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
						NodeSelectorTerms: []corev1.NodeSelectorTerm{
							{
								MatchExpressions: []corev1.NodeSelectorRequirement{
									{
										Key:      "kubernetes.io/arch",
										Operator: corev1.NodeSelectorOpIn,
										Values:   []string{"amd64"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	// Check that StatefulSets have both user affinity and our anti-affinity
	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		affinity := sts.Spec.Template.Spec.Affinity
		assert.NotNil(t, affinity, "statefulset should have affinity")

		// User's node affinity should be preserved
		assert.NotNil(t, affinity.NodeAffinity, "user's node affinity should be preserved")

		// Our anti-affinity should be added
		assert.NotNil(t, affinity.PodAntiAffinity, "pod anti-affinity should be added")
		assert.Len(t, affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, 1)
	}
}

func TestComputeShardSnapshotDir(t *testing.T) {
	tests := []struct {
		name        string
		basePath    string
		clusterName string
		shardName   string
		expected    string
	}{
		{
			name:        "non-cluster mode returns base path",
			basePath:    "/dragonfly/snapshots",
			clusterName: "my-cluster",
			shardName:   "",
			expected:    "/dragonfly/snapshots",
		},
		{
			name:        "cluster mode with local path",
			basePath:    "/dragonfly/snapshots",
			clusterName: "my-cluster",
			shardName:   "shard-0",
			expected:    "/dragonfly/snapshots/my-cluster/shard-0",
		},
		{
			name:        "cluster mode with S3 path",
			basePath:    "s3://my-bucket/backups",
			clusterName: "my-cluster",
			shardName:   "shard-1",
			expected:    "s3://my-bucket/backups/my-cluster/shard-1",
		},
		{
			name:        "cluster mode with S3 path trailing slash",
			basePath:    "s3://my-bucket/backups/",
			clusterName: "my-cluster",
			shardName:   "shard-2",
			expected:    "s3://my-bucket/backups/my-cluster/shard-2",
		},
		{
			name:        "cluster mode with local path trailing slash",
			basePath:    "/dragonfly/snapshots/",
			clusterName: "test-cluster",
			shardName:   "shard-0",
			expected:    "/dragonfly/snapshots/test-cluster/shard-0",
		},
		{
			name:        "cluster mode with nested S3 path",
			basePath:    "s3://my-bucket/env/prod/dragonfly",
			clusterName: "prod-cluster",
			shardName:   "shard-3",
			expected:    "s3://my-bucket/env/prod/dragonfly/prod-cluster/shard-3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ComputeShardSnapshotDir(tt.basePath, tt.clusterName, tt.shardName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClusterModeSnapshotPaths(t *testing.T) {
	// Test that cluster mode generates shard-aware snapshot paths
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:           2,
				ReplicasPerShard: 1,
			},
			Snapshot: &resourcesv1.Snapshot{
				Dir:  "s3://my-bucket/snapshots",
				Cron: "0 * * * *",
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	// Check that each StatefulSet has shard-specific snapshot dir
	shardDirs := make(map[string]string)
	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		// Find the --dir arg
		container := sts.Spec.Template.Spec.Containers[0]
		var dirArg string
		for _, arg := range container.Args {
			if len(arg) > 6 && arg[:6] == "--dir=" {
				dirArg = arg[6:]
				break
			}
		}

		shardName := sts.Spec.Selector.MatchLabels[ShardNameLabelKey]
		assert.NotEmpty(t, dirArg, "statefulset should have --dir arg")
		shardDirs[shardName] = dirArg

		// Verify the path includes the shard name
		expectedPath := "s3://my-bucket/snapshots/test-cluster/" + shardName
		assert.Equal(t, expectedPath, dirArg, "snapshot dir should be shard-specific")
	}

	// Verify we have paths for both shards
	assert.Len(t, shardDirs, 2)
	assert.Contains(t, shardDirs, "shard-0")
	assert.Contains(t, shardDirs, "shard-1")
}

func TestClusterModeDoesNotSetSnapshotCron(t *testing.T) {
	// In cluster mode, the operator manages backups, so snapshot_cron should NOT be set
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Cluster: &resourcesv1.ClusterSpec{
				Shards:           2,
				ReplicasPerShard: 1,
			},
			Snapshot: &resourcesv1.Snapshot{
				Dir:  "/dragonfly/snapshots",
				Cron: "0 * * * *",
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		container := sts.Spec.Template.Spec.Containers[0]
		for _, arg := range container.Args {
			assert.NotContains(t, arg, "--snapshot_cron",
				"cluster mode should not set snapshot_cron, operator manages backups")
		}
	}
}

func TestNonClusterModeSetsSnapshotCron(t *testing.T) {
	// In non-cluster mode, snapshot_cron should be passed to the server
	df := &resourcesv1.Dragonfly{
		TypeMeta:   metav1.TypeMeta{APIVersion: "dragonflydb.io/v1alpha1", Kind: "Dragonfly"},
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "123"},
		Spec: resourcesv1.DragonflySpec{
			Replicas: 3,
			Snapshot: &resourcesv1.Snapshot{
				Dir:  "/dragonfly/snapshots",
				Cron: "0 * * * *",
			},
		},
	}

	objs, err := GenerateDragonflyResources(df, "")
	assert.NoError(t, err)

	var found bool
	for _, obj := range objs {
		sts, ok := obj.(*appsv1.StatefulSet)
		if !ok {
			continue
		}

		container := sts.Spec.Template.Spec.Containers[0]
		for _, arg := range container.Args {
			if arg == "--snapshot_cron=0 * * * *" {
				found = true
				break
			}
		}
	}

	assert.True(t, found, "non-cluster mode should set snapshot_cron")
}
