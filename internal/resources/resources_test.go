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
				Mode:             resourcesv1.ClusterModeMultiShard,
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
