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

package resources

import (
	"fmt"
	"strings"

	resourcesv1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	dflyUserGroup int64 = 999
)

// GenerateDragonflyResources returns the resources required for a Dragonfly Instance.
func GenerateDragonflyResources(df *resourcesv1.Dragonfly, defaultDragonflyImage string) ([]client.Object, error) {
	if df.Spec.Cluster != nil && df.Spec.Cluster.Mode == resourcesv1.ClusterModeMultiShard {
		return generateClusterResources(df, defaultDragonflyImage)
	}
	return generateStandaloneResources(df, defaultDragonflyImage)
}

func generateStandaloneResources(df *resourcesv1.Dragonfly, defaultDragonflyImage string) ([]client.Object, error) {
	var resources []client.Object

	image := df.Spec.Image
	if image == "" {
		if defaultDragonflyImage != "" {
			image = defaultDragonflyImage
		} else {
			image = fmt.Sprintf("%s:%s", DragonflyImage, Version)
		}
	}

	statefulset := buildStatefulSet(df, df.Name, df.Name, df.Spec.Replicas, nil, image)

	if err := applyStatefulSetCustomizations(df, &statefulset); err != nil {
		return nil, err
	}

	resources = append(resources, &statefulset)

	serviceName := df.Name
	if df.Spec.ServiceSpec != nil && df.Spec.ServiceSpec.Name != "" {
		serviceName = df.Spec.ServiceSpec.Name
	}

	service := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: df.Namespace,
			// Useful for automatically deleting the resources when the Dragonfly object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: df.APIVersion,
					Kind:       df.Kind,
					Name:       df.Name,
					UID:        df.UID,
				},
			},
			Labels:      generateResourceLabels(df),
			Annotations: generateResourceAnnotations(df),
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				DragonflyNameLabelKey:     df.Name,
				KubernetesAppNameLabelKey: KubernetesAppName,
				RoleLabelKey:              Master,
			},
			Ports: []corev1.ServicePort{
				{
					Name: DragonflyPortName,
					Port: DragonflyPort,
				},
			},
		},
	}

	if df.Spec.ServiceSpec != nil {
		service.Spec.Type = df.Spec.ServiceSpec.Type
		service.Annotations = df.Spec.ServiceSpec.Annotations
		service.Labels = df.Spec.ServiceSpec.Labels
		service.Spec.Ports[0].NodePort = df.Spec.ServiceSpec.NodePort
	}
	if df.Spec.MemcachedPort != 0 {
		service.Spec.Ports = append(service.Spec.Ports, corev1.ServicePort{
			Name: MemcachedPortName,
			Port: df.Spec.MemcachedPort,
		})
	}

	resources = append(resources, &service)

	pdb := policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      df.Name,
			Namespace: df.Namespace,
			// Useful for automatically deleting the resources when the Dragonfly object is deleted
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: df.APIVersion,
					Kind:       df.Kind,
					Name:       df.Name,
					UID:        df.UID,
				},
			},
			Labels:      generateResourceLabels(df),
			Annotations: generateResourceAnnotations(df),
		},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MaxUnavailable: &intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: 1,
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					DragonflyNameLabelKey:     df.Name,
					KubernetesPartOfLabelKey:  KubernetesPartOf,
					KubernetesAppNameLabelKey: KubernetesAppName,
				},
			},
		},
	}

	if df.Spec.Replicas > 1 {
		resources = append(resources, &pdb)
	}

	return resources, nil
}

func generateClusterResources(df *resourcesv1.Dragonfly, defaultDragonflyImage string) ([]client.Object, error) {
	var resources []client.Object

	image := df.Spec.Image
	if image == "" {
		if defaultDragonflyImage != "" {
			image = defaultDragonflyImage
		} else {
			image = fmt.Sprintf("%s:%s", DragonflyImage, Version)
		}
	}

	replicasPerShard := df.Spec.Cluster.ReplicasPerShard
	if replicasPerShard < 1 {
		replicasPerShard = 1
	}

	// Check if master anti-affinity is enabled (default: true)
	masterAntiAffinity := true
	if df.Spec.Cluster.MasterAntiAffinity != nil {
		masterAntiAffinity = *df.Spec.Cluster.MasterAntiAffinity
	}

	for i := int32(0); i < df.Spec.Cluster.Shards; i++ {
		shardName := fmt.Sprintf("shard-%d", i)
		shardSelector := map[string]string{
			ShardNameLabelKey: shardName,
		}

		serviceName := fmt.Sprintf("%s-%s-headless", df.Name, shardName)
		statefulsetName := fmt.Sprintf("%s-%s", df.Name, shardName)

		statefulset := buildStatefulSet(df, statefulsetName, serviceName, replicasPerShard, shardSelector, image)
		if err := applyStatefulSetCustomizations(df, &statefulset); err != nil {
			return nil, err
		}

		// Apply master anti-affinity for cluster mode
		if masterAntiAffinity {
			applyClusterAntiAffinity(df, &statefulset)
		}
		resources = append(resources, &statefulset)

		shardService := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:        serviceName,
				Namespace:   df.Namespace,
				Labels:      generateResourceLabels(df),
				Annotations: generateResourceAnnotations(df),
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: df.APIVersion,
						Kind:       df.Kind,
						Name:       df.Name,
						UID:        df.UID,
					},
				},
			},
			Spec: corev1.ServiceSpec{
				ClusterIP:                corev1.ClusterIPNone,
				PublishNotReadyAddresses: true,
				Selector: map[string]string{
					DragonflyNameLabelKey:     df.Name,
					KubernetesPartOfLabelKey:  KubernetesPartOf,
					KubernetesAppNameLabelKey: KubernetesAppName,
					ShardNameLabelKey:         shardName,
				},
				Ports: []corev1.ServicePort{
					{
						Name: DragonflyPortName,
						Port: DragonflyPort,
					},
				},
			},
		}

		if df.Spec.MemcachedPort != 0 {
			shardService.Spec.Ports = append(shardService.Spec.Ports, corev1.ServicePort{
				Name: MemcachedPortName,
				Port: df.Spec.MemcachedPort,
			})
		}

		resources = append(resources, &shardService)

		if replicasPerShard > 1 {
			pdb := policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Name:        fmt.Sprintf("%s-%s", df.Name, shardName),
					Namespace:   df.Namespace,
					Labels:      generateResourceLabels(df),
					Annotations: generateResourceAnnotations(df),
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: df.APIVersion,
							Kind:       df.Kind,
							Name:       df.Name,
							UID:        df.UID,
						},
					},
				},
				Spec: policyv1.PodDisruptionBudgetSpec{
					MaxUnavailable: &intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 1,
					},
					Selector: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							DragonflyNameLabelKey:     df.Name,
							KubernetesPartOfLabelKey:  KubernetesPartOf,
							KubernetesAppNameLabelKey: KubernetesAppName,
							ShardNameLabelKey:         shardName,
						},
					},
				},
			}
			resources = append(resources, &pdb)
		}
	}

	serviceName := df.Name
	if df.Spec.ServiceSpec != nil && df.Spec.ServiceSpec.Name != "" {
		serviceName = df.Spec.ServiceSpec.Name
	}

	clusterService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        serviceName,
			Namespace:   df.Namespace,
			Labels:      generateResourceLabels(df),
			Annotations: generateResourceAnnotations(df),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: df.APIVersion,
					Kind:       df.Kind,
					Name:       df.Name,
					UID:        df.UID,
				},
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				DragonflyNameLabelKey:     df.Name,
				KubernetesAppNameLabelKey: KubernetesAppName,
			},
			Ports: []corev1.ServicePort{
				{
					Name: DragonflyPortName,
					Port: DragonflyPort,
				},
			},
		},
	}

	if df.Spec.ServiceSpec != nil {
		clusterService.Spec.Type = df.Spec.ServiceSpec.Type
		clusterService.Annotations = df.Spec.ServiceSpec.Annotations
		clusterService.Labels = df.Spec.ServiceSpec.Labels
		clusterService.Spec.Ports[0].NodePort = df.Spec.ServiceSpec.NodePort
	}

	if df.Spec.MemcachedPort != 0 {
		clusterService.Spec.Ports = append(clusterService.Spec.Ports, corev1.ServicePort{
			Name: MemcachedPortName,
			Port: df.Spec.MemcachedPort,
		})
	}

	resources = append(resources, &clusterService)

	return resources, nil
}

func buildStatefulSet(df *resourcesv1.Dragonfly, name, serviceName string, replicas int32, selectorLabels map[string]string, image string) appsv1.StatefulSet {
	replicasCopy := replicas
	if replicasCopy < 1 {
		replicasCopy = 1
	}

	matchLabels := map[string]string{
		DragonflyNameLabelKey:     df.Name,
		KubernetesPartOfLabelKey:  KubernetesPartOf,
		KubernetesAppNameLabelKey: KubernetesAppName,
	}

	for k, v := range selectorLabels {
		matchLabels[k] = v
	}

	templateLabels := make(map[string]string, len(matchLabels))
	for k, v := range matchLabels {
		templateLabels[k] = v
	}

	statefulset := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: df.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: df.APIVersion,
					Kind:       df.Kind,
					Name:       df.Name,
					UID:        df.UID,
				},
			},
			Labels:      generateResourceLabels(df),
			Annotations: generateResourceAnnotations(df),
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicasCopy,
			ServiceName: serviceName,
			Selector: &metav1.LabelSelector{
				MatchLabels: matchLabels,
			},
			UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
				Type: appsv1.OnDeleteStatefulSetStrategyType,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: templateLabels,
				},
				Spec: corev1.PodSpec{
					ImagePullSecrets: df.Spec.ImagePullSecrets,
					Containers: []corev1.Container{
						{
							Name:  DragonflyContainerName,
							Image: image,
							Ports: []corev1.ContainerPort{
								{
									Name:          DragonflyPortName,
									ContainerPort: DragonflyPort,
								},
								{
									Name:          DragonflyAdminPortName,
									ContainerPort: DragonflyAdminPort,
								},
							},
							Args: append([]string{}, DefaultDragonflyArgs...),
							Env: append(append([]corev1.EnvVar{}, df.Spec.Env...), corev1.EnvVar{
								Name:  "HEALTHCHECK_PORT",
								Value: fmt.Sprintf("%d", DragonflyAdminPort),
							}),
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"/usr/local/bin/healthcheck.sh",
										},
									},
								},
								FailureThreshold:    3,
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"/bin/sh",
											"/usr/local/bin/healthcheck.sh",
										},
									},
								},
								FailureThreshold:    3,
								InitialDelaySeconds: 10,
								PeriodSeconds:       10,
								SuccessThreshold:    1,
								TimeoutSeconds:      5,
							},
							ImagePullPolicy: df.Spec.ImagePullPolicy,
						},
					},
				},
			},
		},
	}

	return statefulset
}

func applyStatefulSetCustomizations(df *resourcesv1.Dragonfly, statefulset *appsv1.StatefulSet) error {
	container := &statefulset.Spec.Template.Spec.Containers[0]

	if len(df.Spec.InitContainers) > 0 {
		statefulset.Spec.Template.Spec.InitContainers = df.Spec.InitContainers
	}

	// Skip Assigning FileSystem Group. Required for platforms such as Openshift that require IDs to not be set, as it injects a fixed randomized ID per namespace into all pods.
	// Skip Assigning FileSystem Group if podSecurityContext is set as well.
	if !df.Spec.SkipFSGroup && df.Spec.PodSecurityContext == nil {
		statefulset.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup: &dflyUserGroup,
		}
	}

	// set podSecurityContext if one is specified
	if df.Spec.PodSecurityContext != nil {
		statefulset.Spec.Template.Spec.SecurityContext = df.Spec.PodSecurityContext
	}

	// set containerSecurityContext if one is specified
	if df.Spec.ContainerSecurityContext != nil {
		container.SecurityContext = df.Spec.ContainerSecurityContext
	}

	// set only if resources are specified
	if df.Spec.Resources != nil {
		container.Resources = *df.Spec.Resources
	}

	if df.Spec.Args != nil {
		container.Args = append(container.Args, df.Spec.Args...)
	}

	if df.Spec.Cluster != nil && df.Spec.Cluster.Mode == resourcesv1.ClusterModeMultiShard {
		container.Args = append(container.Args, fmt.Sprintf("%s=yes", ClusterModeArg))
		if df.Spec.Cluster.AdminPort != 0 {
			container.Args = upsertArg(container.Args, "--admin_port=", fmt.Sprintf("--admin_port=%d", df.Spec.Cluster.AdminPort))
			upsertEnvVar(&container.Env, "HEALTHCHECK_PORT", fmt.Sprintf("%d", df.Spec.Cluster.AdminPort))
		}
	}

	if df.Spec.MemcachedPort != 0 {
		container.Args = append(container.Args, fmt.Sprintf("%s=%d", MemcachedPortArg, df.Spec.MemcachedPort))
		container.Ports = append(container.Ports, corev1.ContainerPort{
			Name:          MemcachedPortName,
			ContainerPort: df.Spec.MemcachedPort,
		})
	}

	if df.Spec.AclFromSecret != nil {
		statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: AclVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: df.Spec.AclFromSecret.Name,
					Items: []corev1.KeyToPath{
						{
							Key:  df.Spec.AclFromSecret.Key,
							Path: AclFileName,
						},
					},
				},
			},
		})

		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      AclVolumeName,
			MountPath: AclDir,
		})

		container.Args = append(container.Args, fmt.Sprintf("%s=%s/%s", AclFileArg, AclDir, AclFileName))
	}

	// Doc: https://www.dragonflydb.io/blog/a-preview-of-dragonfly-ssd-tiering
	if df.Spec.Tiering != nil {

		tieringVolumeName := "tiering"
		tieringMountName := "/dragonfly/tiering"
		tieringDirName := "vol"

		if df.Spec.Tiering.PersistentVolumeClaimSpec != nil {
			statefulset.Spec.VolumeClaimTemplates = append(statefulset.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        tieringVolumeName,
					Labels:      generateResourceLabels(df),
					Annotations: generateResourceAnnotations(df),
				},
				Spec: *df.Spec.Tiering.PersistentVolumeClaimSpec,
			})

			container.VolumeMounts = append(
				container.VolumeMounts,
				corev1.VolumeMount{
					Name:      tieringVolumeName,
					MountPath: tieringMountName,
				},
			)
		}

		container.Args = append(container.Args, fmt.Sprintf("--tiered_prefix=%s/%s", tieringMountName, tieringDirName))
	}

	if df.Spec.Snapshot != nil {
		// validate mutual exclusivity of PVC spec and existing PVC name
		if df.Spec.Snapshot.PersistentVolumeClaimSpec != nil && df.Spec.Snapshot.ExistingPersistentVolumeClaimName != "" {
			return fmt.Errorf("persistentVolumeClaimSpec and existingPersistentVolumeClaimName are mutually exclusive")
		}

		// err if pvc is not specified & s3 sir is not present while cron is specified
		if df.Spec.Snapshot.Cron != "" && df.Spec.Snapshot.PersistentVolumeClaimSpec == nil && df.Spec.Snapshot.ExistingPersistentVolumeClaimName == "" && df.Spec.Snapshot.Dir == "" {
			return fmt.Errorf("cron specified without a persistent volume claim")
		}

		snapshotDir := df.Spec.Snapshot.Dir
		if df.Spec.Snapshot.Dir == "" {
			snapshotDir = SnapshotsDir
		}

		if df.Spec.Snapshot.PersistentVolumeClaimSpec != nil {
			// attach and use the PVC if specified
			statefulset.Spec.VolumeClaimTemplates = append(statefulset.Spec.VolumeClaimTemplates, corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: SnapshotsVolumeName,
					Labels: map[string]string{
						DragonflyNameLabelKey:     df.Name,
						KubernetesPartOfLabelKey:  KubernetesPartOf,
						KubernetesAppNameLabelKey: KubernetesAppName,
					},
				},
				Spec: *df.Spec.Snapshot.PersistentVolumeClaimSpec,
			})

			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      SnapshotsVolumeName,
				MountPath: snapshotDir,
			})
		}

		if df.Spec.Snapshot.ExistingPersistentVolumeClaimName != "" {
			// use an existing PVC
			statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: SnapshotsVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: df.Spec.Snapshot.ExistingPersistentVolumeClaimName,
					},
				},
			})

			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      SnapshotsVolumeName,
				MountPath: snapshotDir,
			})
		}

		container.Args = append(container.Args, fmt.Sprintf("%s=%s", SnapshotsDirArg, snapshotDir))

		if df.Spec.Snapshot.Cron != "" {
			container.Args = append(container.Args, fmt.Sprintf("%s=%s", SnapshotsCronArg, df.Spec.Snapshot.Cron))
		}
	}

	if df.Spec.TLSSecretRef != nil {
		statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: TLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: df.Spec.TLSSecretRef.Name,
				},
			},
		})

		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      TLSVolumeName,
			ReadOnly:  true,
			MountPath: TLSDir,
		})

		container.Args = append(container.Args, []string{
			// no TLS on admin port by default
			NoTLSOnAdminPortArg,
			TLSArg,
			fmt.Sprintf("%s=%s/%s", TLSCertPathArg, TLSDir, TLSCertFileName),
			fmt.Sprintf("%s=%s/%s", TLSKeyPathArg, TLSDir, TLSKeyFileName),
		}...)
	}

	if df.Spec.Annotations != nil {
		statefulset.Spec.Template.ObjectMeta.Annotations = df.Spec.Annotations
	}

	for key := range df.Spec.Labels {
		// Make sure we do not overwrite any existing labels
		if _, ok := statefulset.Spec.Template.ObjectMeta.Labels[key]; !ok {
			statefulset.Spec.Template.ObjectMeta.Labels[key] = df.Spec.Labels[key]
		}
	}

	if df.Spec.Affinity != nil {
		statefulset.Spec.Template.Spec.Affinity = df.Spec.Affinity
	}

	if df.Spec.NodeSelector != nil {
		statefulset.Spec.Template.Spec.NodeSelector = df.Spec.NodeSelector
	}

	if df.Spec.Tolerations != nil {
		statefulset.Spec.Template.Spec.Tolerations = df.Spec.Tolerations
	}

	if df.Spec.TopologySpreadConstraints != nil {
		statefulset.Spec.Template.Spec.TopologySpreadConstraints = df.Spec.TopologySpreadConstraints
	}

	if df.Spec.ServiceAccountName != "" {
		statefulset.Spec.Template.Spec.ServiceAccountName = df.Spec.ServiceAccountName
	}

	if df.Spec.PriorityClassName != "" {
		statefulset.Spec.Template.Spec.PriorityClassName = df.Spec.PriorityClassName
	}

	if df.Spec.Authentication != nil {
		if df.Spec.Authentication.PasswordFromSecret != nil {
			// load the secret key as a password into env
			container.Env = append(container.Env, corev1.EnvVar{
				Name: "DFLY_requirepass",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: df.Spec.Authentication.PasswordFromSecret,
				},
			})
		}

		if df.Spec.Authentication.ClientCaCertSecret != nil {
			// mount the secrets as a volume
			statefulset.Spec.Template.Spec.Volumes = append(statefulset.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: TLSCACertVolumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: df.Spec.Authentication.ClientCaCertSecret.Name,
						Items: []corev1.KeyToPath{
							{
								Key:  df.Spec.Authentication.ClientCaCertSecret.Key,
								Path: TLSCACertFileName,
							},
						},
					},
				},
			})

			// mount it
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      TLSCACertVolumeName,
				MountPath: TLSCACertDir,
			})

			// pass it as an arg
			container.Args = append(container.Args, fmt.Sprintf("%s=%s/%s", TLSCACertPathArg, TLSCACertDir, TLSCACertFileName))
		}
	}

	statefulset.Spec.Template.Spec.Containers = mergeNamedSlices(
		statefulset.Spec.Template.Spec.Containers, df.Spec.AdditionalContainers,
		func(c corev1.Container) string { return c.Name })

	statefulset.Spec.Template.Spec.Volumes = mergeNamedSlices(
		statefulset.Spec.Template.Spec.Volumes, df.Spec.AdditionalVolumes,
		func(v corev1.Volume) string { return v.Name })

	return nil
}

func upsertArg(args []string, prefix, newValue string) []string {
	for i, arg := range args {
		if strings.HasPrefix(arg, prefix) {
			args[i] = newValue
			return args
		}
	}
	return append(args, newValue)
}

func upsertEnvVar(envs *[]corev1.EnvVar, name, value string) {
	for i := range *envs {
		if (*envs)[i].Name == name {
			(*envs)[i].Value = value
			(*envs)[i].ValueFrom = nil
			return
		}
	}
	*envs = append(*envs, corev1.EnvVar{Name: name, Value: value})
}

// mergeNamedSlices will merge base into override, override takes precendence
func mergeNamedSlices[T any](base, override []T, getName func(T) string) []T {
	existing := make(map[string]bool, len(override))
	for _, item := range override {
		existing[getName(item)] = true
	}

	result := make([]T, len(override))
	copy(result, override)

	for _, item := range base {
		if !existing[getName(item)] {
			result = append(result, item)
		}
	}

	return result
}

func generateResourceLabels(df *resourcesv1.Dragonfly) map[string]string {
	labels := map[string]string{
		KubernetesAppComponentLabelKey: KubernetesAppComponent,
		KubernetesAppInstanceLabelKey:  df.Name,
		KubernetesAppNameLabelKey:      KubernetesAppName,
		KubernetesAppVersionLabelKey:   Version,
		KubernetesPartOfLabelKey:       KubernetesPartOf,
		KubernetesManagedByLabelKey:    DragonflyOperatorName,
		DragonflyNameLabelKey:          df.Name,
	}

	if df.Spec.OwnedObjectsMetadata != nil {
		for key, value := range df.Spec.OwnedObjectsMetadata.Labels {
			if _, ok := labels[key]; !ok {
				labels[key] = value
			}
		}
	}

	return labels
}

func generateResourceAnnotations(df *resourcesv1.Dragonfly) map[string]string {
	annotations := map[string]string{}
	if df.Spec.OwnedObjectsMetadata != nil {
		for key, value := range df.Spec.OwnedObjectsMetadata.Annotations {
			annotations[key] = value
		}
	}

	return annotations
}

// applyClusterAntiAffinity adds pod anti-affinity rules for cluster mode to spread
// pods from different shards across nodes. This ensures high availability by
// preventing all shard masters from running on the same node.
func applyClusterAntiAffinity(df *resourcesv1.Dragonfly, statefulset *appsv1.StatefulSet) {
	// Create preferred anti-affinity to spread pods across nodes
	// We use "preferred" instead of "required" to avoid blocking scheduling
	// when there aren't enough nodes
	antiAffinityTerm := corev1.WeightedPodAffinityTerm{
		Weight: 100,
		PodAffinityTerm: corev1.PodAffinityTerm{
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					DragonflyNameLabelKey:     df.Name,
					KubernetesAppNameLabelKey: KubernetesAppName,
				},
			},
			TopologyKey: "kubernetes.io/hostname",
		},
	}

	podSpec := &statefulset.Spec.Template.Spec

	// If user has specified affinity, we need to create a new Affinity object
	// to avoid mutating the shared spec object across multiple shards
	if podSpec.Affinity == nil {
		podSpec.Affinity = &corev1.Affinity{}
	} else {
		// Deep copy the affinity to avoid mutating the shared spec
		newAffinity := &corev1.Affinity{}
		if podSpec.Affinity.NodeAffinity != nil {
			newAffinity.NodeAffinity = podSpec.Affinity.NodeAffinity.DeepCopy()
		}
		if podSpec.Affinity.PodAffinity != nil {
			newAffinity.PodAffinity = podSpec.Affinity.PodAffinity.DeepCopy()
		}
		if podSpec.Affinity.PodAntiAffinity != nil {
			newAffinity.PodAntiAffinity = podSpec.Affinity.PodAntiAffinity.DeepCopy()
		}
		podSpec.Affinity = newAffinity
	}

	if podSpec.Affinity.PodAntiAffinity == nil {
		podSpec.Affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}

	// Append our anti-affinity rule to any existing preferred rules
	podSpec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = append(
		podSpec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution,
		antiAffinityTerm,
	)
}
