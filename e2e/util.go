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

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func parseTieredEntriesFromInfo(info string) (int64, error) {
	sc := bufio.NewScanner(strings.NewReader(info))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Handle "tiered_entries:<number>" (with optional spaces)
		if strings.HasPrefix(line, "tiered_entries") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				val := strings.TrimSpace(parts[1])
				return strconv.ParseInt(val, 10, 64)
			}
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("tiered_entries not found")
}

func waitForStatefulSetReady(ctx context.Context, c client.Client, name, namespace string, maxDuration time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, maxDuration)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for statefulset to be ready")
		default:
			// Check if the statefulset is ready
			ready, err := isStatefulSetReady(ctx, c, name, namespace)
			if err != nil {
				return err
			}
			if ready {
				return nil
			}
		}
	}
}

func isStatefulSetReady(ctx context.Context, c client.Client, name, namespace string) (bool, error) {
	var statefulSet appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, &statefulSet); err != nil {
		return false, nil
	}

	if statefulSet.Status.ReadyReplicas == *statefulSet.Spec.Replicas && statefulSet.Status.UpdatedReplicas == statefulSet.Status.Replicas {
		return true, nil
	}

	return false, nil
}

func checkAndK8sPortForwardRedis(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, stopChan chan struct{}, name, namespace, password string, port int) (*redis.Client, error) {
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("app=%s", name),
	})
	if err != nil {
		return nil, err
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found")
	}

	var master *corev1.Pod
	for _, pod := range pods.Items {
		if pod.Labels[resources.RoleLabelKey] == resources.Master {
			master = &pod
			break
		}
	}

	if master == nil {
		return nil, fmt.Errorf("no master pod found")
	}

	updatedMaster, err := clientset.CoreV1().Pods(master.Namespace).Get(ctx, master.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s/%s: %w", master.Namespace, master.Name, err)
	}
	master = updatedMaster

	if master.Status.Phase != corev1.PodRunning {
		return nil, fmt.Errorf("pod %s/%s is not running (phase: %s)", master.Namespace, master.Name, master.Status.Phase)
	}

	// Verify pod has Ready condition
	hasReadyCondition := false
	for _, condition := range master.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			hasReadyCondition = true
			break
		}
	}
	if !hasReadyCondition {
		return nil, fmt.Errorf("pod %s/%s is not ready (Ready condition not true)", master.Namespace, master.Name)
	}

	// Verify the Dragonfly container is ready
	containerReady := false
	for _, status := range master.Status.ContainerStatuses {
		if status.Name == resources.DragonflyContainerName {
			if status.Ready {
				containerReady = true
				break
			}
		}
	}
	if !containerReady {
		return nil, fmt.Errorf("container %s in pod %s/%s is not ready", resources.DragonflyContainerName, master.Namespace, master.Name)
	}

	fw, err := portForward(ctx, clientset, config, master, stopChan, port)
	if err != nil {
		return nil, err
	}

	redisOptions := &redis.Options{
		Addr: fmt.Sprintf("localhost:%d", port),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}

	if password != "" {
		redisOptions.Password = password
	}

	redisClient := redis.NewClient(redisOptions)

	errChan := make(chan error, 1)
	go func() { errChan <- fw.ForwardPorts() }()

	select {
	case err = <-errChan:
		return nil, errors.Wrap(err, "unable to forward ports")
	case <-fw.Ready:
	}

	pingCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	err = redisClient.Ping(pingCtx).Err()
	if err != nil {
		return nil, fmt.Errorf("unable to ping instance: %w", err)
	}

	return redisClient, nil
}

type slotRange struct {
	Start int64
	End   int64
}

type slotOwner struct {
	Start      int64
	End        int64
	MasterIP   string
	MasterPort int64
}

func computeKeySlot(key string) (int, error) {
	if key == "" {
		return 0, fmt.Errorf("key must not be empty")
	}
	tag := extractHashTag(key)
	slot := int(crc16([]byte(tag)) % 16384)
	return slot, nil
}

func extractHashTag(key string) string {
	start := strings.IndexByte(key, '{')
	if start == -1 {
		return key
	}
	end := strings.IndexByte(key[start+1:], '}')
	if end == -1 {
		return key
	}
	end += start + 1
	if end == start+1 {
		return key
	}
	return key[start+1 : end]
}

func getClusterSlotRanges(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, adminPort int) ([]slotRange, error) {
	result, err := setupPortForwardWithCleanup(ctx, clientset, config, pod, adminPort, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer result.Cleanup()

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", result.LocalPort),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.Do(ctx, "CLUSTER", "SHARDS").Result()
	if err != nil {
		return nil, err
	}

	ranges, err := parseClusterShardSlots(resp)
	if err != nil {
		return nil, err
	}

	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].Start == ranges[j].Start {
			return ranges[i].End < ranges[j].End
		}
		return ranges[i].Start < ranges[j].Start
	})

	return ranges, nil
}

func getClusterSlotOwners(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, adminPort int) ([]slotOwner, error) {
	result, err := setupPortForwardWithCleanup(ctx, clientset, config, pod, adminPort, 10*time.Second)
	if err != nil {
		return nil, err
	}
	defer result.Cleanup()

	redisClient := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("localhost:%d", result.LocalPort),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer redisClient.Close()

	resp, err := redisClient.Do(ctx, "CLUSTER", "SHARDS").Result()
	if err != nil {
		return nil, err
	}

	owners, err := parseClusterShardOwners(resp)
	if err != nil {
		return nil, err
	}

	sort.Slice(owners, func(i, j int) bool {
		if owners[i].Start == owners[j].Start {
			return owners[i].End < owners[j].End
		}
		return owners[i].Start < owners[j].Start
	})

	return owners, nil
}

func parseClusterShardSlots(resp interface{}) ([]slotRange, error) {
	shards, ok := resp.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected cluster shards response type: %T", resp)
	}

	var ranges []slotRange
	for _, shard := range shards {
		items, ok := shard.([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected shard entry type: %T", shard)
		}
		for i := 0; i+1 < len(items); i += 2 {
			key, ok := items[i].(string)
			if !ok {
				continue
			}
			if key != "slots" {
				continue
			}
			slotItems, ok := items[i+1].([]interface{})
			if !ok {
				return nil, fmt.Errorf("unexpected slots type: %T", items[i+1])
			}
			// Dragonfly returns slots as a flat array [start, end, start, end, ...]
			// Parse pairs from the flat list
			for j := 0; j+1 < len(slotItems); j += 2 {
				start, err := toInt64(slotItems[j])
				if err != nil {
					return nil, fmt.Errorf("failed to parse slot start: %w", err)
				}
				end, err := toInt64(slotItems[j+1])
				if err != nil {
					return nil, fmt.Errorf("failed to parse slot end: %w", err)
				}
				ranges = append(ranges, slotRange{Start: start, End: end})
			}
		}
	}

	return ranges, nil
}

func parseClusterShardOwners(resp interface{}) ([]slotOwner, error) {
	shards, ok := resp.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected cluster shards response type: %T", resp)
	}

	var owners []slotOwner
	for _, shard := range shards {
		items, ok := shard.([]interface{})
		if !ok {
			return nil, fmt.Errorf("unexpected shard entry type: %T", shard)
		}

		var ranges []slotRange
		var masterIP string
		var masterPort int64

		for i := 0; i+1 < len(items); i += 2 {
			key, ok := items[i].(string)
			if !ok {
				continue
			}
			switch key {
			case "slots":
				slotItems, ok := items[i+1].([]interface{})
				if !ok {
					return nil, fmt.Errorf("unexpected slots type: %T", items[i+1])
				}
				for j := 0; j+1 < len(slotItems); j += 2 {
					start, err := toInt64(slotItems[j])
					if err != nil {
						return nil, fmt.Errorf("failed to parse slot start: %w", err)
					}
					end, err := toInt64(slotItems[j+1])
					if err != nil {
						return nil, fmt.Errorf("failed to parse slot end: %w", err)
					}
					ranges = append(ranges, slotRange{Start: start, End: end})
				}
			case "nodes":
				nodeItems, ok := items[i+1].([]interface{})
				if !ok {
					return nil, fmt.Errorf("unexpected nodes type: %T", items[i+1])
				}
				for _, node := range nodeItems {
					nodeEntry, ok := node.([]interface{})
					if !ok {
						return nil, fmt.Errorf("unexpected node entry type: %T", node)
					}
					role := ""
					ip := ""
					endpoint := ""
					var port int64
					for k := 0; k+1 < len(nodeEntry); k += 2 {
						nodeKey, ok := nodeEntry[k].(string)
						if !ok {
							continue
						}
						switch nodeKey {
						case "role":
							role = toString(nodeEntry[k+1])
						case "endpoint":
							endpoint = toString(nodeEntry[k+1])
						case "ip":
							ip = toString(nodeEntry[k+1])
						case "port":
							parsedPort, err := toInt64(nodeEntry[k+1])
							if err == nil {
								port = parsedPort
							}
						}
					}
					if role == "master" {
						if endpoint != "" {
							masterIP = endpoint
						} else {
							masterIP = ip
						}
						masterPort = port
						break
					}
				}
			}
		}

		if masterIP == "" || len(ranges) == 0 {
			continue
		}

		for _, r := range ranges {
			owners = append(owners, slotOwner{
				Start:      r.Start,
				End:        r.End,
				MasterIP:   masterIP,
				MasterPort: masterPort,
			})
		}
	}

	return owners, nil
}

func toInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case uint64:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected numeric type: %T", value)
	}
}

func toString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func findMasterPodForSlot(slot int, owners []slotOwner, pods []corev1.Pod) (*corev1.Pod, error) {
	var ownerIP string
	for _, owner := range owners {
		if int64(slot) >= owner.Start && int64(slot) <= owner.End {
			ownerIP = owner.MasterIP
			break
		}
	}
	if ownerIP == "" {
		return nil, fmt.Errorf("no slot owner found for slot %d", slot)
	}

	for i := range pods {
		if pods[i].Status.PodIP == ownerIP {
			return &pods[i], nil
		}
	}
	return nil, fmt.Errorf("no pod found for slot owner ip %s", ownerIP)
}

var crc16Table = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
	0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
	0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
	0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
	0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
	0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
	0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
	0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}

func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = (crc << 8) ^ crc16Table[((crc>>8)^uint16(b))&0x00ff]
	}
	return crc
}

type portForwardResult struct {
	LocalPort int
	Cleanup   func()
}

// setupPortForwardWithCleanup sets up port forwarding with proper cleanup handling
// it finds an available local port, forwards to the pod's remote port, and returns
// the local port and a cleanup function. The cleanup function must be called to
// ensure the port is released.
func setupPortForwardWithCleanup(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, remotePort int, timeout time.Duration) (*portForwardResult, error) {
	// Fetch pod to get latest status
	updatedPod, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	pod = updatedPod

	// Verify pod is running
	if pod.Status.Phase != corev1.PodRunning {
		return nil, fmt.Errorf("pod %s/%s is not running (phase: %s)", pod.Namespace, pod.Name, pod.Status.Phase)
	}

	// Verify pod has Ready condition
	hasReadyCondition := false
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			hasReadyCondition = true
			break
		}
	}
	if !hasReadyCondition {
		return nil, fmt.Errorf("pod %s/%s is not ready (Ready condition not true)", pod.Namespace, pod.Name)
	}

	// Verify the Dragonfly container is ready
	containerReady := false
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == resources.DragonflyContainerName {
			if status.Ready {
				containerReady = true
				break
			}
		}
	}
	if !containerReady {
		return nil, fmt.Errorf("container %s in pod %s/%s is not ready", resources.DragonflyContainerName, pod.Namespace, pod.Name)
	}

	localStopChan := make(chan struct{}, 1)
	portForwardDone := make(chan struct{}, 1)
	portForwardClosed := new(bool)

	// Port forward to pod
	url := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(pod.Namespace).
		Name(pod.Name).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)

	// Find an available local port
	var localPort int
	maxPortAttempts := 10
	for i := 0; i < maxPortAttempts; i++ {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			if i == maxPortAttempts-1 {
				return nil, fmt.Errorf("unable to find available port after %d attempts: %w", maxPortAttempts, err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		localPort = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		time.Sleep(100 * time.Millisecond)
		break
	}

	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	readyChan := make(chan struct{}, 1)

	fw, err := portforward.New(dialer, ports, localStopChan, readyChan, io.Discard, os.Stderr)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error, 1)
	go func() {
		defer close(portForwardDone)
		errChan <- fw.ForwardPorts()
	}()

	// Wait for port forward to be ready or error
	portForwardCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case err = <-errChan:
		// Port forward failed, close and wait for cleanup
		if !*portForwardClosed && localStopChan != nil {
			close(localStopChan)
			*portForwardClosed = true
		}
		select {
		case <-portForwardDone:
		case <-time.After(2 * time.Second):
		}
		time.Sleep(500 * time.Millisecond)
		return nil, errors.Wrap(err, "unable to forward ports")
	case <-fw.Ready:
		// Port forward ready, wait a bit to establish connection
		select {
		case <-time.After(500 * time.Millisecond):
		case <-portForwardCtx.Done():
			if !*portForwardClosed && localStopChan != nil {
				close(localStopChan)
				*portForwardClosed = true
			}
			select {
			case <-portForwardDone:
			case <-time.After(2 * time.Second):
			}
			time.Sleep(500 * time.Millisecond)
			return nil, portForwardCtx.Err()
		}
	case <-portForwardCtx.Done():
		// Timeout waiting for port forward to be ready
		if !*portForwardClosed && localStopChan != nil {
			close(localStopChan)
			*portForwardClosed = true
		}
		select {
		case <-portForwardDone:
		case <-time.After(2 * time.Second):
		}
		time.Sleep(500 * time.Millisecond)
		return nil, fmt.Errorf("timeout waiting for port forward to be ready: %w", portForwardCtx.Err())
	}

	cleanup := func() {
		if !*portForwardClosed && localStopChan != nil {
			close(localStopChan)
			*portForwardClosed = true
		}
		select {
		case <-portForwardDone:
		case <-time.After(3 * time.Second):
		}
		time.Sleep(1 * time.Second)
	}

	return &portForwardResult{
		LocalPort: localPort,
		Cleanup:   cleanup,
	}, nil
}

func portForward(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, stopChan chan struct{}, port int) (*portforward.PortForwarder, error) {
	url := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(pod.Namespace).
		Name(pod.Name).
		SubResource("portforward").
		URL()

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)
	ports := []string{fmt.Sprintf("%d:%d", port, resources.DragonflyPort)}
	readyChan := make(chan struct{}, 1)

	fw, err := portforward.New(dialer, ports, stopChan, readyChan, io.Discard, os.Stderr)
	if err != nil {
		return nil, err
	}
	return fw, err
}

// checkPersistenceInfo checks the persistence info from a pod's admin port
// Uses port forwarding to access the pod's admin port
// ensures container is Ready before attempting connection
func checkPersistenceInfo(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, stopChan chan struct{}) (loading string, loadState string, err error) {
	// Retry with different ports if we get port conflicts
	maxRetries := 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		loading, loadState, err = tryCheckPersistenceInfo(ctx, clientset, config, pod, stopChan)
		if err == nil {
			return loading, loadState, nil
		}

		// Check if error is due to container not being ready
		errStr := err.Error()
		isNotReadyError := strings.Contains(errStr, "is not ready") ||
			strings.Contains(errStr, "not running") ||
			strings.Contains(errStr, "Ready condition not true")

		// Retry on port conflicts or container not ready
		isRetryableError := strings.Contains(errStr, "address already in use") ||
			strings.Contains(errStr, "bind") ||
			strings.Contains(errStr, "unable to forward ports") ||
			isNotReadyError

		if !isRetryableError {
			return "", "", err
		}

		if attempt < maxRetries-1 {
			// Exponential backoff, wait longer if container is not ready
			backoff := time.Duration(attempt+1) * 200 * time.Millisecond
			if isNotReadyError {
				backoff = time.Duration(attempt+1) * 1 * time.Second
			}
			cleanupWait := 2 * time.Second
			time.Sleep(backoff + cleanupWait)
		}
	}
	return "", "", err
}

func tryCheckPersistenceInfo(ctx context.Context, clientset *kubernetes.Clientset, config *rest.Config, pod *corev1.Pod, stopChan chan struct{}) (loading string, loadState string, err error) {
	// Setup port forwarding with proper cleanup
	pfResult, err := setupPortForwardWithCleanup(ctx, clientset, config, pod, resources.DragonflyAdminPort, 30*time.Second)
	if err != nil {
		return "", "", err
	}
	defer pfResult.Cleanup()

	adminClient := redis.NewClient(&redis.Options{
		Addr:                  fmt.Sprintf("localhost:%d", pfResult.LocalPort),
		DialTimeout:           15 * time.Second,
		ReadTimeout:           10 * time.Second,
		WriteTimeout:          10 * time.Second,
		ContextTimeoutEnabled: true,
	})
	defer adminClient.Close()

	infoCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	info, err := adminClient.Info(infoCtx, "persistence").Result()
	if err != nil {
		return "", "", fmt.Errorf("unable to get persistence info: %w", err)
	}

	// Parse info output
	sc := bufio.NewScanner(strings.NewReader(info))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			if key == "loading" {
				loading = value
			}
			if key == "load_state" {
				loadState = value
			}
		}
	}

	return loading, loadState, sc.Err()
}
