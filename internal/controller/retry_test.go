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
	"errors"
	"testing"
	"time"

	"github.com/dragonflydb/dragonfly-operator/internal/resources"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		attempts := 0
		err := retryWithBackoff(context.Background(), 3, 10*time.Millisecond, func() error {
			attempts++
			return nil
		})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if attempts != 1 {
			t.Errorf("expected 1 attempt, got %d", attempts)
		}
	})

	t.Run("succeeds on second attempt", func(t *testing.T) {
		attempts := 0
		err := retryWithBackoff(context.Background(), 3, 10*time.Millisecond, func() error {
			attempts++
			if attempts < 2 {
				return errors.New("transient error")
			}
			return nil
		})
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if attempts != 2 {
			t.Errorf("expected 2 attempts, got %d", attempts)
		}
	})

	t.Run("fails after max attempts", func(t *testing.T) {
		attempts := 0
		expectedErr := errors.New("persistent error")
		err := retryWithBackoff(context.Background(), 3, 10*time.Millisecond, func() error {
			attempts++
			return expectedErr
		})
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
		if attempts != 3 {
			t.Errorf("expected 3 attempts, got %d", attempts)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		attempts := 0
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()
		err := retryWithBackoff(ctx, 10, 50*time.Millisecond, func() error {
			attempts++
			return errors.New("error")
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled error, got %v", err)
		}
		// Should have only attempted a few times before cancellation
		if attempts >= 10 {
			t.Errorf("expected fewer than 10 attempts due to cancellation, got %d", attempts)
		}
	})

	t.Run("exponential backoff timing", func(t *testing.T) {
		attempts := 0
		start := time.Now()
		_ = retryWithBackoff(context.Background(), 3, 20*time.Millisecond, func() error {
			attempts++
			return errors.New("error")
		})
		elapsed := time.Since(start)
		// Expected delays: 20ms (after 1st fail) + 40ms (after 2nd fail) = 60ms minimum
		if elapsed < 50*time.Millisecond {
			t.Errorf("expected at least 50ms elapsed for exponential backoff, got %v", elapsed)
		}
	})
}

// isWithinMasterGracePeriod checks if a pod's masterSince annotation indicates
// it became master within the given grace period. Returns true if within grace,
// false if outside grace or annotation missing/invalid.
func isWithinMasterGracePeriod(pod *corev1.Pod, gracePeriod time.Duration) bool {
	if pod == nil || pod.Annotations == nil {
		return false
	}
	masterSinceStr, ok := pod.Annotations[resources.MasterSinceAnnotationKey]
	if !ok {
		return false
	}
	masterSince, err := time.Parse(time.RFC3339, masterSinceStr)
	if err != nil {
		return false
	}
	return time.Since(masterSince) < gracePeriod
}

func TestMasterGracePeriod(t *testing.T) {
	gracePeriod := 10 * time.Second

	t.Run("returns false when pod is nil", func(t *testing.T) {
		if isWithinMasterGracePeriod(nil, gracePeriod) {
			t.Error("expected false for nil pod")
		}
	})

	t.Run("returns false when annotations are nil", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
			},
		}
		if isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected false when annotations are nil")
		}
	})

	t.Run("returns false when masterSince annotation is missing", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-pod",
				Annotations: map[string]string{"other": "value"},
			},
		}
		if isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected false when masterSince annotation is missing")
		}
	})

	t.Run("returns false when masterSince annotation is invalid", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Annotations: map[string]string{
					resources.MasterSinceAnnotationKey: "not-a-timestamp",
				},
			},
		}
		if isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected false when masterSince annotation is invalid")
		}
	})

	t.Run("returns true when within grace period", func(t *testing.T) {
		// Set masterSince to 1 second ago (within 10s grace)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Annotations: map[string]string{
					resources.MasterSinceAnnotationKey: time.Now().Add(-1 * time.Second).Format(time.RFC3339),
				},
			},
		}
		if !isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected true when within grace period (1s ago)")
		}
	})

	t.Run("returns false when outside grace period", func(t *testing.T) {
		// Set masterSince to 20 seconds ago (outside 10s grace)
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Annotations: map[string]string{
					resources.MasterSinceAnnotationKey: time.Now().Add(-20 * time.Second).Format(time.RFC3339),
				},
			},
		}
		if isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected false when outside grace period (20s ago)")
		}
	})

	t.Run("returns false when masterSince is exactly at grace boundary", func(t *testing.T) {
		// Set masterSince to exactly 10 seconds ago (at boundary)
		// Due to time.Since, this should be >= gracePeriod, so returns false
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-pod",
				Annotations: map[string]string{
					resources.MasterSinceAnnotationKey: time.Now().Add(-10 * time.Second).Format(time.RFC3339),
				},
			},
		}
		// At exactly the boundary, time.Since >= gracePeriod, so should return false
		if isWithinMasterGracePeriod(pod, gracePeriod) {
			t.Error("expected false when at exact grace period boundary")
		}
	})
}
