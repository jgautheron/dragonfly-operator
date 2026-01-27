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
	"strings"
	"testing"

	dfv1alpha1 "github.com/dragonflydb/dragonfly-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func TestComputeMigrationPlan_ScaleUp(t *testing.T) {
	tests := []struct {
		name            string
		previousShards  int32
		targetShards    int32
		wantMigrations  int
		wantScaleUp     bool
		wantScaleDown   bool
	}{
		{
			name:           "2 to 4 shards",
			previousShards: 2,
			targetShards:   4,
			// shard-0 [0-8191] -> shard-1 [4096-8191], shard-1 [8192-16383] -> shard-2, shard-3
			wantMigrations: 3,
			wantScaleUp:    true,
			wantScaleDown:  false,
		},
		{
			name:           "1 to 2 shards",
			previousShards: 1,
			targetShards:   2,
			wantMigrations: 1, // shard-0 [0-16383] -> shard-1 [8192-16383]
			wantScaleUp:    true,
			wantScaleDown:  false,
		},
		{
			name:           "2 to 3 shards",
			previousShards: 2,
			targetShards:   3,
			// shard-0 [0-8191] -> shard-1 [5462-8191], shard-1 [8192-16383] -> shard-2 [10923-16383]
			wantMigrations: 2,
			wantScaleUp:    true,
			wantScaleDown:  false,
		},
		{
			name:           "no change",
			previousShards: 2,
			targetShards:   2,
			wantMigrations: 0,
			wantScaleUp:    false,
			wantScaleDown:  false,
		},
		{
			name:           "from zero (initial)",
			previousShards: 0,
			targetShards:   2,
			wantMigrations: 0, // No migration needed for initial deployment
			wantScaleUp:    false,
			wantScaleDown:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := computeMigrationPlan(tt.previousShards, tt.targetShards)

			if len(plan.Migrations) != tt.wantMigrations {
				t.Errorf("got %d migrations, want %d", len(plan.Migrations), tt.wantMigrations)
				for i, m := range plan.Migrations {
					t.Logf("migration %d: %s -> %s, slots %v", i, m.SourceShard, m.TargetShard, m.SlotRanges)
				}
			}

			if plan.IsScaleUp != tt.wantScaleUp {
				t.Errorf("got IsScaleUp=%v, want %v", plan.IsScaleUp, tt.wantScaleUp)
			}

			if plan.IsScaleDown != tt.wantScaleDown {
				t.Errorf("got IsScaleDown=%v, want %v", plan.IsScaleDown, tt.wantScaleDown)
			}
		})
	}
}

func TestComputeMigrationPlan_ScaleDown(t *testing.T) {
	tests := []struct {
		name            string
		previousShards  int32
		targetShards    int32
		wantMigrations  int
		wantScaleUp     bool
		wantScaleDown   bool
	}{
		{
			name:           "4 to 2 shards",
			previousShards: 4,
			targetShards:   2,
			// shard-1 -> shard-0 (4096-8191), shard-2 -> shard-1, shard-3 -> shard-1
			wantMigrations: 3,
			wantScaleUp:    false,
			wantScaleDown:  true,
		},
		{
			name:           "2 to 1 shard",
			previousShards: 2,
			targetShards:   1,
			wantMigrations: 1, // shard-1 -> shard-0
			wantScaleUp:    false,
			wantScaleDown:  true,
		},
		{
			name:           "3 to 2 shards",
			previousShards: 3,
			targetShards:   2,
			// shard-1 -> shard-0 (5462-8191), shard-2 -> shard-1
			wantMigrations: 2,
			wantScaleUp:    false,
			wantScaleDown:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := computeMigrationPlan(tt.previousShards, tt.targetShards)

			if len(plan.Migrations) != tt.wantMigrations {
				t.Errorf("got %d migrations, want %d", len(plan.Migrations), tt.wantMigrations)
				for i, m := range plan.Migrations {
					t.Logf("migration %d: %s -> %s, slots %v", i, m.SourceShard, m.TargetShard, m.SlotRanges)
				}
			}

			if plan.IsScaleUp != tt.wantScaleUp {
				t.Errorf("got IsScaleUp=%v, want %v", plan.IsScaleUp, tt.wantScaleUp)
			}

			if plan.IsScaleDown != tt.wantScaleDown {
				t.Errorf("got IsScaleDown=%v, want %v", plan.IsScaleDown, tt.wantScaleDown)
			}
		})
	}
}

func TestComputeMigrationPlan_SlotCoverage(t *testing.T) {
	// Test that scale-up from 2 to 4 shards covers all slots correctly
	plan := computeMigrationPlan(2, 4)

	if !plan.IsScaleUp {
		t.Fatal("expected scale-up")
	}

	// Verify migrations exist
	if len(plan.Migrations) == 0 {
		t.Fatal("expected at least one migration")
	}

	// Verify all migrations have pending status
	for _, m := range plan.Migrations {
		if m.Status != dfv1alpha1.MigrationStatePending {
			t.Errorf("migration %s -> %s has status %s, want %s",
				m.SourceShard, m.TargetShard, m.Status, dfv1alpha1.MigrationStatePending)
		}
	}

	// Collect all slots being migrated
	migratedSlots := make(map[int32]bool)
	for _, m := range plan.Migrations {
		for _, sr := range m.SlotRanges {
			for slot := sr.Start; slot <= sr.End; slot++ {
				if migratedSlots[slot] {
					t.Errorf("slot %d is being migrated multiple times", slot)
				}
				migratedSlots[slot] = true
			}
		}
	}

	// Verify that slots are properly redistributed
	// For 2->4 scaling:
	// Original distribution: shard-0 [0-8191], shard-1 [8192-16383]
	// Target distribution: shard-0 [0-4095], shard-1 [4096-8191], shard-2 [8192-12287], shard-3 [12288-16383]
	// Migrations:
	//   shard-0 [4096-8191] -> shard-1 (4096 slots)
	//   shard-1 [8192-12287] -> shard-2 (4096 slots)
	//   shard-1 [12288-16383] -> shard-3 (4096 slots)
	// Total: 12288 slots migrated

	t.Logf("Total slots migrated: %d", len(migratedSlots))

	// Should migrate 12288 slots (shard-0 gives 4096 to shard-1, shard-1 gives 8192 to shard-2 and shard-3)
	if len(migratedSlots) < 12000 || len(migratedSlots) > 12500 {
		t.Errorf("expected ~12288 slots migrated, got %d", len(migratedSlots))
	}
}

func TestComputeMigrationPlan_ScaleDownSlotCoverage(t *testing.T) {
	// Test that scale-down from 4 to 2 shards drains all slots from removed shards
	plan := computeMigrationPlan(4, 2)

	if !plan.IsScaleDown {
		t.Fatal("expected scale-down")
	}

	if len(plan.Migrations) == 0 {
		t.Fatal("expected at least one migration")
	}

	// Verify all source shards are being removed (shard-2, shard-3)
	sourceShards := make(map[string]bool)
	for _, m := range plan.Migrations {
		sourceShards[m.SourceShard] = true
	}

	if !sourceShards["shard-2"] {
		t.Error("expected shard-2 to be a migration source")
	}
	if !sourceShards["shard-3"] {
		t.Error("expected shard-3 to be a migration source")
	}

	// Verify target shards are remaining shards (shard-0, shard-1)
	targetShards := make(map[string]bool)
	for _, m := range plan.Migrations {
		targetShards[m.TargetShard] = true
	}

	if targetShards["shard-2"] || targetShards["shard-3"] {
		t.Error("removed shards should not be migration targets")
	}

	// Collect all slots being migrated (should be all slots from shard-2 and shard-3)
	migratedSlots := make(map[int32]bool)
	for _, m := range plan.Migrations {
		for _, sr := range m.SlotRanges {
			for slot := sr.Start; slot <= sr.End; slot++ {
				migratedSlots[slot] = true
			}
		}
	}

	// Should migrate 12288 slots:
	// shard-1 -> shard-0 (4096 slots), shard-2 -> shard-1 (4096), shard-3 -> shard-1 (4096)
	t.Logf("Total slots migrated in scale-down: %d", len(migratedSlots))
	if len(migratedSlots) < 12000 || len(migratedSlots) > 12500 {
		t.Errorf("expected ~12288 slots migrated, got %d", len(migratedSlots))
	}
}

func TestComputeSlotRanges_Distribution(t *testing.T) {
	tests := []struct {
		name      string
		numShards int32
		wantSlots int
	}{
		{
			name:      "1 shard",
			numShards: 1,
			wantSlots: totalSlots,
		},
		{
			name:      "2 shards",
			numShards: 2,
			wantSlots: totalSlots / 2,
		},
		{
			name:      "4 shards",
			numShards: 4,
			wantSlots: totalSlots / 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := computeSlotRanges(tt.numShards)

			if len(ranges) != int(tt.numShards) {
				t.Errorf("got %d ranges, want %d", len(ranges), tt.numShards)
			}

			// Verify all slots are covered exactly once
			allSlots := make([]bool, totalSlots)
			for shardName, slotRanges := range ranges {
				for _, sr := range slotRanges {
					for slot := sr.Start; slot <= sr.End; slot++ {
						if allSlots[slot] {
							t.Errorf("slot %d is covered multiple times", slot)
						}
						allSlots[slot] = true
					}
				}
				t.Logf("%s: %v", shardName, slotRanges)
			}

			// Verify no gaps
			for i, covered := range allSlots {
				if !covered {
					t.Errorf("slot %d is not covered", i)
				}
			}
		})
	}
}

func TestDetectScaleChange(t *testing.T) {
	tests := []struct {
		name           string
		specShards     int32
		previousShards int32
		wantScaleUp    bool
		wantScaleDown  bool
		wantDelta      int32
	}{
		{
			name:           "scale up 2 to 4",
			specShards:     4,
			previousShards: 2,
			wantScaleUp:    true,
			wantScaleDown:  false,
			wantDelta:      2,
		},
		{
			name:           "scale down 4 to 2",
			specShards:     2,
			previousShards: 4,
			wantScaleUp:    false,
			wantScaleDown:  true,
			wantDelta:      2,
		},
		{
			name:           "no change",
			specShards:     2,
			previousShards: 2,
			wantScaleUp:    false,
			wantScaleDown:  false,
			wantDelta:      0,
		},
		{
			name:           "initial deployment (previousShards=0)",
			specShards:     2,
			previousShards: 0,
			wantScaleUp:    false,
			wantScaleDown:  false,
			wantDelta:      0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dfi := &DragonflyInstance{
				df: &dfv1alpha1.Dragonfly{
					Spec: dfv1alpha1.DragonflySpec{
						Cluster: &dfv1alpha1.ClusterSpec{
							Shards: tt.specShards,
						},
					},
					Status: dfv1alpha1.DragonflyStatus{
						Cluster: &dfv1alpha1.ClusterStatus{
							PreviousShards: tt.previousShards,
						},
					},
				},
			}

			scaleUp, scaleDown, delta := dfi.detectScaleChange()

			if scaleUp != tt.wantScaleUp {
				t.Errorf("got scaleUp=%v, want %v", scaleUp, tt.wantScaleUp)
			}
			if scaleDown != tt.wantScaleDown {
				t.Errorf("got scaleDown=%v, want %v", scaleDown, tt.wantScaleDown)
			}
			if delta != tt.wantDelta {
				t.Errorf("got delta=%d, want %d", delta, tt.wantDelta)
			}
		})
	}
}

func TestDetectScaleChange_NilCluster(t *testing.T) {
	// Test with nil cluster spec
	dfi := &DragonflyInstance{
		df: &dfv1alpha1.Dragonfly{
			Spec: dfv1alpha1.DragonflySpec{
				Cluster: nil,
			},
		},
	}

	scaleUp, scaleDown, delta := dfi.detectScaleChange()

	if scaleUp || scaleDown || delta != 0 {
		t.Errorf("expected no scale change for nil cluster, got scaleUp=%v, scaleDown=%v, delta=%d",
			scaleUp, scaleDown, delta)
	}
}

func TestValidateScaleDownMasters(t *testing.T) {
	t.Run("errors when draining shard is missing master", func(t *testing.T) {
		masters := map[string]*corev1.Pod{
			"shard-0": {},
			"shard-1": {},
			// shard-2 missing
			"shard-3": {},
		}

		err := validateScaleDownMasters(masters, 4, 2)
		if err == nil {
			t.Fatalf("expected error when missing draining shard master")
		}
	})

	t.Run("no error when all draining masters present", func(t *testing.T) {
		masters := map[string]*corev1.Pod{
			"shard-0": {},
			"shard-1": {},
			"shard-2": {},
			"shard-3": {},
		}

		err := validateScaleDownMasters(masters, 4, 2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestParseMigrationStatus(t *testing.T) {
	t.Run("handles array of bulk strings", func(t *testing.T) {
		result := []interface{}{
			[]byte("in"),
			[]byte("node-id"),
			[]byte("FINISHED"),
			[]byte("100"),
			[]byte("0"),
		}
		status := parseMigrationStatus(result)
		if !strings.Contains(status, "FINISHED") {
			t.Fatalf("expected FINISHED in status, got %q", status)
		}
	})

	t.Run("handles array of strings", func(t *testing.T) {
		result := []interface{}{"out", "node-id", "FINISHED", "0", "0"}
		status := parseMigrationStatus(result)
		if !strings.Contains(status, "FINISHED") {
			t.Fatalf("expected FINISHED in status, got %q", status)
		}
	})

	t.Run("handles empty array", func(t *testing.T) {
		result := []interface{}{}
		status := parseMigrationStatus(result)
		if status != "" {
			t.Fatalf("expected empty status, got %q", status)
		}
	})

	t.Run("handles string response", func(t *testing.T) {
		status := parseMigrationStatus("NO_MIGRATIONS")
		if status != "NO_MIGRATIONS" {
			t.Fatalf("expected NO_MIGRATIONS, got %q", status)
		}
	})
}
