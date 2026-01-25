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
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
)

func TestGenerateBackupSetID(t *testing.T) {
	id := GenerateBackupSetID()

	// Verify format is valid
	assert.NotEmpty(t, id)

	// Verify it can be parsed back
	_, err := time.Parse(BackupSetIDFormat, id)
	assert.NoError(t, err, "backup set ID should be parseable with the defined format")
}

func TestBackupSetIDFormat(t *testing.T) {
	// Verify the format produces unique IDs at different times
	id1 := time.Now().UTC().Format(BackupSetIDFormat)
	time.Sleep(1 * time.Second)
	id2 := time.Now().UTC().Format(BackupSetIDFormat)

	assert.NotEqual(t, id1, id2, "IDs generated at different times should differ")
}

func TestCronParsing(t *testing.T) {
	tests := []struct {
		name  string
		cron  string
		valid bool
	}{
		{
			name:  "hourly cron",
			cron:  "0 * * * *",
			valid: true,
		},
		{
			name:  "daily at midnight",
			cron:  "0 0 * * *",
			valid: true,
		},
		{
			name:  "every 5 minutes",
			cron:  "*/5 * * * *",
			valid: true,
		},
		{
			name:  "weekly on sunday",
			cron:  "0 0 * * 0",
			valid: true,
		},
		{
			name:  "invalid cron - too few fields",
			cron:  "* * *",
			valid: false,
		},
		{
			name:  "invalid cron - bad value",
			cron:  "99 * * * *",
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cron.ParseStandard(tt.cron)
			if tt.valid {
				assert.NoError(t, err, "cron expression should be valid")
			} else {
				assert.Error(t, err, "cron expression should be invalid")
			}
		})
	}
}

func TestCronNextRun(t *testing.T) {
	// Test that cron.Next() correctly calculates the next run time
	schedule, err := cron.ParseStandard("0 * * * *") // Every hour at minute 0
	assert.NoError(t, err)

	// Set a reference time at 10:30
	ref := time.Date(2026, 1, 24, 10, 30, 0, 0, time.UTC)
	next := schedule.Next(ref)

	// Next run should be at 11:00
	expected := time.Date(2026, 1, 24, 11, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestCronNextRunAfterMidnight(t *testing.T) {
	// Test daily cron around midnight
	schedule, err := cron.ParseStandard("0 0 * * *") // Every day at midnight
	assert.NoError(t, err)

	// Set a reference time at 23:30 on Jan 24
	ref := time.Date(2026, 1, 24, 23, 30, 0, 0, time.UTC)
	next := schedule.Next(ref)

	// Next run should be at 00:00 on Jan 25
	expected := time.Date(2026, 1, 25, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestParsePersistenceStatus(t *testing.T) {
	info := strings.Join([]string{
		"# Persistence",
		"rdb_bgsave_in_progress:0",
		"rdb_last_bgsave_status:ok",
		"rdb_last_save_time:1700000000",
		"loading:0",
		"load_state:done",
		"",
	}, "\n")

	status := parsePersistenceStatus(info)
	assert.False(t, status.isSaving)
	assert.True(t, status.lastSaveOK)
	assert.True(t, status.hasLastSave)
	assert.Equal(t, time.Unix(1700000000, 0).UTC(), status.lastSaveTime)
	assert.Equal(t, "0", status.loadingStatus)
	assert.Equal(t, "done", status.loadState)
}

func TestEvaluateBackupCompletion(t *testing.T) {
	startedAt := time.Unix(1700000000, 0).UTC()

	t.Run("in progress", func(t *testing.T) {
		status := &persistenceStatus{
			isSaving:     true,
			hasLastSave:  true,
			lastSaveOK:   true,
			lastSaveTime: startedAt.Add(10 * time.Second),
		}
		completed, failed := evaluateBackupCompletion(startedAt, status)
		assert.False(t, completed)
		assert.False(t, failed)
	})

	t.Run("missing last save time", func(t *testing.T) {
		status := &persistenceStatus{
			isSaving:   false,
			lastSaveOK: true,
		}
		completed, failed := evaluateBackupCompletion(startedAt, status)
		assert.False(t, completed)
		assert.False(t, failed)
	})

	t.Run("last save before backup", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:  true,
			lastSaveOK:   true,
			lastSaveTime: startedAt.Add(-10 * time.Second),
		}
		completed, failed := evaluateBackupCompletion(startedAt, status)
		assert.False(t, completed)
		assert.False(t, failed)
	})

	t.Run("failed save", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:  true,
			lastSaveOK:   false,
			lastSaveTime: startedAt.Add(10 * time.Second),
		}
		completed, failed := evaluateBackupCompletion(startedAt, status)
		assert.True(t, completed)
		assert.True(t, failed)
	})

	t.Run("successful save", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:  true,
			lastSaveOK:   true,
			lastSaveTime: startedAt.Add(10 * time.Second),
		}
		completed, failed := evaluateBackupCompletion(startedAt, status)
		assert.True(t, completed)
		assert.False(t, failed)
	})
}

func TestRestoredFromBackupSet(t *testing.T) {
	restoreTime := time.Unix(1700000000, 0).UTC()
	restoreSetID := restoreTime.Format(BackupSetIDFormat)

	t.Run("restored from same set", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:   true,
			lastSaveTime:  restoreTime.Add(10 * time.Second),
			loadingStatus: "0",
			loadState:     "done",
		}
		ok, err := restoredFromBackupSet(restoreSetID, status)
		assert.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("still loading", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:   true,
			lastSaveTime:  restoreTime.Add(10 * time.Second),
			loadingStatus: "1",
			loadState:     "loading",
		}
		ok, err := restoredFromBackupSet(restoreSetID, status)
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("older snapshot", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:   true,
			lastSaveTime:  restoreTime.Add(-10 * time.Second),
			loadingStatus: "0",
			loadState:     "done",
		}
		ok, err := restoredFromBackupSet(restoreSetID, status)
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("invalid restore set", func(t *testing.T) {
		status := &persistenceStatus{
			hasLastSave:   true,
			lastSaveTime:  restoreTime.Add(10 * time.Second),
			loadingStatus: "0",
			loadState:     "done",
		}
		_, err := restoredFromBackupSet("not-a-time", status)
		assert.Error(t, err)
	})
}
