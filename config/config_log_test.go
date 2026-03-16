package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// override home dir for testing by using a temp directory
func setupTestDir(t *testing.T) (string, func()) {
	t.Helper()
	tmpDir := t.TempDir()

	// Override HOME so GetLogDir uses the temp directory
	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)

	cleanup := func() {
		os.Setenv("HOME", origHome)
	}
	return tmpDir, cleanup
}

func TestGetLogFilePath_ReturnsDateFormat(t *testing.T) {
	_, cleanup := setupTestDir(t)
	defer cleanup()

	fixedTime := time.Date(2026, 2, 13, 10, 30, 0, 0, time.UTC)
	logPath, err := GetLogFilePathForTime(fixedTime)
	if err != nil {
		t.Fatalf("GetLogFilePathForTime returned error: %v", err)
	}

	base := filepath.Base(logPath)
	expected := "lazykafka-13-02-2026.log"
	if base != expected {
		t.Errorf("expected filename %q, got %q", expected, base)
	}
}

func TestSetupLogging_CreatesFileAndOldDir(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	fixedTime := time.Date(2026, 2, 13, 10, 30, 0, 0, time.UTC)
	f, err := SetupLoggingForTime(fixedTime)
	if err != nil {
		t.Fatalf("SetupLoggingForTime returned error: %v", err)
	}
	f.Close()

	logDir := filepath.Join(tmpDir, ".lazykafka")

	// Check today's log file exists
	todayFile := filepath.Join(logDir, "lazykafka-13-02-2026.log")
	if _, err := os.Stat(todayFile); os.IsNotExist(err) {
		t.Errorf("expected today's log file to exist at %s", todayFile)
	}

	// Check old/ directory exists
	oldDir := filepath.Join(logDir, "old")
	info, err := os.Stat(oldDir)
	if os.IsNotExist(err) {
		t.Errorf("expected old/ directory to exist at %s", oldDir)
	} else if !info.IsDir() {
		t.Errorf("expected %s to be a directory", oldDir)
	}
}

func TestSetupLogging_MovesOldLogs(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	logDir := filepath.Join(tmpDir, ".lazykafka")
	os.MkdirAll(logDir, 0755)

	// Create a stale log file from a previous day
	staleFile := filepath.Join(logDir, "lazykafka-01-01-2025.log")
	os.WriteFile(staleFile, []byte("old log data"), 0644)

	// Run setup for today
	fixedTime := time.Date(2026, 2, 13, 10, 30, 0, 0, time.UTC)
	f, err := SetupLoggingForTime(fixedTime)
	if err != nil {
		t.Fatalf("SetupLoggingForTime returned error: %v", err)
	}
	f.Close()

	// Stale file should have been moved to old/
	if _, err := os.Stat(staleFile); !os.IsNotExist(err) {
		t.Errorf("expected stale log file to be removed from log dir, but it still exists")
	}

	movedFile := filepath.Join(logDir, "old", "lazykafka-01-01-2025.log")
	if _, err := os.Stat(movedFile); os.IsNotExist(err) {
		t.Errorf("expected stale log file to be moved to old/, but it was not found at %s", movedFile)
	}
}

func TestSetupLogging_KeepsTodayLog(t *testing.T) {
	tmpDir, cleanup := setupTestDir(t)
	defer cleanup()

	logDir := filepath.Join(tmpDir, ".lazykafka")
	os.MkdirAll(logDir, 0755)

	// Create today's log file with some existing content
	todayFile := filepath.Join(logDir, "lazykafka-13-02-2026.log")
	os.WriteFile(todayFile, []byte("existing log data\n"), 0644)

	fixedTime := time.Date(2026, 2, 13, 10, 30, 0, 0, time.UTC)
	f, err := SetupLoggingForTime(fixedTime)
	if err != nil {
		t.Fatalf("SetupLoggingForTime returned error: %v", err)
	}
	f.Close()

	// Today's file should still be in the log dir (NOT moved to old/)
	if _, err := os.Stat(todayFile); os.IsNotExist(err) {
		t.Errorf("expected today's log file to remain in log dir, but it was removed")
	}

	movedFile := filepath.Join(logDir, "old", "lazykafka-13-02-2026.log")
	if _, err := os.Stat(movedFile); !os.IsNotExist(err) {
		t.Errorf("today's log file should NOT be moved to old/, but found at %s", movedFile)
	}

	// Verify existing content is preserved (append mode)
	content, _ := os.ReadFile(todayFile)
	if string(content) != "existing log data\n" {
		t.Errorf("expected existing content to be preserved, got %q", string(content))
	}
}
