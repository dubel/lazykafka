package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSaveConfig_RoundTripAndPermissions(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	want := &Config{Brokers: []Broker{{Address: "localhost:19092", Alias: "local"}}}
	if err := SaveConfig(want); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	got, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(got.Brokers) != 1 || got.Brokers[0] != want.Brokers[0] {
		t.Fatalf("round trip mismatch: got %+v", got.Brokers)
	}

	path := filepath.Join(home, ".lazykafka", "brokers.json")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("brokers.json mode = %o, want 600", perm)
	}

	// No temp files left behind.
	entries, _ := os.ReadDir(filepath.Dir(path))
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("leftover temp file %s", e.Name())
		}
	}
}

func TestLoadConfig_CorruptFileIsBackedUpNotLost(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	dir := filepath.Join(home, ".lazykafka")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "brokers.json")
	corrupt := []byte(`{"brokers":[{"address":"prod:9092"`)
	if err := os.WriteFile(path, corrupt, 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig: expected error for corrupt file")
	}

	matches, _ := filepath.Glob(path + ".corrupt-*")
	if len(matches) != 1 {
		t.Fatalf("expected one backup, found %v", matches)
	}
	data, _ := os.ReadFile(matches[0])
	if string(data) != string(corrupt) {
		t.Errorf("backup content changed: %q", data)
	}

	// A save after the failed load must not destroy the backup.
	if err := SaveConfig(&Config{Brokers: []Broker{}}); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}
	if _, err := os.Stat(matches[0]); err != nil {
		t.Errorf("backup disappeared after save: %v", err)
	}
}
