package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Broker struct {
	Address string `json:"address"`
	Alias   string `json:"alias,omitempty"`
}

type Config struct {
	Brokers []Broker `json:"brokers"`
}

func LoadConfig() (*Config, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(home, ".lazykafka", "brokers.json")
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{Brokers: []Broker{}}, nil
		}
		return nil, err
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func SaveConfig(cfg *Config) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	dir := filepath.Join(home, ".lazykafka")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	path := filepath.Join(dir, "brokers.json")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(cfg)
}

type Settings struct {
	ShowSplashOnStartup bool `json:"show_splash_on_startup"`
	CheckForUpdates     bool `json:"check_for_updates"`
}

func LoadSettings() (*Settings, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(home, ".lazykafka", "settings.json")
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &Settings{ShowSplashOnStartup: true, CheckForUpdates: false}, nil
		}
		return nil, err
	}
	defer file.Close()

	var settings Settings
	if err := json.NewDecoder(file).Decode(&settings); err != nil {
		return &Settings{ShowSplashOnStartup: true, CheckForUpdates: false}, nil
	}
	return &settings, nil
}

func SaveSettings(settings *Settings) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}

	dir := filepath.Join(home, ".lazykafka")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	path := filepath.Join(dir, "settings.json")
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(settings)
}

// GetLogDir returns the path to the log directory in the user's home directory
func GetLogDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(home, ".lazykafka")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	return dir, nil
}

// GetLogFilePath returns the path to today's log file (lazykafka-DD-MM-YYYY.log)
func GetLogFilePath() (string, error) {
	return GetLogFilePathForTime(time.Now())
}

// GetLogFilePathForTime returns the log file path for a specific time (used for testing)
func GetLogFilePathForTime(t time.Time) (string, error) {
	dir, err := GetLogDir()
	if err != nil {
		return "", err
	}

	filename := fmt.Sprintf("lazykafka-%s.log", t.Format("02-01-2006"))
	return filepath.Join(dir, filename), nil
}

// SetupLogging creates today's log file line, rotates old logs into the "old"
// subdirectory, and returns the opened file handle for today's log.
func SetupLogging() (*os.File, error) {
	return SetupLoggingForTime(time.Now())
}

// SetupLoggingForTime is the testable version of SetupLogging that accepts a time.
func SetupLoggingForTime(now time.Time) (*os.File, error) {
	dir, err := GetLogDir()
	if err != nil {
		return nil, err
	}

	// Ensure old/ subdirectory exists
	oldDir := filepath.Join(dir, "old")
	if err := os.MkdirAll(oldDir, 0755); err != nil {
		return nil, err
	}

	// Today's filename
	todayFilename := fmt.Sprintf("lazykafka-%s.log", now.Format("02-01-2006"))

	// Move stale log files into old/
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		if !strings.HasPrefix(name, "lazykafka-") || !strings.HasSuffix(name, ".log") {
			continue
		}
		if name == todayFilename {
			continue
		}
		src := filepath.Join(dir, name)
		dst := filepath.Join(oldDir, name)
		_ = os.Rename(src, dst) // best-effort move
	}

	// Open today's log file in append mode
	logPath := filepath.Join(dir, todayFilename)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return f, nil
}
