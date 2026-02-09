package config

import (
	"encoding/json"
	"os"
	"path/filepath"
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

// GetLogFilePath returns the path to the log file in the user's home directory
func GetLogFilePath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(home, ".lazykafka")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	return filepath.Join(dir, "lazykafka.log"), nil
}
