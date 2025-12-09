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
	Brokers       []Broker `json:"brokers"`
	MouseDisabled bool     `json:"mouse_disabled"`
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
