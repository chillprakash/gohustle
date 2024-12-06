package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gohustle/logger"
)

type Config struct {
	Timescale TimescaleConfig `json:"timescale"`
	Redis     RedisConfig     `json:"redis"`
	Kite      KiteConfig      `json:"kite"`
}

type TimescaleConfig struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	User            string `json:"user"`
	Password        string `json:"password"`
	DBName          string `json:"DBName"`
	MaxConnections  int    `json:"max_connections"`
	MinConnections  int    `json:"min_connections"`
	MaxConnLifetime string `json:"max_conn_lifetime"`
	MaxConnIdleTime string `json:"max_conn_idle_time"`

	// Private fields to store parsed durations
	maxConnLifetimeDuration time.Duration
	maxConnIdleTimeDuration time.Duration
}

type RedisConfig struct {
	Host           string `json:"host"`
	Port           int    `json:"port"`
	DB             int    `json:"db"`
	MaxConnections int    `json:"max_connections"`
}

type KiteConfig struct {
	APIKey       string `json:"api_key"`
	APISecret    string `json:"api_secret"`
	UserID       string `json:"user_id"`
	UserPassword string `json:"user_password"`
	TOTPKey      string `json:"totp_key"`
	LoginURL     string `json:"login_url"`
	TwoFAURL     string `json:"twofa_url"`
	DataPath     string `json:"data_path"`
	TickWorkers  int    `json:"tick_workers"`
}

// GetConfig loads configuration and handles errors internally
func GetConfig() *Config {
	log := logger.GetLogger()

	workDir, err := os.Getwd()
	if err != nil {
		log.Error("Failed to get working directory", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	configPath := filepath.Join(workDir, "config", "config.json")

	fileInfo, err := os.Stat(configPath)
	if err != nil {
		log.Error("Config file not found", map[string]interface{}{
			"error": err.Error(),
			"path":  configPath,
		})
		os.Exit(1)
	}

	log.Info("Loading configuration", map[string]interface{}{
		"working_dir":    workDir,
		"config_path":    configPath,
		"file_exists":    true,
		"file_size":      fileInfo.Size(),
		"file_readable":  fileInfo.Mode().Perm()&0400 != 0,
		"file_writeable": fileInfo.Mode().Perm()&0200 != 0,
		"absolute_path":  configPath,
	})

	configFile, err := os.ReadFile(configPath)
	if err != nil {
		log.Error("Failed to read config file", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	var config Config
	if err := json.Unmarshal(configFile, &config); err != nil {
		log.Error("Failed to parse config file", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Simple validation
	if config.Kite.APIKey == "" {
		log.Error("Invalid configuration", map[string]interface{}{
			"error": "kite API key is required",
		})
		os.Exit(1)
	}

	log.Info("Successfully loaded config", map[string]interface{}{
		"path": configPath,
	})

	return &config
}

// Add this method to convert the string values to time.Duration after unmarshaling
func (t *TimescaleConfig) ToDuration() error {
	var err error
	t.maxConnLifetimeDuration, err = time.ParseDuration(t.MaxConnLifetime)
	if err != nil {
		return fmt.Errorf("invalid max_conn_lifetime duration: %w", err)
	}

	t.maxConnIdleTimeDuration, err = time.ParseDuration(t.MaxConnIdleTime)
	if err != nil {
		return fmt.Errorf("invalid max_conn_idle_time duration: %w", err)
	}

	return nil
}

// Add getter methods to access the parsed durations
func (t *TimescaleConfig) GetMaxConnLifetime() time.Duration {
	return t.maxConnLifetimeDuration
}

func (t *TimescaleConfig) GetMaxConnIdleTime() time.Duration {
	return t.maxConnIdleTimeDuration
}
