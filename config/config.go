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
	Database        string `json:"database"`
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
}

// GetConfig returns the configuration from the default location
func GetConfig() (*Config, error) {
	log := logger.GetLogger()

	pwd, err := os.Getwd()
	if err != nil {
		log.Error("Failed to get working directory", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get working directory: %w", err)
	}

	configPath := filepath.Join(pwd, "config", "config.json")
	absPath, err := filepath.Abs(configPath)
	if err != nil {
		log.Error("Failed to get absolute path", map[string]interface{}{
			"error": err.Error(),
			"path":  configPath,
		})
	}

	// Log all path information
	log.Info("Loading configuration", map[string]interface{}{
		"working_dir":    pwd,
		"config_path":    configPath,
		"absolute_path":  absPath,
		"file_exists":    fileExists(configPath),
		"file_size":      getFileSize(configPath),
		"file_readable":  isReadable(configPath),
		"file_writeable": isWriteable(configPath),
	})

	return loadConfigFile(configPath)
}

// Helper functions for detailed file information
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

func isReadable(path string) bool {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return false
	}
	file.Close()
	return true
}

func isWriteable(path string) bool {
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return false
	}
	file.Close()
	return true
}

func loadConfigFile(filePath string) (*Config, error) {
	log := logger.GetLogger()

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		log.Error("Failed to get absolute path", map[string]interface{}{
			"error": err.Error(),
			"path":  filePath,
		})
		return nil, fmt.Errorf("error getting absolute path: %w", err)
	}

	// Check if file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Error("Config file not found", map[string]interface{}{
			"path": absPath,
		})
		return nil, fmt.Errorf("config file not found at %s", absPath)
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		log.Error("Failed to read config file", map[string]interface{}{
			"error": err.Error(),
			"path":  absPath,
		})
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	if len(data) == 0 {
		log.Error("Config file is empty", map[string]interface{}{
			"path": absPath,
		})
		return nil, fmt.Errorf("config file is empty")
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Error("Failed to parse config file", map[string]interface{}{
			"error":   err.Error(),
			"path":    absPath,
			"content": string(data),
		})
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	// Convert duration strings to time.Duration
	if err := config.Timescale.ToDuration(); err != nil {
		log.Error("Failed to parse duration values", map[string]interface{}{
			"error": err.Error(),
			"path":  absPath,
		})
		return nil, fmt.Errorf("error parsing durations: %w", err)
	}

	log.Info("Successfully loaded config", map[string]interface{}{
		"path": absPath,
	})

	return &config, nil
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
