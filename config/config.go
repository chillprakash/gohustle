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
	Timescale  TimescaleConfig  `json:"timescale"`
	Redis      RedisConfig      `json:"redis"`
	Kite       KiteConfig       `json:"kite"`
	Asynq      AsynqConfig      `json:"asynq"`
	Telegram   TelegramConfig   `json:"telegram"`
	Indices    IndicesConfig    `json:"indices"`
	ClickHouse ClickHouseConfig `json:"clickhouse"`
}

type TimescaleConfig struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	PgBouncerPort   int    `json:"pgb_port"`
	User            string `json:"user"`
	Password        string `json:"password"`
	DBName          string `json:"DBName"`
	MaxConnections  int    `json:"max_connections"`
	MinConnections  int    `json:"min_connections"`
	MaxConnLifetime string `json:"max_conn_lifetime"`
	MaxConnIdleTime string `json:"max_conn_idle_time"`
	PoolMode        string `json:"pool_mode"`

	// Private fields to store parsed durations
	maxConnLifetimeDuration time.Duration
	maxConnIdleTimeDuration time.Duration
}

type RedisConfig struct {
	Host            string           `json:"host"`
	Port            string           `json:"port"`
	Password        string           `json:"password"`
	MaxConnections  int              `json:"max_connections"`
	MinConnections  int              `json:"min_connections"`
	ConnectTimeout  string           `json:"connect_timeout"`
	MaxConnLifetime string           `json:"max_conn_lifetime"`
	MaxConnIdleTime string           `json:"max_conn_idle_time"`
	Persistence     RedisPersistence `json:"persistence"`

	// Private fields to store parsed durations
	connectTimeoutDuration  time.Duration
	maxConnLifetimeDuration time.Duration
	maxConnIdleTimeDuration time.Duration
}

type RedisPersistence struct {
	AOFEnabled    bool           `json:"aof_enabled"`
	AOFSync       string         `json:"aof_fsync"` // always, everysec, no
	RDBEnabled    bool           `json:"rdb_enabled"`
	SaveIntervals []SaveInterval `json:"save_intervals"`
}

type SaveInterval struct {
	Seconds int `json:"seconds"`
	Changes int `json:"changes"`
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

type AsynqConfig struct {
	Host           string                 `json:"host"`
	Port           string                 `json:"port"`
	Password       string                 `json:"password"`
	DB             int                    `json:"db"`
	MaxConnections int                    `json:"max_connections"`
	Concurrency    int                    `json:"concurrency"`
	Queues         map[string]QueueConfig `json:"queues"`
}

type QueueConfig struct {
	Priority    int  `json:"priority"`
	Concurrency int  `json:"concurrency"`
	Enabled     bool `json:"enabled"`
}

type TelegramConfig struct {
	BotToken string `json:"bot_token"`
	ChatID   string `json:"chat_id"`
}

type IndicesConfig struct {
	DerivedIndices []string `json:"derived_indices"`
	SpotIndices    []string `json:"spot_indices"`
}

// Add ClickHouse configuration
type ClickHouseConfig struct {
	Host            string `json:"host"`
	Port            int    `json:"port"`
	User            string `json:"user"`
	Password        string `json:"password"`
	DBName          string `json:"DBName"`
	MaxOpenConns    int    `json:"max_open_conns"`
	MaxIdleConns    int    `json:"max_idle_conns"`
	ConnMaxLifetime string `json:"conn_max_lifetime"`
	DialTimeout     string `json:"dial_timeout"`
	BlockSize       int    `json:"block_size"`
	PoolSize        int    `json:"pool_size"`
	Debug           bool   `json:"debug"`
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

// Add method to parse duration strings
func (r *RedisConfig) ToDuration() error {
	var err error
	r.connectTimeoutDuration, err = time.ParseDuration(r.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("invalid connect_timeout duration: %w", err)
	}

	r.maxConnLifetimeDuration, err = time.ParseDuration(r.MaxConnLifetime)
	if err != nil {
		return fmt.Errorf("invalid max_conn_lifetime duration: %w", err)
	}

	r.maxConnIdleTimeDuration, err = time.ParseDuration(r.MaxConnIdleTime)
	if err != nil {
		return fmt.Errorf("invalid max_conn_idle_time duration: %w", err)
	}

	return nil
}

// Add getter methods
func (r *RedisConfig) GetConnectTimeout() time.Duration {
	return r.connectTimeoutDuration
}

func (r *RedisConfig) GetMaxConnLifetime() time.Duration {
	return r.maxConnLifetimeDuration
}

func (r *RedisConfig) GetMaxConnIdleTime() time.Duration {
	return r.maxConnIdleTimeDuration
}
