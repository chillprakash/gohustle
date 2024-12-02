package db

import (
	"context"
	"fmt"
	"os"
	"time"

	"gohustle/config"
	"gohustle/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TimescaleDB struct {
	pool   *pgxpool.Pool
	config *config.TimescaleConfig
}

// InitDB initializes the database connection
func InitDB(cfg *config.TimescaleConfig) *TimescaleDB {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create connection string
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName,
	)

	// Configure connection pool
	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		log.Error("Failed to parse database config", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Set pool configuration from TimescaleConfig
	poolConfig.MaxConns = int32(cfg.MaxConnections)
	poolConfig.MinConns = int32(cfg.MinConnections)

	// Parse MaxConnLifetime
	maxLifetime, err := time.ParseDuration(cfg.MaxConnLifetime)
	if err != nil {
		log.Error("Invalid MaxConnLifetime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnLifetime,
		})
		os.Exit(1)
	}
	poolConfig.MaxConnLifetime = maxLifetime

	// Parse MaxConnIdleTime
	maxIdleTime, err := time.ParseDuration(cfg.MaxConnIdleTime)
	if err != nil {
		log.Error("Invalid MaxConnIdleTime", map[string]interface{}{
			"error": err.Error(),
			"value": cfg.MaxConnIdleTime,
		})
		os.Exit(1)
	}
	poolConfig.MaxConnIdleTime = maxIdleTime

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Error("Failed to create database pool", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		log.Error("Failed to ping database", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	log.Info("Successfully connected to database", map[string]interface{}{
		"host":         cfg.Host,
		"port":         cfg.Port,
		"db":           cfg.DBName,
		"max_conns":    cfg.MaxConnections,
		"min_conns":    cfg.MinConnections,
		"max_lifetime": cfg.MaxConnLifetime,
		"max_idletime": cfg.MaxConnIdleTime,
	})

	return &TimescaleDB{
		pool:   pool,
		config: cfg,
	}
}

// GetConfig returns the database configuration
func (db *TimescaleDB) GetConfig() *config.TimescaleConfig {
	return db.config
}

// Close closes the database connection
func (db *TimescaleDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// QueryRow executes a query that returns a single row
func (db *TimescaleDB) QueryRow(ctx context.Context, query string, args ...interface{}) Row {
	return db.pool.QueryRow(ctx, query, args...)
}

// Exec executes a query that doesn't return rows
func (db *TimescaleDB) Exec(ctx context.Context, query string, args ...interface{}) (CommandTag, error) {
	return db.pool.Exec(ctx, query, args...)
}

// Row interface for query results
type Row interface {
	Scan(dest ...interface{}) error
}

// CommandTag interface for exec results
type CommandTag interface {
	RowsAffected() int64
}
