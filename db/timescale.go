package db

import (
	"context"
	"fmt"
	"os"

	"gohustle/config"
	"gohustle/logger"

	"github.com/jackc/pgx/v4/pgxpool"
)

type TimescaleDB struct {
	pool *pgxpool.Pool
}

// InitDB initializes and returns a database connection, handles errors internally
func InitDB(cfg *config.TimescaleConfig) *TimescaleDB {
	log := logger.GetLogger()

	db, err := newTimescaleDB(cfg)
	if err != nil {
		log.Error("Failed to initialize TimescaleDB", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	return db
}

// newTimescaleDB is the internal implementation that returns error
func newTimescaleDB(cfg *config.TimescaleConfig) (*TimescaleDB, error) {
	log := logger.GetLogger()

	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.DBName,
	)

	pool, err := pgxpool.Connect(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Info("Successfully connected to TimescaleDB", map[string]interface{}{
		"host": cfg.Host,
		"port": cfg.Port,
		"user": cfg.User,
		"db":   cfg.DBName,
	})

	return &TimescaleDB{pool: pool}, nil
}

// Close closes the database connection
func (db *TimescaleDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}
