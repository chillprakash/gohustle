package db

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"gohustle/config"
)

// TimescaleDB represents the database wrapper
type TimescaleDB struct {
	pool   *pgxpool.Pool
	config config.TimescaleConfig
	mu     sync.RWMutex
}

// NewTimescaleDB creates a new TimescaleDB instance
func NewTimescaleDB(config config.TimescaleConfig) (*TimescaleDB, error) {
	db := &TimescaleDB{
		config: config,
	}

	if err := db.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return db, nil
}

func (db *TimescaleDB) initialize() error {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		url.QueryEscape(db.config.User),
		url.QueryEscape(db.config.Password),
		db.config.Host,
		db.config.Port,
		db.config.Database,
	)

	poolConfig, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("unable to parse pool config: %w", err)
	}

	poolConfig.MaxConns = int32(db.config.MaxConnections)
	poolConfig.MinConns = int32(db.config.MinConnections)
	poolConfig.MaxConnLifetime = db.config.GetMaxConnLifetime()
	poolConfig.MaxConnIdleTime = db.config.GetMaxConnIdleTime()

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("unable to ping database: %w", err)
	}

	db.pool = pool
	return nil
}

// GetPool returns the connection pool
func (db *TimescaleDB) GetPool() *pgxpool.Pool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.pool
}

// Close closes the database connection pool
func (db *TimescaleDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// Query executes a query and returns rows
func (db *TimescaleDB) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return db.pool.Query(ctx, sql, args...)
}

// QueryRow executes a query and returns a single row
func (db *TimescaleDB) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return db.pool.QueryRow(ctx, sql, args...)
}

// Exec executes a query without returning rows
func (db *TimescaleDB) Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error) {
	return db.pool.Exec(ctx, sql, args...)
}

// Begin starts a transaction
func (db *TimescaleDB) Begin(ctx context.Context) (pgx.Tx, error) {
	return db.pool.Begin(ctx)
}

// Add health check method
func (db *TimescaleDB) HealthCheck(ctx context.Context) error {
	return db.pool.Ping(ctx)
}

// Add pool stats method
func (db *TimescaleDB) Stats() *pgxpool.Stat {
	return db.pool.Stat()
}
