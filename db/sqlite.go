package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DatabasePath = "./data/credentials.db"
)

// Credentials represents the structure for storing credentials
type Credentials struct {
	Key       string
	Value     string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SQLiteHelper provides a wrapper around SQLite operations
type SQLiteHelper struct {
	db *sql.DB
	mu sync.RWMutex
}

var (
	instance *SQLiteHelper
	once     sync.Once
	mu       sync.RWMutex
)

// GetSQLiteHelper returns a singleton instance of SQLiteHelper
func GetSQLiteHelper() (*SQLiteHelper, error) {
	mu.RLock()
	if instance != nil {
		mu.RUnlock()
		return instance, nil
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()

	if instance != nil {
		return instance, nil
	}

	var initErr error
	once.Do(func() {
		instance, initErr = newSQLiteHelper()
	})

	return instance, initErr
}

// newSQLiteHelper creates a new instance of SQLiteHelper
func newSQLiteHelper() (*SQLiteHelper, error) {
	// Ensure the database directory exists
	dbDir := filepath.Dir(DatabasePath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	db, err := sql.Open("sqlite3", DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create the credentials table if it doesn't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS credentials (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Test database connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &SQLiteHelper{
		db: db,
	}, nil
}

// Close closes the database connection
func (h *SQLiteHelper) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// StoreCredential stores a credential in the database
func (h *SQLiteHelper) StoreCredential(key, value string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	_, err := h.db.Exec(
		`INSERT INTO credentials (key, value, created_at, updated_at) 
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(key) DO UPDATE SET 
		 value=excluded.value, 
		 updated_at=excluded.updated_at`,
		key, value, now, now,
	)
	if err != nil {
		return fmt.Errorf("failed to store credential: %w", err)
	}
	return nil
}

// GetCredential retrieves a credential from the database
func (h *SQLiteHelper) GetCredential(key string) (*Credentials, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var cred Credentials
	err := h.db.QueryRow(
		"SELECT key, value, created_at, updated_at FROM credentials WHERE key = ?",
		key,
	).Scan(&cred.Key, &cred.Value, &cred.CreatedAt, &cred.UpdatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get credential: %w", err)
	}

	return &cred, nil
}

// DeleteCredential removes a credential from the database
func (h *SQLiteHelper) DeleteCredential(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, err := h.db.Exec("DELETE FROM credentials WHERE key = ?", key)
	if err != nil {
		return fmt.Errorf("failed to delete credential: %w", err)
	}
	return nil
}

// ListCredentials returns all stored credential keys
func (h *SQLiteHelper) ListCredentials() ([]string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	rows, err := h.db.Query("SELECT key FROM credentials")
	if err != nil {
		return nil, fmt.Errorf("failed to list credentials: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed to scan key: %w", err)
		}
		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return keys, nil
}
