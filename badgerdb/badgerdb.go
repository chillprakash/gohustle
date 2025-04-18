package badgerdb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Define a constant for the database path
const (
	DatabasePath = "./data/badgerdb" // Relative path to the data directory
)

// Credentials represents the structure for storing multiple credentials
type Credentials struct {
	Key       string    `json:"key"`        // Unique identifier for the credential
	Value     string    `json:"value"`      // The actual credential value
	CreatedAt time.Time `json:"created_at"` // Timestamp for when the credential was created
	UpdatedAt time.Time `json:"updated_at"` // Timestamp for when the credential was last updated
}

// BadgerDBHelper provides a wrapper around BadgerDB operations
type BadgerDBHelper struct {
	db *badger.DB
	mu sync.RWMutex
}

// NewBadgerDBHelper creates a new instance of BadgerDBHelper
func NewBadgerDBHelper() (*BadgerDBHelper, error) {
	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(DatabasePath), os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	opts := badger.DefaultOptions(DatabasePath) // Use the constant here
	opts.Logger = nil                           // Disable Badger's internal logger

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open BadgerDB: %w", err)
	}

	return &BadgerDBHelper{
		db: db,
	}, nil
}

// Close closes the database connection
func (h *BadgerDBHelper) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.db.Close()
}

// StoreCredential stores a credential in the database
func (h *BadgerDBHelper) StoreCredential(key string, value string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	credentials := Credentials{
		Key:       key, // Set the unique key
		Value:     value,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	data, err := json.Marshal(credentials)
	if err != nil {
		return fmt.Errorf("failed to marshal credentials: %w", err)
	}

	return h.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// GetCredential retrieves a credential from the database
func (h *BadgerDBHelper) GetCredential(key string) (*Credentials, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var credentials Credentials

	err := h.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &credentials)
		})
	})

	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to get credentials: %w", err)
	}

	return &credentials, nil
}

// DeleteCredential removes a credential from the database
func (h *BadgerDBHelper) DeleteCredential(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// ListCredentials returns all stored credential keys
func (h *BadgerDBHelper) ListCredentials() ([]string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var keys []string

	err := h.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(it.Item().Key()))
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list credentials: %w", err)
	}

	return keys, nil
}

// UpdateCredential updates an existing credential
func (h *BadgerDBHelper) UpdateCredential(key string, value string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// First check if credential exists
	exists := false
	err := h.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get([]byte(key))
		if err == nil {
			exists = true
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to check credential existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("credential not found")
	}

	credentials := Credentials{
		Key:       key, // Keep the same key
		Value:     value,
		UpdatedAt: time.Now(),
	}

	data, err := json.Marshal(credentials)
	if err != nil {
		return fmt.Errorf("failed to marshal credentials: %w", err)
	}

	return h.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}
