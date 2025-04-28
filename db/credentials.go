package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Credentials represents the structure for storing credentials
type Credentials struct {
	ID        int64     `db:"id"`
	Key       string    `db:"key"`
	Value     string    `db:"value"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// StoreCredential stores a credential in the database
func (t *TimescaleDB) StoreCredential(key, value string) error {
	ctx := context.Background()
	query := `
		INSERT INTO credentials (key, value, created_at, updated_at)
		VALUES (@key, @value, @created_at, @updated_at)
		ON CONFLICT (key) DO UPDATE SET
			value = @value,
			updated_at = @updated_at
	`

	now := time.Now()
	args := pgx.NamedArgs{
		"key":        key,
		"value":      value,
		"created_at": pgtype.Timestamp{Time: now, Valid: true},
		"updated_at": pgtype.Timestamp{Time: now, Valid: true},
	}

	_, err := t.pool.Exec(ctx, query, args)
	if err != nil {
		t.log.Error("Failed to store credential", map[string]interface{}{
			"error": err.Error(),
			"key":   key,
		})
		return fmt.Errorf("failed to store credential: %w", err)
	}

	return nil
}

// GetCredential retrieves a credential from the database
func (t *TimescaleDB) GetCredential(key string) (string, error) {
	ctx := context.Background()
	query := `SELECT value FROM credentials WHERE key = $1`

	var value string
	err := t.pool.QueryRow(ctx, query, key).Scan(&value)
	if err != nil {
		t.log.Error("Failed to get credential", map[string]interface{}{
			"error": err.Error(),
			"key":   key,
		})
		return "", fmt.Errorf("failed to get credential: %w", err)
	}

	return value, nil
}

// DeleteCredential deletes a credential from the database
func (t *TimescaleDB) DeleteCredential(key string) error {
	ctx := context.Background()
	query := `DELETE FROM credentials WHERE key = $1`

	_, err := t.pool.Exec(ctx, query, key)
	if err != nil {
		t.log.Error("Failed to delete credential", map[string]interface{}{
			"error": err.Error(),
			"key":   key,
		})
		return fmt.Errorf("failed to delete credential: %w", err)
	}

	return nil
}

// ListCredentials retrieves all credentials from the database
func (t *TimescaleDB) ListCredentials() ([]Credentials, error) {
	ctx := context.Background()
	query := `SELECT id, key, value, created_at, updated_at FROM credentials ORDER BY key`

	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		t.log.Error("Failed to list credentials", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to list credentials: %w", err)
	}
	defer rows.Close()

	var credentials []Credentials
	for rows.Next() {
		var cred Credentials
		err := rows.Scan(&cred.ID, &cred.Key, &cred.Value, &cred.CreatedAt, &cred.UpdatedAt)
		if err != nil {
			t.log.Error("Failed to scan credential row", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, fmt.Errorf("failed to scan credential row: %w", err)
		}
		credentials = append(credentials, cred)
	}

	if err := rows.Err(); err != nil {
		t.log.Error("Error iterating credential rows", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("error iterating credential rows: %w", err)
	}

	return credentials, nil
}
