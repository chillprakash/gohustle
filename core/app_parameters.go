package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key constants
const (
	AppParamKeyPrefix = "app:param:"
	AppParamTTL       = 24 * time.Hour
)

// AppParameter represents a configurable application parameter
type AppParameter struct {
	ID          int       `db:"id" json:"id"`
	Key         string    `db:"key" json:"key"`
	Value       string    `db:"value" json:"value"`
	ValueType   string    `db:"value_type" json:"value_type"`
	Description string    `db:"description" json:"description"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

// AppParameterManager handles operations on app parameters
type AppParameterManager struct {
	db    *db.TimescaleDB
	log   *logger.Logger
	cache *redis.Client
}

var (
	appParamInstance *AppParameterManager
	appParamOnce     sync.Once
)

// GetAppParameterManager returns a singleton instance
func GetAppParameterManager() *AppParameterManager {
	appParamOnce.Do(func() {
		redisCache, err := cache.GetRedisCache()
		if err != nil {
			logger.L().Error("Failed to get Redis cache", map[string]interface{}{
				"error": err.Error(),
			})
			// Continue without Redis
			appParamInstance = &AppParameterManager{
				db:  db.GetTimescaleDB(),
				log: logger.L(),
			}
			return
		}

		appParamInstance = &AppParameterManager{
			db:    db.GetTimescaleDB(),
			log:   logger.L(),
			cache: redisCache.GetCacheDB1(),
		}
	})

	return appParamInstance
}

// getParameter retrieves a parameter by key
func (apm *AppParameterManager) getParameter(ctx context.Context, key string) (*AppParameter, error) {
	// First check Redis if available
	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
		// Only get the raw value from Redis
		value, err := apm.cache.Get(ctx, redisKey).Result()
		if err == nil {
			// Return a minimal parameter with just the value
			// Type conversion will be handled by the caller
			return &AppParameter{
				Key:   key,
				Value: value,
			}, nil
		}
	}

	// Not found in cache or error, get from database
	var param AppParameter
	query := `SELECT id, key, value, value_type, description, created_at, updated_at 
             FROM app_parameters WHERE key = $1`
	err := apm.db.QueryRow(ctx, query, key).Scan(
		&param.ID, &param.Key, &param.Value, &param.ValueType,
		&param.Description, &param.CreatedAt, &param.UpdatedAt)
	if err != nil {
		return nil, err
	}

	// Update Redis if available - only store the value
	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
		// Only store the value string in Redis
		if err := apm.cache.Set(ctx, redisKey, param.Value, AppParamTTL).Err(); err != nil {
			apm.log.Error("Failed to cache parameter in Redis", map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			})
		}
	}

	return &param, nil
}

// GetAllParameters retrieves all parameters
func (apm *AppParameterManager) GetAllParameters(ctx context.Context) ([]AppParameter, error) {
	var params []AppParameter
	query := `SELECT id, key, value, value_type, description, created_at, updated_at 
              FROM app_parameters ORDER BY key`

	rows, err := apm.db.GetPool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all parameters: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var param AppParameter
		err := rows.Scan(
			&param.ID, &param.Key, &param.Value, &param.ValueType,
			&param.Description, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan parameter: %w", err)
		}
		params = append(params, param)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating parameters: %w", err)
	}

	return params, nil
}

// SetParameter creates or updates a parameter
func (apm *AppParameterManager) SetParameter(ctx context.Context, key, value, valueType, description string) (*AppParameter, error) {
	// Validate value type
	if !isValidValueType(valueType) {
		return nil, fmt.Errorf("invalid value type: %s", valueType)
	}

	// Validate value based on type
	if err := validateValue(value, valueType); err != nil {
		return nil, err
	}

	// Check if parameter exists
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM app_parameters WHERE key = $1)`
	err := apm.db.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check if parameter exists: %w", err)
	}

	var param AppParameter
	now := time.Now().UTC()

	if exists {
		// Update existing parameter
		query = `UPDATE app_parameters 
                SET value = $1, value_type = $2, description = $3, updated_at = $4
                WHERE key = $5
                RETURNING id, key, value, value_type, description, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query,
			value, valueType, description, now, key).Scan(
			&param.ID, &param.Key, &param.Value, &param.ValueType,
			&param.Description, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to update parameter: %w", err)
		}
	} else {
		// Create new parameter
		query = `INSERT INTO app_parameters (key, value, value_type, description, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, key, value, value_type, description, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query,
			key, value, valueType, description, now, now).Scan(
			&param.ID, &param.Key, &param.Value, &param.ValueType,
			&param.Description, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parameter: %w", err)
		}
	}

	// Update Redis if available - only store the value
	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
		// Only store the value string in Redis
		if err := apm.cache.Set(ctx, redisKey, param.Value, AppParamTTL).Err(); err != nil {
			apm.log.Error("Failed to cache parameter in Redis", map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			})
		}
	}

	return &param, nil
}

// DeleteParameter deletes a parameter by key
func (apm *AppParameterManager) DeleteParameter(ctx context.Context, key string) error {
	// Delete from database
	query := `DELETE FROM app_parameters WHERE key = $1`
	_, err := apm.db.Exec(ctx, query, key)
	if err != nil {
		return fmt.Errorf("failed to delete parameter: %w", err)
	}

	// Delete from Redis if available
	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
		_, err := apm.cache.Del(ctx, redisKey).Result()
		if err != nil {
			apm.log.Error("Failed to delete parameter from Redis", map[string]interface{}{
				"key":   key,
				"error": err.Error(),
			})
		}
	}

	return nil
}

// ParameterExists checks if a parameter exists
func (apm *AppParameterManager) ParameterExists(ctx context.Context, key string) (bool, error) {
	// First check Redis if available
	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
		exists, err := apm.cache.Exists(ctx, redisKey).Result()
		if err == nil && exists > 0 {
			return true, nil
		}
	}

	// Check database
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM app_parameters WHERE key = $1)`
	err := apm.db.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if parameter exists: %w", err)
	}

	return exists, nil
}

// GetFloat retrieves a parameter as float64
// Returns (value, found, error) where found indicates if the parameter exists
func (apm *AppParameterManager) GetFloat(ctx context.Context, key string) (float64, bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}

	val, err := strconv.ParseFloat(param.Value, 64)
	if err != nil {
		return 0, true, fmt.Errorf("failed to parse float value: %w", err)
	}

	return val, true, nil
}

// GetInt retrieves a parameter as int
// Returns (value, found, error) where found indicates if the parameter exists
func (apm *AppParameterManager) GetInt(ctx context.Context, key string) (int, bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}

	if param.ValueType != "int" {
		return 0, true, fmt.Errorf("parameter %s is not an int", key)
	}

	val, err := strconv.Atoi(param.Value)
	if err != nil {
		return 0, true, fmt.Errorf("failed to parse int value: %w", err)
	}

	return val, true, nil
}

// GetBool retrieves a parameter as bool
// Returns (value, found, error) where found indicates if the parameter exists
func (apm *AppParameterManager) GetBool(ctx context.Context, key string) (bool, bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, false, nil
		}
		return false, false, err
	}

	if param.ValueType != "bool" {
		return false, true, fmt.Errorf("parameter %s is not a bool", key)
	}

	val, err := strconv.ParseBool(param.Value)
	if err != nil {
		return false, true, fmt.Errorf("failed to parse bool value: %w", err)
	}

	return val, true, nil
}

// GetString retrieves a parameter as string
// Returns (value, found, error) where found indicates if the parameter exists
func (apm *AppParameterManager) GetString(ctx context.Context, key string) (string, bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", false, nil
		}
		return "", false, err
	}

	if param.ValueType != "string" {
		return "", true, fmt.Errorf("parameter %s is not a string", key)
	}

	return param.Value, true, nil
}

// GetJSON retrieves a parameter as JSON and unmarshals it into the provided interface
// Returns (found, error) where found indicates if the parameter exists
func (apm *AppParameterManager) GetJSON(ctx context.Context, key string, result interface{}) (bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}

	if param.ValueType != "json" {
		return true, fmt.Errorf("parameter %s is not JSON", key)
	}

	return true, json.Unmarshal([]byte(param.Value), result)
}

// Helper functions

// isValidValueType checks if the provided value type is valid
func isValidValueType(valueType string) bool {
	validTypes := map[string]bool{
		"float":  true,
		"int":    true,
		"bool":   true,
		"string": true,
		"json":   true,
	}
	return validTypes[valueType]
}

// validateValue validates a value based on its type
func validateValue(value, valueType string) error {
	switch valueType {
	case "float":
		_, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid float value: %s", value)
		}
	case "int":
		_, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid int value: %s", value)
		}
	case "bool":
		_, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool value: %s", value)
		}
	case "json":
		var js json.RawMessage
		if err := json.Unmarshal([]byte(value), &js); err != nil {
			return fmt.Errorf("invalid JSON value: %s", value)
		}
	}
	return nil
}
