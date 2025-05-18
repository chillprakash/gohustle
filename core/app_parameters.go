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
		paramStr, err := apm.cache.Get(ctx, redisKey).Result()
		if err == nil {
			var param AppParameter
			if err := json.Unmarshal([]byte(paramStr), &param); err == nil {
				return &param, nil
			} else {
				apm.log.Error("Failed to unmarshal parameter from Redis", map[string]interface{}{
					"key":   key,
					"error": err.Error(),
				})
			}
		}
	}

	// If not in Redis, check database
	var param AppParameter
	query := `SELECT id, key, value, value_type, description, created_at, updated_at 
              FROM app_parameters WHERE key = $1`
	err := apm.db.QueryRow(ctx, query, key).Scan(
		&param.ID, &param.Key, &param.Value, &param.ValueType,
		&param.Description, &param.CreatedAt, &param.UpdatedAt)
	if err != nil {
		return nil, err
	}

	// Update Redis if available
	if apm.cache != nil {
		jsonBytes, err := json.Marshal(param)
		if err == nil {
			redisKey := AppParamKeyPrefix + key
			apm.cache.Set(ctx, redisKey, string(jsonBytes), AppParamTTL)
		} else {
			apm.log.Error("Failed to marshal parameter for Redis", map[string]interface{}{
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
	if exists {
		// Update existing parameter
		query = `UPDATE app_parameters 
                 SET value = $1, value_type = $2, description = $3, updated_at = NOW() 
                 WHERE key = $4 
                 RETURNING id, key, value, value_type, description, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query, value, valueType, description, key).Scan(
			&param.ID, &param.Key, &param.Value, &param.ValueType,
			&param.Description, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to update parameter: %w", err)
		}
	} else {
		// Create new parameter
		query = `INSERT INTO app_parameters (key, value, value_type, description, created_at, updated_at) 
                 VALUES ($1, $2, $3, $4, NOW(), NOW()) 
                 RETURNING id, key, value, value_type, description, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query, key, value, valueType, description).Scan(
			&param.ID, &param.Key, &param.Value, &param.ValueType,
			&param.Description, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parameter: %w", err)
		}
	}

	// Update Redis if available
	if apm.cache != nil {
		jsonBytes, err := json.Marshal(param)
		if err == nil {
			redisKey := AppParamKeyPrefix + key
			apm.cache.Set(ctx, redisKey, string(jsonBytes), AppParamTTL)
		} else {
			apm.log.Error("Failed to marshal parameter for Redis cache", map[string]interface{}{
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
func (apm *AppParameterManager) GetFloat(ctx context.Context, key string, defaultValue float64) (float64, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return defaultValue, nil
		}
		return defaultValue, err
	}

	if param.ValueType != "float" {
		return defaultValue, fmt.Errorf("parameter %s is not a float", key)
	}

	val, err := strconv.ParseFloat(param.Value, 64)
	if err != nil {
		return defaultValue, fmt.Errorf("failed to parse float value: %w", err)
	}

	return val, nil
}

// GetInt retrieves a parameter as int
func (apm *AppParameterManager) GetInt(ctx context.Context, key string, defaultValue int) (int, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return defaultValue, nil
		}
		return defaultValue, err
	}

	if param.ValueType != "int" {
		return defaultValue, fmt.Errorf("parameter %s is not an int", key)
	}

	val, err := strconv.Atoi(param.Value)
	if err != nil {
		return defaultValue, fmt.Errorf("failed to parse int value: %w", err)
	}

	return val, nil
}

// GetBool retrieves a parameter as bool
func (apm *AppParameterManager) GetBool(ctx context.Context, key string, defaultValue bool) (bool, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return defaultValue, nil
		}
		return defaultValue, err
	}

	if param.ValueType != "bool" {
		return defaultValue, fmt.Errorf("parameter %s is not a bool", key)
	}

	val, err := strconv.ParseBool(param.Value)
	if err != nil {
		return defaultValue, fmt.Errorf("failed to parse bool value: %w", err)
	}

	return val, nil
}

// GetString retrieves a parameter as string
func (apm *AppParameterManager) GetString(ctx context.Context, key string, defaultValue string) (string, error) {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		if err == sql.ErrNoRows {
			return defaultValue, nil
		}
		return defaultValue, err
	}

	if param.ValueType != "string" {
		return defaultValue, fmt.Errorf("parameter %s is not a string", key)
	}

	return param.Value, nil
}

// GetJSON retrieves a parameter as JSON and unmarshals it into the provided interface
func (apm *AppParameterManager) GetJSON(ctx context.Context, key string, result interface{}) error {
	param, err := apm.getParameter(ctx, key)
	if err != nil {
		return err
	}

	if param.ValueType != "json" {
		return fmt.Errorf("parameter %s is not JSON", key)
	}

	return json.Unmarshal([]byte(param.Value), result)
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
