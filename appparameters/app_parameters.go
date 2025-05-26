package appparameters

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis key constants
const (
	AppParamKeyPrefix = "app:param:"
	AppParamTTL       = 24 * time.Hour
)

type AppParameterTypes string

const (
	AppParamOrderProductType AppParameterTypes = "order_product_type" //CNC, MIS, NRML
	AppParamOrderType        AppParameterTypes = "order_type"         //LIMIT, MARKET
	AppParamExitPNL          AppParameterTypes = "exit_pnl"
	AppParamTargetPNL        AppParameterTypes = "target_pnl"
)

// AppParameter represents a configurable application parameter
type AppParameter struct {
	ID        int       `db:"id" json:"id"`
	Key       string    `db:"key" json:"key"`
	Value     string    `db:"value" json:"value"`
	CreatedAt time.Time `db:"created_at" json:"created_at"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}

type OrderAppParameters struct {
	ProductType AppParameterTypes `json:"product_type"` //CNC, MIS, NRML
	OrderType   AppParameterTypes `json:"order_type"`   //LIMIT, MARKET
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

func GetAppParameterManager() *AppParameterManager {
	appParamOnce.Do(func() {
		redisCache, _ := cache.GetRedisCache()
		appParamInstance = &AppParameterManager{
			db:    db.GetTimescaleDB(),
			log:   logger.L(),
			cache: redisCache.GetCacheDB1(),
		}
	})

	return appParamInstance
}

func (apm *AppParameterManager) GetOrderAppParameters() OrderAppParameters {
	params, _ := apm.GetParameters(context.Background(), []AppParameterTypes{
		AppParamOrderProductType,
		AppParamOrderType,
	})
	return OrderAppParameters{
		ProductType: AppParameterTypes(params[string(AppParamOrderProductType)].Value),
		OrderType:   AppParameterTypes(params[string(AppParamOrderType)].Value),
	}

}

func (apm *AppParameterManager) GetParameter(ctx context.Context, key AppParameterTypes) (*AppParameter, error) {
	params, err := apm.GetParameters(ctx, []AppParameterTypes{key})
	if err != nil {
		return nil, err
	}
	return params[string(key)], nil
}

// GetParameters retrieves multiple parameters at once by their keys
// Returns a map of parameter key to AppParameter
func (apm *AppParameterManager) GetParameters(ctx context.Context, keys []AppParameterTypes) (map[string]*AppParameter, error) {
	// Initialize result map
	result := make(map[string]*AppParameter, len(keys))

	// Check which keys are in Redis first (if available)
	redisKeys := make([]string, 0, len(keys))
	redisKeyToOriginal := make(map[string]string, len(keys))

	for _, key := range keys {
		redisKey := AppParamKeyPrefix + string(key)
		redisKeys = append(redisKeys, redisKey)
		redisKeyToOriginal[redisKey] = string(key)
	}

	// Try to get values from Redis in a single batch operation
	if apm.cache != nil && len(redisKeys) > 0 {
		values, err := apm.cache.MGet(ctx, redisKeys...).Result()
		if err == nil {
			for i, value := range values {
				if value != nil {
					// Found in Redis
					originalKey := redisKeyToOriginal[redisKeys[i]]
					result[originalKey] = &AppParameter{
						Key:   originalKey,
						Value: value.(string),
					}
				}
			}
		}
	}

	// Get remaining keys from database
	missingKeys := make([]string, 0)
	for _, key := range keys {
		if _, exists := result[string(key)]; !exists {
			missingKeys = append(missingKeys, string(key))
		}
	}

	if len(missingKeys) > 0 {
		// Build the query with multiple placeholders
		placeholders := make([]string, len(missingKeys))
		args := make([]interface{}, len(missingKeys))

		for i, key := range missingKeys {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = key
		}

		query := fmt.Sprintf(
			"SELECT id, key, value, value_type, description, created_at, updated_at "+
				"FROM app_parameters WHERE key IN (%s)",
			strings.Join(placeholders, ","),
		)

		rows, err := apm.db.GetPool().Query(ctx, query, args...)
		if err != nil {
			return result, fmt.Errorf("failed to query parameters: %w", err)
		}
		defer rows.Close()

		// Process rows and update Redis cache
		for rows.Next() {
			var param AppParameter
			err := rows.Scan(
				&param.ID, &param.Key, &param.Value,
				&param.CreatedAt, &param.UpdatedAt,
			)
			if err != nil {
				apm.log.Error("Failed to scan parameter row", map[string]interface{}{
					"error": err.Error(),
				})
				continue
			}

			// Add to result
			result[param.Key] = &param

			// Update Redis if available
			if apm.cache != nil {
				redisKey := AppParamKeyPrefix + param.Key
				if err := apm.cache.Set(ctx, redisKey, param.Value, AppParamTTL).Err(); err != nil {
					apm.log.Error("Failed to cache parameter in Redis", map[string]interface{}{
						"key":   param.Key,
						"error": err.Error(),
					})
				}
			}
		}

		if err := rows.Err(); err != nil {
			apm.log.Error("Error iterating parameter rows", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}

	return result, nil
}

// GetAllParameters retrieves all parameters
func (apm *AppParameterManager) GetAllParameters(ctx context.Context) ([]AppParameter, error) {
	var params []AppParameter
	query := `SELECT id, key, value, created_at, updated_at 
              FROM app_parameters ORDER BY key`

	rows, err := apm.db.GetPool().Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all parameters: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var param AppParameter
		err := rows.Scan(
			&param.ID, &param.Key, &param.Value,
			&param.CreatedAt, &param.UpdatedAt)
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
func (apm *AppParameterManager) SetParameter(ctx context.Context, key, value string) (*AppParameter, error) {
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM app_parameters WHERE key = $1)`
	err := apm.db.QueryRow(ctx, query, key).Scan(&exists)
	if err != nil {
		return nil, fmt.Errorf("failed to check if parameter exists: %w", err)
	}

	var param AppParameter
	now := time.Now().UTC()

	if exists {
		query = `UPDATE app_parameters 
                SET value = $1, updated_at = $2
                WHERE key = $3
                RETURNING id, key, value, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query,
			value, now, key).Scan(
			&param.ID, &param.Key, &param.Value, &param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to update parameter: %w", err)
		}
	} else {
		query = `INSERT INTO app_parameters (key, value, created_at, updated_at)
                VALUES ($1, $2, $3, $4)
                RETURNING id, key, value, created_at, updated_at`
		err = apm.db.QueryRow(ctx, query,
			key, value, now, now).Scan(
			&param.ID, &param.Key, &param.Value,
			&param.CreatedAt, &param.UpdatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to create parameter: %w", err)
		}
	}

	if apm.cache != nil {
		redisKey := AppParamKeyPrefix + key
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
