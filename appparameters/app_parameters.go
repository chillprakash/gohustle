package appparameters

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/cache"
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
type ProductType string
type OrderType string

// Define constants for ProductType
const (
	ProductMIS  ProductType = "MIS"
	ProductNRML ProductType = "NRML"
)

// Define constants for OrderType
const (
	OrderTypeMARKET OrderType = "MARKET"
	OrderTypeLIMIT  OrderType = "LIMIT"
)

const (
	AppParamOrderProductType AppParameterTypes = "order_product_type" //CNC, MIS, NRML
	AppParamOrderType        AppParameterTypes = "order_type"         //LIMIT, MARKET
	AppParamExitPNL          AppParameterTypes = "exit_pnl"
	AppParamTargetPNL        AppParameterTypes = "target_pnl"
)

// AppParameter represents a configurable application parameter
type AppParameter struct {
	ID        int       `json:"id"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type OrderAppParameters struct {
	ProductType ProductType `json:"order_product_type"` //CNC, MIS, NRML
	OrderType   OrderType   `json:"order_type"`         //LIMIT, MARKET
}

// AppParameterManager handles operations on app parameters
type AppParameterManager struct {
	log   *logger.Logger
	cache *redis.Client
}

var (
	appParamInstance *AppParameterManager
	appParamOnce     sync.Once
)

func GetAppParameterManager() *AppParameterManager {
	appParamOnce.Do(func() {
		// Initialize logger first
		log := logger.L()
		// Initialize Redis cache with error handling
		redisCache, err := cache.GetRedisCache()
		if err != nil {
			log.Error("Failed to initialize Redis cache", map[string]interface{}{
				"error": err.Error(),
			})
			// Don't set instance if Redis fails
			return
		}

		// Initialize the instance
		appParamInstance = &AppParameterManager{
			log:   log,
			cache: redisCache.GetCacheDB1(),
		}

		log.Info("AppParameterManager initialized successfully")
	})

	return appParamInstance
}

func (apm *AppParameterManager) GetOrderAppParameters() OrderAppParameters {
	params, _ := apm.GetParameters(context.Background(), []AppParameterTypes{
		AppParamOrderProductType,
		AppParamOrderType,
	})
	return OrderAppParameters{
		ProductType: ProductType(params[string(AppParamOrderProductType)].Value),
		OrderType:   OrderType(params[string(AppParamOrderType)].Value),
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

	// Check which keys are in Redis
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

					// Try to unmarshal as JSON first (for stored AppParameter objects)
					var param AppParameter
					valueStr := value.(string)

					if err := json.Unmarshal([]byte(valueStr), &param); err != nil {
						// If not JSON, just use the value directly
						param = AppParameter{
							Key:       originalKey,
							Value:     valueStr,
							CreatedAt: time.Now(),
							UpdatedAt: time.Now(),
						}
					}

					result[originalKey] = &param
				}
			}
		}
	}

	// Check if we got all the keys
	missingKeys := make([]AppParameterTypes, 0)
	for _, key := range keys {
		if _, exists := result[string(key)]; !exists {
			missingKeys = append(missingKeys, key)
		}
	}

	// If there are missing keys, return default values
	for _, key := range missingKeys {
		// Set default values based on key type
		var defaultValue string
		switch key {
		case AppParamOrderProductType:
			defaultValue = string(ProductMIS)
		case AppParamOrderType:
			defaultValue = string(OrderTypeMARKET)
		case AppParamExitPNL:
			defaultValue = "-1000"
		case AppParamTargetPNL:
			defaultValue = "1000"
		default:
			defaultValue = ""
		}

		// Create parameter with default value
		now := time.Now()
		param := &AppParameter{
			Key:       string(key),
			Value:     defaultValue,
			CreatedAt: now,
			UpdatedAt: now,
		}

		// Store in Redis for next time
		if apm.cache != nil {
			paramJSON, _ := json.Marshal(param)
			redisKey := AppParamKeyPrefix + string(key)
			if err := apm.cache.Set(ctx, redisKey, string(paramJSON), AppParamTTL).Err(); err != nil {
				apm.log.Error("Failed to cache parameter in Redis", map[string]interface{}{
					"key":   key,
					"error": err.Error(),
				})
			}
		}

		result[string(key)] = param
	}

	return result, nil
}

// GetAllParameters retrieves all parameters
func (apm *AppParameterManager) GetAllParameters(ctx context.Context) ([]AppParameter, error) {
	var params []AppParameter

	if apm.cache == nil {
		return params, fmt.Errorf("redis client not initialized")
	}

	// Get all keys with the parameter prefix
	keys, err := apm.cache.Keys(ctx, AppParamKeyPrefix+"*").Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get parameter keys: %w", err)
	}

	// Get all values in a pipeline
	if len(keys) > 0 {
		pipe := apm.cache.Pipeline()
		cmds := make([]*redis.StringCmd, len(keys))

		for i, key := range keys {
			cmds[i] = pipe.Get(ctx, key)
		}

		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get parameter values: %w", err)
		}

		// Process results
		for i, cmd := range cmds {
			valueStr, err := cmd.Result()
			if err == nil {
				var param AppParameter
				if err := json.Unmarshal([]byte(valueStr), &param); err != nil {
					// If not JSON, create a parameter with the raw value
					key := strings.TrimPrefix(keys[i], AppParamKeyPrefix)
					param = AppParameter{
						Key:       key,
						Value:     valueStr,
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					}
				}
				params = append(params, param)
			}
		}
	}

	return params, nil
}

// SetParameter creates or updates a parameter
func (apm *AppParameterManager) SetParameter(ctx context.Context, key AppParameterTypes, value string) (*AppParameter, error) {
	if apm.cache == nil {
		return nil, fmt.Errorf("redis client not initialized")
	}

	now := time.Now().UTC()
	redisKey := AppParamKeyPrefix + string(key)

	// Check if parameter exists
	exists, err := apm.cache.Exists(ctx, redisKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to check if parameter exists: %w", err)
	}

	var param AppParameter

	if exists > 0 {
		// Get existing parameter
		valueStr, err := apm.cache.Get(ctx, redisKey).Result()
		if err != nil && err != redis.Nil {
			return nil, fmt.Errorf("failed to get existing parameter: %w", err)
		}

		if err == nil {
			// Try to unmarshal existing parameter
			if err := json.Unmarshal([]byte(valueStr), &param); err == nil {
				// Update value and timestamp
				param.Value = value
				param.UpdatedAt = now
			}
		}
	}

	// If parameter doesn't exist or couldn't be unmarshaled, create a new one
	if param.Key == "" {
		param = AppParameter{
			Key:       string(key),
			Value:     value,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}

	// Serialize and store in Redis
	paramJSON, err := json.Marshal(param)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal parameter: %w", err)
	}

	if err := apm.cache.Set(ctx, redisKey, string(paramJSON), AppParamTTL).Err(); err != nil {
		return nil, fmt.Errorf("failed to store parameter in Redis: %w", err)
	}

	return &param, nil
}

// DeleteParameter deletes a parameter by key
func (apm *AppParameterManager) DeleteParameter(ctx context.Context, key string) error {
	if apm.cache == nil {
		return fmt.Errorf("redis client not initialized")
	}

	redisKey := AppParamKeyPrefix + key
	_, err := apm.cache.Del(ctx, redisKey).Result()
	if err != nil {
		return fmt.Errorf("failed to delete parameter: %w", err)
	}

	return nil
}

// ParameterExists checks if a parameter exists
func (apm *AppParameterManager) ParameterExists(ctx context.Context, key string) (bool, error) {
	if apm.cache == nil {
		return false, fmt.Errorf("redis client not initialized")
	}

	redisKey := AppParamKeyPrefix + key
	exists, err := apm.cache.Exists(ctx, redisKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check if parameter exists: %w", err)
	}

	return exists > 0, nil
}
