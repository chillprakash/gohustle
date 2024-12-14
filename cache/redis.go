package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/config"
	"gohustle/logger"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisTokenData represents token information stored in Redis
type RedisTokenData struct {
	AccessToken string `json:"access_token"`
	CreatedAt   int64  `json:"created_at"` // Unix milliseconds
	ExpiresAt   int64  `json:"expires_at"` // Unix milliseconds
	IsValid     bool   `json:"is_valid"`
}

// Constants for database numbers
const (
	RelationalDB = 1 // For relational data
	ListDB       = 2 // For list data
	LTPDB        = 3 // For LTP data
)

// Add these constants for token management
const (
	TokenKeyPrefix = "kite:token:" // Prefix for token keys
	TokenSetKey    = "kite:tokens" // Set to track all tokens
)

var (
	redisInstance *RedisCache
	redisOnce     sync.Once
	redisMu       sync.RWMutex
)

type RedisCache struct {
	pools  map[int]*redis.Client
	log    *logger.Logger
	config *config.RedisConfig
	mu     sync.RWMutex
}

// NewRedisCache creates or returns existing Redis cache instance
func NewRedisCache() (*RedisCache, error) {
	redisMu.Lock()
	defer redisMu.Unlock()

	var initErr error
	redisOnce.Do(func() {
		cfg := config.GetConfig()
		redisInstance = &RedisCache{
			pools:  make(map[int]*redis.Client),
			log:    logger.GetLogger(),
			config: &cfg.Redis,
			mu:     sync.RWMutex{},
		}
		initErr = redisInstance.initialize()
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize Redis: %w", initErr)
	}

	return redisInstance, nil
}

// initialize is internal initialization method
func (rc *RedisCache) initialize() error {
	// Set defaults if not configured
	if rc.config.MaxConnections == 0 {
		rc.config.MaxConnections = 100
	}
	if rc.config.MinConnections == 0 {
		rc.config.MinConnections = 10
	}
	if rc.config.GetConnectTimeout() == 0 {
		rc.config.ConnectTimeout = "5s"
		if err := rc.config.ToDuration(); err != nil {
			rc.log.Error("Failed to parse connect timeout", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}

	// Create default DB client
	client := rc.getRedisClient(0)
	if client == nil {
		return fmt.Errorf("failed to create default Redis client")
	}

	rc.log.Info("Redis cache initialized", map[string]interface{}{
		"host":        rc.config.Host,
		"port":        rc.config.Port,
		"max_conns":   rc.config.MaxConnections,
		"min_conns":   rc.config.MinConnections,
		"aof_enabled": rc.config.Persistence.AOFEnabled,
		"aof_sync":    rc.config.Persistence.AOFSync,
	})

	return nil
}

// getRedisClient gets or creates a Redis client for the specified database
func (rc *RedisCache) getRedisClient(db int) *redis.Client {
	rc.mu.RLock()
	if client, exists := rc.pools[db]; exists {
		rc.mu.RUnlock()
		return client
	}
	rc.mu.RUnlock()

	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists := rc.pools[db]; exists {
		return client
	}

	client := redis.NewClient(&redis.Options{
		Addr:            rc.config.Host + ":" + rc.config.Port,
		Password:        rc.config.Password,
		DB:              db,
		PoolSize:        rc.config.MaxConnections,
		MinIdleConns:    rc.config.MinConnections,
		ConnMaxLifetime: rc.config.GetMaxConnLifetime(),
		ConnMaxIdleTime: rc.config.GetMaxConnIdleTime(),
		DialTimeout:     rc.config.GetConnectTimeout(),
		ReadTimeout:     rc.config.GetConnectTimeout(),
		WriteTimeout:    rc.config.GetConnectTimeout(),
		PoolTimeout:     rc.config.GetConnectTimeout(),
	})

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		rc.log.Error("Failed to connect to Redis", map[string]interface{}{
			"error": err.Error(),
			"db":    db,
			"host":  rc.config.Host,
			"port":  rc.config.Port,
		})
		return nil
	}

	rc.pools[db] = client
	rc.log.Info("Created new Redis connection pool", map[string]interface{}{
		"db":             db,
		"max_conns":      rc.config.MaxConnections,
		"min_idle_conns": rc.config.MinConnections,
	})

	return client
}

func (rc *RedisCache) GetRelationalDB1() *redis.Client {
	return rc.getRedisClient(RelationalDB)
}

func (rc *RedisCache) GetListDB2() *redis.Client {
	return rc.getRedisClient(ListDB)
}

func (rc *RedisCache) GetLTPDB3() *redis.Client {
	return rc.getRedisClient(LTPDB)
}

// Close closes all Redis connections
func (rc *RedisCache) Close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for db, client := range rc.pools {
		if err := client.Close(); err != nil {
			rc.log.Error("Failed to close Redis connection", map[string]interface{}{
				"error": err.Error(),
				"db":    db,
			})
		}
	}
}

// StoreAccessToken stores a new access token with 6 AM next day expiry
func (rc *RedisCache) StoreAccessToken(ctx context.Context, accessToken string) error {
	log := logger.GetLogger()

	// Calculate expiry (6 AM next day) in milliseconds
	tomorrow := time.Now().Add(24 * time.Hour)
	expiryTime := time.Date(
		tomorrow.Year(), tomorrow.Month(), tomorrow.Day(),
		6, 0, 0, 0, tomorrow.Location(),
	)

	tokenData := RedisTokenData{
		AccessToken: accessToken,
		CreatedAt:   time.Now().UnixMilli(),
		ExpiresAt:   expiryTime.UnixMilli(),
		IsValid:     true,
	}

	// Serialize token data
	data, err := json.Marshal(tokenData)
	if err != nil {
		return fmt.Errorf("failed to marshal token data: %w", err)
	}

	// Get relational DB client
	client := rc.GetRelationalDB1()

	// Use fixed key "access_token" for both store and get
	tokenKey := fmt.Sprintf("%s%s", TokenKeyPrefix, "access_token")

	log.Info("Storing access token", map[string]interface{}{
		"key":          tokenKey,
		"token_length": len(accessToken),
		"expires_at":   expiryTime.Format("2006-01-02 15:04:05"),
		"data":         string(data),
	})

	pipe := client.Pipeline()

	// Store token data with expiry
	pipe.Set(ctx, tokenKey, data, time.Until(expiryTime))

	// Add to token set for tracking
	pipe.SAdd(ctx, TokenSetKey, tokenKey)

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	return err
}

// GetValidToken gets the current valid token or returns empty string if expired/invalid
func (rc *RedisCache) GetValidToken(ctx context.Context) string {
	log := logger.GetLogger()
	client := rc.GetRelationalDB1()
	tokenKey := fmt.Sprintf("%s%s", TokenKeyPrefix, "access_token")

	data, err := client.Get(ctx, tokenKey).Bytes()
	if err != nil {
		log.Error("Failed to get token from Redis", map[string]interface{}{
			"error": err.Error(),
			"key":   tokenKey,
		})
		return ""
	}

	var tokenData RedisTokenData
	if err := json.Unmarshal(data, &tokenData); err != nil {
		log.Error("Failed to unmarshal token data", map[string]interface{}{
			"error": err.Error(),
			"data":  string(data),
		})
		return ""
	}

	log.Info("Retrieved token data", map[string]interface{}{
		"key":          tokenKey,
		"token_length": len(tokenData.AccessToken),
		"is_valid":     tokenData.IsValid,
		"expires_at":   time.UnixMilli(tokenData.ExpiresAt).Format("2006-01-02 15:04:05"),
		"created_at":   time.UnixMilli(tokenData.CreatedAt).Format("2006-01-02 15:04:05"),
	})

	if tokenData.IsValid && time.UnixMilli(tokenData.ExpiresAt).After(time.Now()) {
		return tokenData.AccessToken
	}

	log.Info("Token invalid or expired", map[string]interface{}{
		"is_valid":     tokenData.IsValid,
		"expires_at":   time.UnixMilli(tokenData.ExpiresAt).Format("2006-01-02 15:04:05"),
		"current_time": time.Now().Format("2006-01-02 15:04:05"),
	})
	return ""
}

// InvalidateToken marks a token as invalid
func (rc *RedisCache) InvalidateToken(ctx context.Context, apiKey string) error {
	client := rc.GetRelationalDB1()
	tokenKey := fmt.Sprintf("%s%s", TokenKeyPrefix, apiKey)

	// Get existing token data
	data, err := client.Get(ctx, tokenKey).Bytes()
	if err != nil {
		return err
	}

	var tokenData RedisTokenData
	if err := json.Unmarshal(data, &tokenData); err != nil {
		return err
	}

	// Mark as invalid
	tokenData.IsValid = false

	// Update token data
	updatedData, err := json.Marshal(tokenData)
	if err != nil {
		return err
	}

	// Store updated data with remaining TTL
	ttl := time.Until(time.UnixMilli(tokenData.ExpiresAt))
	return client.Set(ctx, tokenKey, updatedData, ttl).Err()
}
