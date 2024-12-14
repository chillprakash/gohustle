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
	IndexSpotDB     = 1 // For index spot data
	NiftyOptionsDB  = 2 // For NIFTY options
	SensexOptionsDB = 3 // For SENSEX options
	RelationalDB    = 4 // For relational data
	SummaryDB       = 5 // For summary data
	LTPDB           = 6 // For LTP data
	ListDB          = 7 // For list data
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

	// Get database name based on DB number
	dbName := getDBName(db)

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
			"error":   err.Error(),
			"db":      db,
			"db_name": dbName,
			"host":    rc.config.Host,
			"port":    rc.config.Port,
		})
		return nil
	}

	rc.pools[db] = client
	rc.log.Info("Created new Redis connection pool", map[string]interface{}{
		"db":             db,
		"db_name":        dbName,
		"max_conns":      rc.config.MaxConnections,
		"min_idle_conns": rc.config.MinConnections,
	})

	return client
}

// Helper methods for specific databases
func (rc *RedisCache) GetIndexSpotDB1() *redis.Client {
	return rc.getRedisClient(IndexSpotDB)
}

func (rc *RedisCache) GetNiftyOptionsDB2() *redis.Client {
	return rc.getRedisClient(NiftyOptionsDB)
}

func (rc *RedisCache) GetSensexOptionsDB3() *redis.Client {
	return rc.getRedisClient(SensexOptionsDB)
}

func (rc *RedisCache) GetRelationalDB4() *redis.Client {
	return rc.getRedisClient(RelationalDB)
}

func (rc *RedisCache) GetSummaryDB5() *redis.Client {
	return rc.getRedisClient(SummaryDB)
}

func (rc *RedisCache) GetLTPDB6() *redis.Client {
	return rc.getRedisClient(LTPDB)
}

func (rc *RedisCache) GetListDB7() *redis.Client {
	return rc.getRedisClient(ListDB)
}

// Helper function to get database names
func getDBName(db int) string {
	switch db {
	case IndexSpotDB:
		return fmt.Sprintf("ticks_index_spot_db_%d", db)
	case NiftyOptionsDB:
		return fmt.Sprintf("ticks_nifty_options_db_%d", db)
	case SensexOptionsDB:
		return fmt.Sprintf("ticks_sensex_options_db_%d", db)
	case RelationalDB:
		return fmt.Sprintf("relational_db_%d", db)
	default:
		return fmt.Sprintf("db_%d", db)
	}
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
	client := rc.GetRelationalDB4()

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
	client := rc.GetRelationalDB4()
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
	client := rc.GetRelationalDB4()
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
