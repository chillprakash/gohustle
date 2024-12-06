package cache

import (
	"context"
	"errors"
	"gohustle/config"
	"gohustle/logger"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	pools  map[int]*redis.Client
	log    *logger.Logger
	config *config.RedisConfig
	mu     sync.RWMutex
}

// RedisInterface defines the contract for Redis operations
type RedisInterface interface {
	// Connection management
	Close()
	GetStats(db int) map[string]interface{}

	// Database-specific client
	GetDefaultRedisDB0() *redis.Client // DB 0: Default database

	// Stream operations
	XAdd(ctx context.Context, stream string, values interface{}) error
	XRead(ctx context.Context, args *redis.XReadArgs) ([]redis.XStream, error)
	XGroupCreate(ctx context.Context, stream, group, start string) error
	XReadGroup(ctx context.Context, group, consumer string, streams ...string) ([]redis.XStream, error)
	XAck(ctx context.Context, stream, group string, ids ...string) error
	Exists(ctx context.Context, key string) (int64, error)
	SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd
	SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd
	SMembers(ctx context.Context, key string) *redis.StringSliceCmd
}

// Verify RedisCache implements RedisInterface at compile time
var _ RedisInterface = (*RedisCache)(nil)

// NewRedisCache creates a new Redis cache instance with connection pooling
func NewRedisCache(config *config.RedisConfig) *RedisCache {
	log := logger.GetLogger()

	if config.MaxConnections == 0 {
		config.MaxConnections = 100 // default max connections
	}
	if config.MinConnections == 0 {
		config.MinConnections = 10 // default min connections
	}
	if config.GetConnectTimeout() == 0 {
		config.ConnectTimeout = "5s"
		config.ParseDurations()
	}
	if config.GetMaxConnLifetime() == 0 {
		config.MaxConnLifetime = "30m"
		config.ParseDurations()
	}
	if config.GetMaxConnIdleTime() == 0 {
		config.MaxConnIdleTime = "5m"
		config.ParseDurations()
	}

	return &RedisCache{
		pools:  make(map[int]*redis.Client),
		log:    log,
		config: config,
		mu:     sync.RWMutex{},
	}
}

// getRedisClient gets or creates a Redis client for the specified database with connection pooling
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

// Helper methods for specific databases
func (rc *RedisCache) GetDefaultRedisDB0() *redis.Client {
	return rc.getRedisClient(1)
}

// GetStats returns current connection pool statistics
func (rc *RedisCache) GetStats(db int) map[string]interface{} {
	rc.mu.RLock()
	client, exists := rc.pools[db]
	rc.mu.RUnlock()

	if !exists {
		return nil
	}

	stats := client.PoolStats()
	return map[string]interface{}{
		"total_conns":    stats.TotalConns,
		"idle_conns":     stats.IdleConns,
		"stale_conns":    stats.StaleConns,
		"hits":           stats.Hits,
		"misses":         stats.Misses,
		"timeouts":       stats.Timeouts,
		"total_commands": stats.TotalConns,
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
		} else {
			rc.log.Info("Closed Redis connection", map[string]interface{}{
				"db": db,
			})
		}
	}
}

// Implementation
func (rc *RedisCache) XAdd(ctx context.Context, stream string, values interface{}) error {
	client := rc.GetDefaultRedisDB0()
	if client == nil {
		return errors.New("redis client not initialized")
	}
	return client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Err()
}
func (rc *RedisCache) XGroupCreate(ctx context.Context, stream, group, start string) error {
	log := logger.GetLogger()

	log.Info("Attempting to create consumer group", map[string]interface{}{
		"stream": stream,
		"group":  group,
		"start":  start,
	})

	client := rc.GetDefaultRedisDB0()
	if client == nil {
		log.Error("Redis client not initialized", map[string]interface{}{
			"stream": stream,
			"group":  group,
		})
		return errors.New("redis client not initialized")
	}

	log.Debug("Got Redis client, creating group", map[string]interface{}{
		"stream": stream,
		"group":  group,
	})

	// Add debug for actual Redis command
	log.Debug("Executing XGroupCreate command", map[string]interface{}{
		"stream":        stream,
		"group":         group,
		"start":         start,
		"client_status": client.PoolStats(),
	})

	result := client.XGroupCreate(ctx, stream, group, start)
	if result.Err() != nil {
		if strings.Contains(result.Err().Error(), "BUSYGROUP") {
			log.Info("Consumer group already exists", map[string]interface{}{
				"stream": stream,
				"group":  group,
				"start":  start,
			})
			return nil // Not an error case
		}
		log.Error("XGroupCreate command failed", map[string]interface{}{
			"error":      result.Err().Error(),
			"stream":     stream,
			"group":      group,
			"start":      start,
			"raw_result": result.String(),
		})
		return result.Err()
	}

	log.Info("Successfully created new consumer group", map[string]interface{}{
		"stream": stream,
		"group":  group,
		"start":  start,
		"result": result.String(),
	})
	return nil
}

func (rc *RedisCache) XReadGroup(ctx context.Context, group, consumer string, streams ...string) ([]redis.XStream, error) {
	client := rc.GetDefaultRedisDB0()
	if client == nil {
		return nil, errors.New("redis client not initialized")
	}
	return client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  streams,
		Count:    10,
		Block:    0,
	}).Result()
}

func (rc *RedisCache) XAck(ctx context.Context, stream, group string, ids ...string) error {
	client := rc.GetDefaultRedisDB0()
	if client == nil {
		return errors.New("redis client not initialized")
	}
	return client.XAck(ctx, stream, group, ids...).Err()
}

func (rc *RedisCache) Exists(ctx context.Context, key string) (int64, error) {
	client := rc.GetDefaultRedisDB0()
	if client == nil {
		return 0, errors.New("redis client not initialized")
	}
	return client.Exists(ctx, key).Result()
}

// SIsMember checks if a member exists in a set
func (rc *RedisCache) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	log := logger.GetLogger()

	client := rc.GetDefaultRedisDB0()
	if client == nil {
		log.Error("Redis client not initialized", map[string]interface{}{
			"key":    key,
			"member": member,
		})
		return redis.NewBoolCmd(ctx)
	}

	log.Debug("Checking set membership", map[string]interface{}{
		"key":    key,
		"member": member,
	})

	return client.SIsMember(ctx, key, member)
}

// SAdd adds members to a set
func (rc *RedisCache) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	log := logger.GetLogger()

	client := rc.GetDefaultRedisDB0()
	if client == nil {
		log.Error("Redis client not initialized", map[string]interface{}{
			"key":     key,
			"members": members,
		})
		return redis.NewIntCmd(ctx)
	}

	log.Debug("Adding to set", map[string]interface{}{
		"key":     key,
		"members": members,
	})

	return client.SAdd(ctx, key, members...)
}

// Optional: Add method to get all members
func (rc *RedisCache) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	log := logger.GetLogger()

	client := rc.GetDefaultRedisDB0()
	if client == nil {
		log.Error("Redis client not initialized", map[string]interface{}{
			"key": key,
		})
		return redis.NewStringSliceCmd(ctx)
	}

	log.Debug("Getting set members", map[string]interface{}{
		"key": key,
	})

	return client.SMembers(ctx, key)
}

func (rc *RedisCache) XRead(ctx context.Context, args *redis.XReadArgs) ([]redis.XStream, error) {
	client := rc.GetDefaultRedisDB0()
	if client == nil {
		return nil, errors.New("redis client not initialized")
	}
	return client.XRead(ctx, args).Result()
}
