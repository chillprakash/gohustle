package cache

import (
	"context"
	"fmt"
	"gohustle/config"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	relationalDB1 *redis.Client
	ltpDB3        *redis.Client
}

var redisInstance *RedisCache

// NewRedisCache creates a new Redis cache instance
func NewRedisCache() (*RedisCache, error) {
	if redisInstance != nil {
		return redisInstance, nil
	}

	cfg := config.GetConfig()

	// Initialize Redis client for relational DB 1
	relationalDB1 := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           1, // Use DB 1 for relational data
		MinIdleConns: cfg.Redis.MinConnections,
		PoolSize:     cfg.Redis.MaxConnections,
	})

	// Initialize Redis client for LTP data (DB 3)
	ltpDB3 := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           3, // Use DB 3 for LTP data
		MinIdleConns: cfg.Redis.MinConnections,
		PoolSize:     cfg.Redis.MaxConnections,
	})

	// Test the connections
	ctx := context.Background()
	if err := relationalDB1.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis DB 1: %w", err)
	}
	if err := ltpDB3.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis DB 3: %w", err)
	}

	redisInstance = &RedisCache{
		relationalDB1: relationalDB1,
		ltpDB3:        ltpDB3,
	}

	return redisInstance, nil
}

// GetRelationalDB1 returns the Redis client for relational database 1
func (r *RedisCache) GetRelationalDB1() *redis.Client {
	return r.relationalDB1
}

// GetLTPDB3 returns the Redis client for LTP database 3
func (r *RedisCache) GetLTPDB3() *redis.Client {
	return r.ltpDB3
}

// GetValidToken retrieves a valid token from Redis
func (r *RedisCache) GetValidToken(ctx context.Context) string {
	token, err := r.relationalDB1.Get(ctx, "kite:access_token").Result()
	if err != nil {
		return ""
	}
	return token
}

// StoreAccessToken stores the access token in Redis with expiry
func (r *RedisCache) StoreAccessToken(ctx context.Context, token string) error {
	return r.relationalDB1.Set(ctx, "kite:access_token", token, 24*time.Hour).Err()
}

// Close closes all Redis connections
func (r *RedisCache) Close() error {
	var errs []error
	if r.relationalDB1 != nil {
		if err := r.relationalDB1.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Redis DB 1: %w", err))
		}
	}
	if r.ltpDB3 != nil {
		if err := r.ltpDB3.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Redis DB 3: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("errors closing Redis connections: %v", errs)
	}
	return nil
}
