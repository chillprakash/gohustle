package cache

import (
	"context"
	"fmt"
	"gohustle/config"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	ltpDB3 *redis.Client
}

var (
	redisInstance *RedisCache
	redisMu       sync.RWMutex
	redisOnce     sync.Once
)

// GetRedisCache returns the singleton instance of RedisCache
func GetRedisCache() (*RedisCache, error) {
	// Fast path: check if instance exists with read lock
	redisMu.RLock()
	if redisInstance != nil {
		redisMu.RUnlock()
		return redisInstance, nil
	}
	redisMu.RUnlock()

	// Slow path: create instance with write lock
	var initErr error
	redisOnce.Do(func() {
		redisMu.Lock()
		defer redisMu.Unlock()

		// Double-check after acquiring lock
		if redisInstance != nil {
			return
		}

		instance, err := initializeRedisCache()
		if err != nil {
			initErr = fmt.Errorf("failed to initialize Redis cache: %w", err)
			return
		}
		redisInstance = instance
	})

	if initErr != nil {
		return nil, initErr
	}

	return redisInstance, nil
}

// initializeRedisCache creates and initializes a new Redis cache instance
func initializeRedisCache() (*RedisCache, error) {
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

	// Test the connections with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test connections in parallel
	errChan := make(chan error, 2)
	go func() {
		errChan <- relationalDB1.Ping(ctx).Err()
	}()
	go func() {
		errChan <- ltpDB3.Ping(ctx).Err()
	}()

	// Wait for both pings
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			// Clean up on failure
			relationalDB1.Close()
			ltpDB3.Close()
			return nil, fmt.Errorf("failed to connect to Redis: %w", err)
		}
	}

	return &RedisCache{
		ltpDB3: ltpDB3,
	}, nil
}

// GetLTPDB3 returns the Redis client for LTP database 3
func (r *RedisCache) GetLTPDB3() *redis.Client {
	return r.ltpDB3
}

// Close closes all Redis connections
func (r *RedisCache) Close() error {
	var errs []error

	// Close connections in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if r.ltpDB3 != nil {
			if err := r.ltpDB3.Close(); err != nil {
				errChan <- fmt.Errorf("failed to close Redis DB 3: %w", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		if r.ltpDB3 != nil {
			if err := r.ltpDB3.Close(); err != nil {
				errChan <- fmt.Errorf("failed to close Redis DB 3: %w", err)
			}
		}
	}()

	// Wait for all closures and collect errors
	wg.Wait()
	close(errChan)

	for err := range errChan {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing Redis connections: %v", errs)
	}

	// Reset singleton instance
	redisMu.Lock()
	redisInstance = nil
	redisMu.Unlock()

	return nil
}
