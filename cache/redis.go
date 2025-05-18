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
	cacheDB1      *redis.Client
	positionsDB2  *redis.Client
	ltpDB3        *redis.Client
	timeSeriesDB4 *redis.Client // New client for time series data
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

	cacheDB1 := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           1,
		MinIdleConns: cfg.Redis.MinConnections,
		PoolSize:     cfg.Redis.MaxConnections,
	})

	// Initialize Redis client for relational DB 1
	positionsDB2 := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           2, // Use DB 2 for positions data
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

	// Initialize Redis client for time series data (DB 4)
	timeSeriesDB4 := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", cfg.Redis.Host, cfg.Redis.Port),
		Password:     cfg.Redis.Password,
		DB:           4,                            // Use DB 4 for time series data
		MinIdleConns: cfg.Redis.MinConnections * 2, // Double the min connections for time series
		PoolSize:     cfg.Redis.MaxConnections * 2, // Double the pool size for time series
		ReadTimeout:  200 * time.Millisecond,       // Shorter read timeout
		WriteTimeout: 200 * time.Millisecond,       // Shorter write timeout
		MaxRetries:   3,                            // Add retries for resilience
		PoolTimeout:  300 * time.Millisecond,       // Pool timeout slightly higher than R/W timeout
	})

	// Test the connections with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test connections in parallel
	errChan := make(chan error, 3) // Increased to 3 for new DB
	go func() {
		errChan <- positionsDB2.Ping(ctx).Err()
	}()
	go func() {
		errChan <- ltpDB3.Ping(ctx).Err()
	}()
	go func() {
		errChan <- timeSeriesDB4.Ping(ctx).Err()
	}()
	go func() {
		errChan <- cacheDB1.Ping(ctx).Err()
	}()

	// Wait for all pings
	for i := 0; i < 3; i++ { // Changed to 3
		if err := <-errChan; err != nil {
			// Clean up on failure
			positionsDB2.Close()
			ltpDB3.Close()
			timeSeriesDB4.Close()
			cacheDB1.Close()
			return nil, fmt.Errorf("failed to connect to Redis: %w", err)
		}
	}

	return &RedisCache{
		ltpDB3:        ltpDB3,
		positionsDB2:  positionsDB2,
		timeSeriesDB4: timeSeriesDB4,
		cacheDB1:      cacheDB1,
	}, nil
}

// GetLTPDB3 returns the Redis client for LTP database 3
func (r *RedisCache) GetLTPDB3() *redis.Client {
	return r.ltpDB3
}

func (r *RedisCache) GetPositionsDB2() *redis.Client {
	return r.positionsDB2
}

// GetTimeSeriesDB returns the Redis client for time series database
func (r *RedisCache) GetTimeSeriesDB() *redis.Client {
	return r.timeSeriesDB4
}

func (r *RedisCache) GetCacheDB1() *redis.Client {
	return r.cacheDB1
}

// Close closes all Redis connections
func (r *RedisCache) Close() error {
	var errs []error
	var wg sync.WaitGroup
	errChan := make(chan error, 10) // Buffer for parallel errors

	// Function to safely close a Redis client
	closeClient := func(name string, client *redis.Client) {
		if client == nil {
			wg.Done()
			return
		}
		if err := client.Close(); err != nil {
			errChan <- fmt.Errorf("failed to close %s: %w", name, err)
		}
		wg.Done()
	}

	// List of all Redis clients to close
	clients := []struct {
		name   string
		client *redis.Client
	}{
		{"cache DB1", r.cacheDB1},
		{"positions DB2", r.positionsDB2},
		{"LTP DB3", r.ltpDB3},
		{"time series DB4", r.timeSeriesDB4},
	}

	// Count non-nil clients
	var clientCount int
	for _, c := range clients {
		if c.client != nil {
			clientCount++
		}
	}

	// Set the WaitGroup counter
	wg.Add(clientCount)

	// Close all clients in parallel
	for _, c := range clients {
		if c.client != nil {
			go closeClient(c.name, c.client)
		}
	}

	// Wait for all closures to complete
	wg.Wait()
	close(errChan)

	// Collect any errors
	for err := range errChan {
		if err != nil {
			errs = append(errs, err)
		}
	}

	// Reset singleton instance
	redisMu.Lock()
	redisInstance = nil
	redisMu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("errors closing Redis connections: %v", errs)
	}
	return nil
}

// Ping tests the Redis connection
func (c *RedisCache) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test each Redis DB
	dbs := []struct {
		name string
		db   *redis.Client
	}{
		{"LTP_DB3", c.GetLTPDB3()},
		{"Positions_DB2", c.GetPositionsDB2()},
		{"TimeSeries_DB4", c.GetTimeSeriesDB()},
		{"Cache_DB1", c.GetCacheDB1()},
	}

	for _, db := range dbs {
		if db.db == nil {
			return fmt.Errorf("%s not initialized", db.name)
		}
		if err := db.db.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("failed to ping %s: %w", db.name, err)
		}
	}

	return nil
}
