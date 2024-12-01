package main

import (
	"context"
	"fmt"
	"time"

	"gohustle/config"
	"gohustle/logger"

	"github.com/redis/go-redis/v9"
)

// SampleRedisData represents a sample data structure for Redis
type SampleRedisData struct {
	Key       string
	Value     string
	ExpiresIn time.Duration
}

type RedisService struct {
	redis  *redis.Client
	logger *logger.Logger
}

func NewRedisService(redis *redis.Client) *RedisService {
	return &RedisService{
		redis:  redis,
		logger: logger.GetLogger(),
	}
}

func main() {
	log := logger.GetLogger()

	// Load config
	cfg, err := config.GetConfig()
	if err != nil {
		log.Error("Failed to load config", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Initialize Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		DB:       cfg.Redis.DB,
		PoolSize: cfg.Redis.MaxConnections,
	})
	defer rdb.Close()

	// Ping Redis to verify connection
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("Failed to connect to Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	service := NewRedisService(rdb)

	// Generate sample data
	data := []SampleRedisData{
		{
			Key:       "user:1",
			Value:     `{"name": "John Doe", "age": 30}`,
			ExpiresIn: 1 * time.Hour,
		},
		{
			Key:       "user:2",
			Value:     `{"name": "Jane Doe", "age": 25}`,
			ExpiresIn: 2 * time.Hour,
		},
		{
			Key:       "product:1",
			Value:     `{"name": "Laptop", "price": 999.99}`,
			ExpiresIn: 30 * time.Minute,
		},
	}

	// Insert data
	startTime := time.Now()
	for _, item := range data {
		err := service.SetData(ctx, item)
		if err != nil {
			log.Error("Failed to insert data", map[string]interface{}{
				"error": err.Error(),
				"key":   item.Key,
			})
			continue
		}

		// Verify insertion by reading back
		val, err := service.GetData(ctx, item.Key)
		if err != nil {
			log.Error("Failed to read data", map[string]interface{}{
				"error": err.Error(),
				"key":   item.Key,
			})
			continue
		}

		log.Info("Successfully inserted and verified data", map[string]interface{}{
			"key":   item.Key,
			"value": val,
		})
	}

	log.Info("Operation completed", map[string]interface{}{
		"duration_ms": time.Since(startTime).Milliseconds(),
		"records":     len(data),
	})

	// Optional: List all keys matching a pattern
	keys, err := service.GetKeysByPattern(ctx, "user:*")
	if err != nil {
		log.Error("Failed to list keys", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		log.Info("Found keys", map[string]interface{}{
			"pattern": "user:*",
			"keys":    keys,
		})
	}
}

func (s *RedisService) SetData(ctx context.Context, data SampleRedisData) error {
	return s.redis.Set(ctx, data.Key, data.Value, data.ExpiresIn).Err()
}

func (s *RedisService) GetData(ctx context.Context, key string) (string, error) {
	return s.redis.Get(ctx, key).Result()
}

func (s *RedisService) GetKeysByPattern(ctx context.Context, pattern string) ([]string, error) {
	return s.redis.Keys(ctx, pattern).Result()
}
