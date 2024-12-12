package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/redis/go-redis/v9"
	googleproto "google.golang.org/protobuf/proto"
)

var (
	redisInstance *RedisStore
	redisOnce     sync.Once
	redisMu       sync.RWMutex
)

type RedisStore struct {
	client *redis.Client
	config *config.RedisConfig
}

// NewRedisStore creates or returns existing Redis instance
func NewRedisStore() (*RedisStore, error) {
	redisMu.Lock()
	defer redisMu.Unlock()

	var initErr error
	redisOnce.Do(func() {
		// Get config directly
		cfg := config.GetConfig()
		redisInstance = &RedisStore{}
		initErr = redisInstance.initialize(&cfg.Redis)
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize Redis: %w", initErr)
	}

	return redisInstance, nil
}

// initialize is internal initialization method
func (r *RedisStore) initialize(cfg *config.RedisConfig) error {
	log := logger.GetLogger()

	// Configure Redis with persistence
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       0,

		// Connection pool settings
		PoolSize:     cfg.MaxConnections,
		MinIdleConns: cfg.MinConnections,

		// Timeouts
		DialTimeout:  cfg.GetConnectTimeout(),
		ReadTimeout:  time.Second * 3,
		WriteTimeout: time.Second * 3,

		// Connection lifetime
		ConnMaxLifetime: cfg.GetMaxConnLifetime(),
		ConnMaxIdleTime: cfg.GetMaxConnIdleTime(),

		// Enable client-side persistence checks
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			// Verify persistence settings
			if cfg.Persistence.AOFEnabled {
				cmd := cn.Do(ctx, "CONFIG", "GET", "appendonly")
				if res, err := cmd.Result(); err != nil {
					return err
				} else if res.([]interface{})[1].(string) != "yes" {
					return fmt.Errorf("AOF persistence not enabled")
				}

				// Set fsync mode
				if err := cn.Do(ctx, "CONFIG", "SET", "appendfsync", cfg.Persistence.AOFSync).Err(); err != nil {
					return fmt.Errorf("failed to set appendfsync: %w", err)
				}
			}
			return nil
		},
	})

	// Test connection
	if err := client.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	r.client = client
	r.config = cfg

	log.Info("Connected to Redis", map[string]interface{}{
		"host":           cfg.Host,
		"port":           cfg.Port,
		"max_conns":      cfg.MaxConnections,
		"min_conns":      cfg.MinConnections,
		"aof_enabled":    cfg.Persistence.AOFEnabled,
		"aof_sync":       cfg.Persistence.AOFSync,
		"rdb_enabled":    cfg.Persistence.RDBEnabled,
		"save_intervals": cfg.Persistence.SaveIntervals,
	})

	return nil
}

// StoreTick stores a tick with persistence guarantees
func (r *RedisStore) StoreTick(tick *proto.TickData) error {
	ctx := context.Background()

	// Create key with date for partitioning
	date := time.Unix(tick.Timestamp, 0).Format("2006-01-02")
	key := fmt.Sprintf("tick:%s:%d:%d",
		date,
		tick.InstrumentToken,
		tick.Timestamp,
	)

	// Serialize tick
	data, err := googleproto.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick: %w", err)
	}

	// Use MULTI/EXEC for atomic operations
	pipe := r.client.Pipeline()

	// Store tick data with expiry
	pipe.Set(ctx, key, data, 24*time.Hour)

	// Add to sorted set for time-based queries
	score := float64(tick.Timestamp)
	zkey := fmt.Sprintf("ticks:%s:%d", date, tick.InstrumentToken)
	pipe.ZAdd(ctx, zkey, redis.Z{
		Score:  score,
		Member: key,
	})

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	return err
}

// GetTicksInRange retrieves ticks for a given time range
func (r *RedisStore) GetTicksInRange(token uint32, start, end time.Time) ([]*proto.TickData, error) {
	ctx := context.Background()

	// Get keys from sorted set
	date := start.Format("2006-01-02")
	zkey := fmt.Sprintf("ticks:%s:%d", date, token)

	keys, err := r.client.ZRangeByScore(ctx, zkey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start.Unix()),
		Max: fmt.Sprintf("%d", end.Unix()),
	}).Result()

	if err != nil {
		return nil, err
	}

	// Get ticks
	var ticks []*proto.TickData
	for _, key := range keys {
		data, err := r.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		tick := &proto.TickData{}
		if err := googleproto.Unmarshal(data, tick); err != nil {
			continue
		}
		ticks = append(ticks, tick)
	}

	return ticks, nil
}

// Close closes Redis connection
func (r *RedisStore) Close() {
	if r.client != nil {
		r.client.Close()
		logger.GetLogger().Info("Redis connection closed", nil)
	}
}
