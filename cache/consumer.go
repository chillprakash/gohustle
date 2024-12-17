package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/redis/go-redis/v9"
	googleproto "google.golang.org/protobuf/proto"
)

type Consumer struct {
	numLists         int
	primaryWorkers   []*Worker
	secondaryWorkers []*Worker
	log              *logger.Logger
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	redisCache       *RedisCache
}

type Worker struct {
	id           int
	isPrimary    bool
	assignedList int // -1 for secondary workers
	log          *logger.Logger
	ctx          context.Context
	batchSize    int
	redisCache   *RedisCache
}

type WorkerMetrics struct {
	ProcessedTicks uint64
	FailedTicks    uint64
	BatchesWritten uint64
	LastProcessed  time.Time
	mu             sync.Mutex
}

func NewConsumer() *Consumer {
	cfg := config.GetConfig()
	log := logger.GetLogger()
	ctx, cancel := context.WithCancel(context.Background())
	redisCache, err := NewRedisCache()
	if err != nil {
		log.Error("Failed to create Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return nil
	}

	return &Consumer{
		numLists:         cfg.Queue.PrimaryWorkers,
		primaryWorkers:   make([]*Worker, cfg.Queue.PrimaryWorkers),
		secondaryWorkers: make([]*Worker, cfg.Queue.SecondaryWorkers),
		log:              logger.GetLogger(),
		ctx:              ctx,
		cancel:           cancel,
		redisCache:       redisCache,
	}
}

func (w *Worker) processListItems(listKey string) error {
	w.log.Info("Checking list for items", map[string]interface{}{
		"worker_id":  w.id,
		"list_key":   listKey,
		"is_primary": w.isPrimary,
	})
	defaultTimeout := 1 * time.Second
	// Try to get batch of items from Redis list
	result, err := w.redisCache.GetListDB2().BRPop(w.ctx, defaultTimeout, listKey).Result()
	if err != nil {
		return fmt.Errorf("failed to pop items: %w", err)
	}

	if len(result) < 2 {
		return nil
	}

	// Process the item
	tick := &proto.TickData{}
	if err := googleproto.Unmarshal([]byte(result[1]), tick); err != nil {
		return fmt.Errorf("failed to unmarshal tick: %w", err)
	}

	timescaleDB := db.GetTimescaleDB()

	// Write to DuckDB
	if err := timescaleDB.WriteTick(tick); err != nil {
		w.log.Error("Failed to write ticks to DuckDB", map[string]interface{}{
			"error": err.Error(),
		})
		// Note: Not returning error here to continue with other operations
	} else {
		w.log.Info("Successfully wrote ticks to DuckDB", map[string]interface{}{
			"ticks_count": 1,
		})
	}

	return nil
}

func (c *Consumer) runPrimaryWorker(w *Worker) {
	defer c.wg.Done()
	cfg := config.GetConfig()

	listKey := fmt.Sprintf("%s%d", cfg.Queue.ListPrefix, w.assignedList)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			if err := w.processListItems(listKey); err != nil {
				w.log.Error("Error processing list items", map[string]interface{}{
					"error":    err.Error(),
					"list_key": listKey,
					"worker":   w.id,
				})
			}
		}
	}
}

func (c *Consumer) runSecondaryWorker(w *Worker) {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Find longest list
			listKey := c.findLongestList()
			if listKey == "" {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			if err := w.processListItems(listKey); err != nil {
				w.log.Error("Error processing list items", map[string]interface{}{
					"error":    err.Error(),
					"list_key": listKey,
					"worker":   w.id,
				})
			}
		}
	}
}

func (c *Consumer) findLongestList() string {
	cfg := config.GetConfig()
	var maxLen int64
	var longestKey string

	pipe := c.redisCache.GetListDB2().Pipeline()
	cmds := make([]*redis.IntCmd, c.numLists)

	// Queue all LLEN commands
	for i := 0; i < c.numLists; i++ {
		listKey := fmt.Sprintf("%s%d", cfg.Queue.ListPrefix, i)
		cmds[i] = pipe.LLen(c.ctx, listKey)
	}

	// Execute pipeline
	pipe.Exec(c.ctx)

	// Find longest list
	for i, cmd := range cmds {
		length, err := cmd.Result()
		if err != nil {
			continue
		}
		if length > maxLen {
			maxLen = length
			longestKey = fmt.Sprintf("%s%d", cfg.Queue.ListPrefix, i)
		}
	}

	return longestKey
}

func (c *Consumer) Start() {
	c.log.Info("Starting consumer", map[string]interface{}{
		"primary_workers":   c.numLists,
		"secondary_workers": len(c.secondaryWorkers),
	})
	defaultBatchSize := 1000
	// Start primary workers
	for i := 0; i < c.numLists; i++ {
		worker := &Worker{
			id:           i,
			isPrimary:    true,
			assignedList: i,
			log:          c.log,
			ctx:          c.ctx,
			batchSize:    defaultBatchSize,
			redisCache:   c.redisCache,
		}
		c.primaryWorkers[i] = worker
		c.wg.Add(1)
		c.log.Info("Starting primary worker", map[string]interface{}{
			"worker_id":     i,
			"assigned_list": i,
		})
		go c.runPrimaryWorker(worker)
	}

	// Start secondary workers
	for i := 0; i < len(c.secondaryWorkers); i++ {
		worker := &Worker{
			id:           i + c.numLists,
			isPrimary:    false,
			assignedList: -1,
			log:          c.log,
			ctx:          c.ctx,
			batchSize:    defaultBatchSize,
			redisCache:   c.redisCache,
		}
		c.secondaryWorkers[i] = worker
		c.wg.Add(1)
		c.log.Info("Starting secondary worker", map[string]interface{}{
			"worker_id": i + c.numLists,
		})
		go c.runSecondaryWorker(worker)
	}
}

func (c *Consumer) Stop() {
	c.log.Info("Stopping consumer", map[string]interface{}{
		"primary_workers":   len(c.primaryWorkers),
		"secondary_workers": len(c.secondaryWorkers),
	})

	c.cancel()

	// Log metrics before stopping
	for _, w := range c.primaryWorkers {
		c.log.Info("Primary worker metrics", map[string]interface{}{
			"worker_id":     w.id,
			"assigned_list": w.assignedList,
		})
	}

	for _, w := range c.secondaryWorkers {
		c.log.Info("Secondary worker metrics", map[string]interface{}{
			"worker_id": w.id,
		})
	}

	c.wg.Wait()
	c.log.Info("Consumer stopped successfully", nil)
}
