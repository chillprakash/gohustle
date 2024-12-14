package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/redis/go-redis/v9"
	googleproto "google.golang.org/protobuf/proto"
)

const (
	defaultBatchSize = 1000
	defaultTimeout   = 1 * time.Second
	maxRetries       = 3
)

type Consumer struct {
	numLists         int
	primaryWorkers   []*Worker
	secondaryWorkers []*Worker
	redisCache       *RedisCache
	log              *logger.Logger
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	writerPool       *filestore.WriterPool
}

type Worker struct {
	id           int
	isPrimary    bool
	assignedList int // -1 for secondary workers
	redisCache   *RedisCache
	log          *logger.Logger
	ctx          context.Context
	batchSize    int
	metrics      *WorkerMetrics
	writerPool   *filestore.WriterPool
}

type WorkerMetrics struct {
	ProcessedTicks uint64
	FailedTicks    uint64
	BatchesWritten uint64
	LastProcessed  time.Time
	mu             sync.Mutex
}

func NewConsumer(writerPool *filestore.WriterPool) *Consumer {
	cfg := config.GetConfig()
	redisCache, _ := NewRedisCache()
	ctx, cancel := context.WithCancel(context.Background())

	return &Consumer{
		numLists:         cfg.Queue.PrimaryWorkers,
		primaryWorkers:   make([]*Worker, cfg.Queue.PrimaryWorkers),
		secondaryWorkers: make([]*Worker, cfg.Queue.SecondaryWorkers),
		redisCache:       redisCache,
		log:              logger.GetLogger(),
		ctx:              ctx,
		cancel:           cancel,
		writerPool:       writerPool,
	}
}

func (w *Worker) processBatch(ticks []*proto.TickData) error {
	if len(ticks) == 0 {
		return nil
	}

	// Group ticks by target file
	fileGroups := make(map[string][]*proto.TickData)
	for _, tick := range ticks {
		fileGroups[tick.TargetFile] = append(fileGroups[tick.TargetFile], tick)
	}

	// Process each file group
	for targetFile, fileTicks := range fileGroups {
		if err := w.writeTicksToFile(targetFile, fileTicks); err != nil {
			w.metrics.mu.Lock()
			w.metrics.FailedTicks += uint64(len(fileTicks))
			w.metrics.mu.Unlock()

			w.log.Error("Failed to write ticks to file", map[string]interface{}{
				"error":       err.Error(),
				"target_file": targetFile,
				"tick_count":  len(fileTicks),
			})
			continue // Continue with next file group instead of returning error
		}
	}

	return nil
}

func (w *Worker) writeTicksToFile(targetFile string, ticks []*proto.TickData) error {
	// Create write request for each tick
	for _, tick := range ticks {
		req := &filestore.WriteRequest{
			TargetFile: targetFile,
			Tick:       tick,
			ResultChan: make(chan error, 1),
		}

		if err := w.writerPool.Write(req); err != nil {
			return fmt.Errorf("failed to queue write request: %w", err)
		}

		// Wait for result
		if err := <-req.ResultChan; err != nil {
			return fmt.Errorf("failed to write tick: %w", err)
		}
	}
	return nil
}

func (w *Worker) processListItems(listKey string) error {
	w.log.Debug("Checking list for items", map[string]interface{}{
		"worker_id":  w.id,
		"list_key":   listKey,
		"is_primary": w.isPrimary,
	})

	// Try to get batch of items from Redis list
	result, err := w.redisCache.GetListDB2().BRPop(w.ctx, defaultTimeout, listKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return fmt.Errorf("failed to pop items: %w", err)
	}

	if len(result) < 2 {
		return nil
	}

	// Process the item
	tick := &proto.TickData{}
	if err := googleproto.Unmarshal([]byte(result[1]), tick); err != nil {
		w.metrics.mu.Lock()
		w.metrics.FailedTicks++
		w.metrics.mu.Unlock()
		return fmt.Errorf("failed to unmarshal tick: %w", err)
	}

	// Create write request
	req := &filestore.WriteRequest{
		TargetFile: tick.TargetFile,
		Tick:       tick,
		ResultChan: make(chan error, 1),
	}

	// Send to writer pool
	if err := w.writerPool.Write(req); err != nil {
		w.log.Error("Failed to write tick", map[string]interface{}{
			"error":       err.Error(),
			"worker_id":   w.id,
			"target_file": tick.TargetFile,
		})
		return err
	}

	// Wait for write confirmation
	if err := <-req.ResultChan; err != nil {
		w.log.Error("Write failed", map[string]interface{}{
			"error":       err.Error(),
			"worker_id":   w.id,
			"target_file": tick.TargetFile,
		})
		return err
	}

	w.metrics.mu.Lock()
	w.metrics.ProcessedTicks++
	w.metrics.LastProcessed = time.Now()
	w.metrics.mu.Unlock()

	w.log.Info("Processed and wrote tick", map[string]interface{}{
		"worker_id":   w.id,
		"list_key":    listKey,
		"target_file": tick.TargetFile,
	})

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

	// Start primary workers
	for i := 0; i < c.numLists; i++ {
		worker := &Worker{
			id:           i,
			isPrimary:    true,
			assignedList: i,
			redisCache:   c.redisCache,
			log:          c.log,
			ctx:          c.ctx,
			batchSize:    defaultBatchSize,
			metrics:      &WorkerMetrics{},
			writerPool:   c.writerPool,
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
			redisCache:   c.redisCache,
			log:          c.log,
			ctx:          c.ctx,
			batchSize:    defaultBatchSize,
			metrics:      &WorkerMetrics{},
			writerPool:   c.writerPool,
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
			"worker_id":       w.id,
			"processed_ticks": w.metrics.ProcessedTicks,
			"failed_ticks":    w.metrics.FailedTicks,
			"batches_written": w.metrics.BatchesWritten,
			"last_processed":  w.metrics.LastProcessed,
			"assigned_list":   w.assignedList,
		})
	}

	for _, w := range c.secondaryWorkers {
		c.log.Info("Secondary worker metrics", map[string]interface{}{
			"worker_id":       w.id,
			"processed_ticks": w.metrics.ProcessedTicks,
			"failed_ticks":    w.metrics.FailedTicks,
			"batches_written": w.metrics.BatchesWritten,
			"last_processed":  w.metrics.LastProcessed,
		})
	}

	c.wg.Wait()
	c.log.Info("Consumer stopped successfully", nil)
}
