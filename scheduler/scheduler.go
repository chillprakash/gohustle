package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/redis/go-redis/v9"
	googleproto "google.golang.org/protobuf/proto"
)

// Batch configuration
const (
	batchSize    = 1000
	maxRetries   = 3
	batchTimeout = 5 * time.Second

	// Redis keys
	lastProcessedKeyPrefix = "scheduler:last_processed:"
	defaultLookbackPeriod  = 60 * time.Second
)

// Metrics for monitoring
type Metrics struct {
	ProcessedTicks uint64
	FailedTicks    uint64
	BatchesCreated uint64
	BatchesFailed  uint64
	LastProcessed  time.Time
	ProcessingLag  time.Duration
	mu             sync.Mutex
}

type Scheduler struct {
	redisCache *cache.RedisCache
	log        *logger.Logger
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	metrics    *Metrics
	batch      []*proto.TickData
	batchMu    sync.Mutex
}

func NewScheduler() *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	redisCache, _ := cache.NewRedisCache()

	return &Scheduler{
		redisCache: redisCache,
		log:        logger.GetLogger(),
		ctx:        ctx,
		cancel:     cancel,
		metrics:    &Metrics{},
		batch:      make([]*proto.TickData, 0, batchSize),
	}
}

func (s *Scheduler) Start() {
	// Start all schedulers
	s.wg.Add(4)

	// NIFTY Index Scheduler
	go s.runIndexScheduler("NIFTY")

	// SENSEX Index Scheduler
	go s.runIndexScheduler("SENSEX")

	// NIFTY Options Scheduler
	go s.runOptionsScheduler("NIFTY", s.redisCache.GetNiftyOptionsDB2())

	// SENSEX Options Scheduler
	go s.runOptionsScheduler("SENSEX", s.redisCache.GetSensexOptionsDB3())
}

func (s *Scheduler) Stop() {
	s.log.Info("Stopping scheduler...", nil)

	// Signal shutdown to all goroutines
	s.cancel()

	// Create a channel for timeout
	done := make(chan struct{})

	go func() {
		// Wait for all goroutines to finish
		s.wg.Wait()
		close(done)
	}()

	// Wait for graceful shutdown with timeout
	select {
	case <-done:
		s.log.Info("Scheduler stopped successfully", nil)
	case <-time.After(5 * time.Second):
		s.log.Error("Scheduler stop timed out, some goroutines may not have cleaned up", nil)
	}

	// Ensure Redis connections are closed
	if s.redisCache != nil {
		s.redisCache.Close()
	}
}

func (s *Scheduler) runIndexScheduler(index string) {
	defer s.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	batchTicker := time.NewTicker(batchTimeout)
	defer ticker.Stop()
	defer batchTicker.Stop()

	actualDataRedis := s.getRedisDBForIndex(index)
	if actualDataRedis == nil {
		s.log.Error("Invalid actual data Redis client", nil)
		return
	}

	s.log.Info("Starting index scheduler", map[string]interface{}{
		"index":     index,
		"actual_db": fmt.Sprintf("db_%d", actualDataRedis.Options().DB),
	})

	retryCount := 0

	for {
		select {
		case <-s.ctx.Done():
			s.log.Info("Shutting down index scheduler", map[string]interface{}{
				"index": index,
			})
			if err := s.flushBatch(index); err != nil {
				s.log.Error("Failed to flush final batch", map[string]interface{}{
					"error": err.Error(),
					"index": index,
				})
			}
			return

		case <-batchTicker.C:
			// Flush batch on timeout if there's data
			if len(s.batch) > 0 {
				if err := s.flushBatch(index); err != nil {
					s.log.Error("Failed to flush batch on timeout", map[string]interface{}{
						"error": err.Error(),
						"index": index,
					})
				}
			}

		case <-ticker.C:
			currentTime := time.Now().Unix()

			// Get last processed time with error recovery
			lastProcessedTime, err := s.getLastProcessedTime(index)
			if err != nil {
				if retryCount < maxRetries {
					retryCount++
					s.log.Error("Failed to get last processed time, retrying", map[string]interface{}{
						"error": err.Error(),
						"retry": retryCount,
						"index": index,
					})
					continue
				}
				s.log.Error("Max retries reached for getting last processed time", map[string]interface{}{
					"error": err.Error(),
					"index": index,
				})
				continue
			}
			retryCount = 0 // Reset retry count on success

			// Log processing attempt
			s.log.Info("Processing ticks", map[string]interface{}{
				"index": index,
				"from":  time.Unix(lastProcessedTime, 0).Format("15:04:05"),
				"to":    time.Unix(currentTime, 0).Format("15:04:05"),
			})

			// Get tick keys from summary DB
			setKey := fmt.Sprintf("index:%s:ticks", index)
			tickKeys, err := s.redisCache.GetSummaryDB5().ZRangeByScore(s.ctx, setKey, &redis.ZRangeBy{
				Min: fmt.Sprintf("%d", lastProcessedTime),
				Max: fmt.Sprintf("%d", currentTime),
			}).Result()

			if err != nil {
				s.log.Error("Failed to get tick keys", map[string]interface{}{
					"error": err.Error(),
					"index": index,
					"from":  time.Unix(lastProcessedTime, 0),
					"to":    time.Unix(currentTime, 0),
				})
				continue
			}

			if len(tickKeys) > 0 {
				s.log.Info("Found ticks to process", map[string]interface{}{
					"index": index,
					"count": len(tickKeys),
				})

				// Process each tick from the appropriate DB
				for _, key := range tickKeys {
					// Get tick data from the data DB
					data, err := actualDataRedis.Get(s.ctx, key).Bytes()
					if err != nil {
						s.metrics.FailedTicks++
						s.log.Error("Failed to get tick data", map[string]interface{}{
							"error": err.Error(),
							"key":   key,
							"db":    fmt.Sprintf("db_%d", actualDataRedis.Options().DB),
						})
						continue
					}

					tick := &proto.TickData{}
					if err := googleproto.Unmarshal(data, tick); err != nil {
						s.metrics.FailedTicks++
						continue
					}

					// Add to batch
					s.batchMu.Lock()
					s.batch = append(s.batch, tick)

					// Flush if batch is full
					if len(s.batch) >= batchSize {
						if err := s.flushBatch(index); err != nil {
							s.log.Error("Failed to flush full batch", map[string]interface{}{
								"error": err.Error(),
								"index": index,
							})
						}
					}
					s.batchMu.Unlock()
				}

				// Update last processed time
				if err := s.updateLastProcessedTime(index, currentTime); err != nil {
					s.log.Error("Failed to update last processed time", map[string]interface{}{
						"error": err.Error(),
						"index": index,
					})
				}

				s.updateMetrics(index, lastProcessedTime, currentTime)
			} else {
				s.log.Info("No new ticks found", map[string]interface{}{
					"index": index,
					"time":  time.Unix(currentTime, 0).Format("15:04:05"),
				})
			}
		}
	}
}

func (s *Scheduler) processTicks(index string, start, end int64) error {
	setKey := fmt.Sprintf("index:%s:ticks", index)

	// Get tick keys for the time range
	tickKeys, err := s.redisCache.GetSummaryDB5().ZRangeByScore(s.ctx, setKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start),
		Max: fmt.Sprintf("%d", end),
	}).Result()

	if err != nil {
		return fmt.Errorf("failed to get tick keys: %w", err)
	}

	actualDataRedis := s.getRedisDBForIndex(index)
	if actualDataRedis == nil {
		s.log.Error("Invalid actual data Redis client", nil)
		return fmt.Errorf("invalid actual data Redis client")
	}

	processedCount := 0
	// Process each tick
	for _, key := range tickKeys {
		data, err := actualDataRedis.Get(s.ctx, key).Bytes()
		if err != nil {
			s.metrics.FailedTicks++
			s.log.Error("Failed to get tick data", map[string]interface{}{
				"error": err.Error(),
				"key":   key,
			})
			continue
		}

		tick := &proto.TickData{}
		if err := googleproto.Unmarshal(data, tick); err != nil {
			s.metrics.FailedTicks++
			s.log.Error("Failed to unmarshal tick", map[string]interface{}{
				"error": err.Error(),
				"key":   key,
			})
			continue
		}

		// Process tick
		if err := s.processIndexTick(index, tick); err != nil {
			s.log.Error("Failed to process tick", map[string]interface{}{
				"error": err.Error(),
				"key":   key,
			})
			continue
		}

		processedCount++
	}

	s.log.Info("Processed index ticks", map[string]interface{}{
		"index":           index,
		"from":            time.Unix(start, 0).Format("15:04:05"),
		"to":              time.Unix(end, 0).Format("15:04:05"),
		"total_ticks":     len(tickKeys),
		"processed_ticks": processedCount,
	})

	return nil
}

// getLastProcessedTime gets the last processed timestamp from Redis
func (s *Scheduler) getLastProcessedTime(index string) (int64, error) {
	key := lastProcessedKeyPrefix + index

	// Try to get from Redis
	val, err := s.redisCache.GetRelationalDB4().Get(s.ctx, key).Int64()
	if err == redis.Nil {
		// Key doesn't exist, return current time minus lookback period
		defaultTime := time.Now().Add(-defaultLookbackPeriod).Unix()

		// Set the default value in Redis
		err = s.redisCache.GetRelationalDB4().Set(s.ctx, key, defaultTime, 24*time.Hour).Err()
		if err != nil {
			return 0, fmt.Errorf("failed to set default last processed time: %w", err)
		}

		return defaultTime, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to get last processed time: %w", err)
	}

	return val, nil
}

// updateLastProcessedTime updates the last processed timestamp in Redis
func (s *Scheduler) updateLastProcessedTime(index string, timestamp int64) error {
	key := lastProcessedKeyPrefix + index

	err := s.redisCache.GetRelationalDB4().Set(s.ctx, key, timestamp, 24*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("failed to update last processed time: %w", err)
	}

	return nil
}

// processIndexTick handles individual tick processing
func (s *Scheduler) processIndexTick(index string, tick *proto.TickData) error {
	// Here you can implement the logic to:
	// 1. Accumulate ticks for parquet file
	// 2. Calculate metrics
	// 3. Generate analytics
	return nil
}

func (s *Scheduler) runOptionsScheduler(index string, db *redis.Client) {
	defer s.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	s.log.Info("Starting options scheduler", map[string]interface{}{
		"index": index,
	})

	for {
		select {
		case <-s.ctx.Done():
			s.log.Info("Shutting down options scheduler", map[string]interface{}{
				"index": index,
			})
			return
		case t := <-ticker.C:
			if err := s.processOptionsData(index, db, t); err != nil {
				s.log.Error("Failed to process options data", map[string]interface{}{
					"error": err.Error(),
					"index": index,
					"time":  t,
				})
			}
		}
	}
}

func (s *Scheduler) processOptionsData(index string, db *redis.Client, t time.Time) error {
	// Similar to processIndexData but for options
	// You can add specific processing logic for options here
	return nil
}

// getDataDB returns the appropriate Redis client based on index type
func (s *Scheduler) getRedisDBForIndex(index string) *redis.Client {
	if index == "NIFTY" || index == "SENSEX" {
		return s.redisCache.GetIndexSpotDB1()
	}
	return nil
}

func (s *Scheduler) flushBatch(index string) error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	if len(s.batch) == 0 {
		return nil
	}

	// Create batch proto
	batch := &proto.TickBatch{
		Ticks: s.batch,
		Metadata: &proto.BatchMetadata{
			Timestamp:  time.Now().Unix(),
			BatchSize:  int32(len(s.batch)),
			RetryCount: 0,
		},
	}

	// Write batch to parquet file
	if err := filestore.WriteTickBatchToParquet(index, batch); err != nil {
		s.metrics.BatchesFailed++
		s.log.Error("Failed to write batch to parquet", map[string]interface{}{
			"error": err.Error(),
			"index": index,
			"size":  len(s.batch),
		})
		return err
	}

	// Update metrics
	s.metrics.BatchesCreated++
	s.metrics.ProcessedTicks += uint64(len(s.batch))

	// Clear batch
	s.batch = make([]*proto.TickData, 0, batchSize)

	return nil
}

func (s *Scheduler) updateMetrics(index string, start, end int64) {
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()

	s.metrics.LastProcessed = time.Now()
	s.metrics.ProcessingLag = time.Since(time.Unix(end, 0))
}
