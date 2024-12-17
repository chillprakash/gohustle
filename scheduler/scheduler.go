package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// Redis keys
	lastProcessedKeyPrefix = "scheduler:last_processed:"
	defaultLookbackPeriod  = 60 * time.Second
)

type Scheduler struct {
	ctx        context.Context
	redisCache *RedisCache
	log        *log.Logger
	wg         *sync.WaitGroup
}

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

func (s *Scheduler) updateLastProcessedTime(index string, timestamp int64) error {
	key := lastProcessedKeyPrefix + index

	err := s.redisCache.GetRelationalDB4().Set(s.ctx, key, timestamp, 24*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("failed to update last processed time: %w", err)
	}

	return nil
}

func (s *Scheduler) runIndexScheduler(index string) {
	defer s.wg.Done()

	// ... existing setup code ...

	for {
		select {
		case <-s.ctx.Done():
			// ... existing shutdown code ...

		case <-batchTicker.C:
			// ... existing batch handling code ...

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

			// ... rest of the processing code ...

			// Update last processed time after successful processing
			if len(tickKeys) > 0 {
				if err := s.updateLastProcessedTime(index, currentTime); err != nil {
					s.log.Error("Failed to update last processed time", map[string]interface{}{
						"error": err.Error(),
						"index": index,
					})
				}
			}

			// ... rest of the function remains the same ...
		}
	}
}
