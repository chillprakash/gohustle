package nats

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/core"
	"gohustle/filestore"
	"gohustle/logger"
	pb "gohustle/proto"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultConsumerWorkers = 20
	ChannelBuffer          = 500000 // Increased from 10000 to 100000 for high-frequency data
	StreamName             = "tick_stream"
	MaxMsgAge              = 24 * time.Hour
	MaxBytes               = 1024 * 1024 * 1024 // 1GB max storage
	PendingMsgsLimit       = 1000
	TicksSubject           = "ticks"
)

var (
	consumerInstance *TickConsumer
	consumerOnce     sync.Once
	consumerMu       sync.RWMutex
)

// TickConsumer handles parallel consumption of ticks
type TickConsumer struct {
	nats          *NATSHelper
	log           *logger.Logger
	workerCount   int // Change back to int since DefaultConsumerWorkers is int
	subscriptions []*nats.Subscription
	subMu         sync.Mutex
	wg            sync.WaitGroup
	metrics       *consumerMetrics
	done          chan struct{}
	msgChan       chan *nats.Msg
	batchChan     chan []*nats.Msg
}

type consumerMetrics struct {
	receivedCount  uint64
	processedCount uint64
	errorCount     uint64
	mu             sync.RWMutex
}

// GetTickConsumer returns a singleton instance of TickConsumer
func GetTickConsumer(ctx context.Context) (*TickConsumer, error) {
	if consumerInstance != nil {
		return consumerInstance, nil
	}

	consumerMu.Lock()
	defer consumerMu.Unlock()

	var initErr error
	consumerOnce.Do(func() {
		instance := &TickConsumer{
			nats:        GetNATSHelper(),
			log:         logger.L(),
			workerCount: DefaultConsumerWorkers,
			metrics:     &consumerMetrics{},
			done:        make(chan struct{}),
			msgChan:     make(chan *nats.Msg, ChannelBuffer),
		}

		if err := instance.Initialize(ctx); err != nil {
			instance.log.Error("Failed to initialize consumer", map[string]interface{}{
				"error": err.Error(),
			})
			initErr = fmt.Errorf("failed to initialize consumer: %w", err)
			return
		}

		consumerInstance = instance
	})

	if initErr != nil {
		return nil, initErr
	}

	return consumerInstance, nil
}

// Initialize initializes the consumer
func (c *TickConsumer) Initialize(ctx context.Context) error {
	if err := c.nats.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize NATS connection: %w", err)
	}
	return nil
}

// Start begins consuming tick messages with parallel workers
func (c *TickConsumer) Start(ctx context.Context, subject string, queue string) error {
	c.log.Info("Starting tick consumer", map[string]interface{}{
		"worker_count": c.workerCount,
		"subject":      subject,
		"queue_group":  queue,
		"buffer_size":  ChannelBuffer,
	})

	workerCtx, cancel := context.WithCancel(context.Background())

	// Start metrics logging
	go c.logMetrics(workerCtx)

	// Get JetStream context
	js := c.nats.js
	if js == nil {
		return fmt.Errorf("JetStream context not available")
	}

	// Subscribe using JetStream
	sub, err := js.QueueSubscribe(subject, queue, func(msg *nats.Msg) {
		select {
		case <-workerCtx.Done():
			return
		case c.msgChan <- msg:
			// Message queued successfully
			msg.Ack()
		default:
			// Channel full - increment error and log with more details
			c.incrementErrorCount()
			c.log.Error("Message channel full - dropping message", map[string]interface{}{
				"subject":      msg.Subject,
				"channel_size": ChannelBuffer,
				"pending_msgs": len(c.msgChan),
				"worker_count": c.workerCount,
				"error_count":  c.metrics.errorCount,
			})
			msg.Term() // Tell server we won't process this message
		}
	}, nats.Durable(queue), nats.ManualAck())

	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	c.subMu.Lock()
	c.subscriptions = append(c.subscriptions, sub)
	c.subMu.Unlock()

	// Start worker pool
	for i := 0; i < c.workerCount; i++ {
		workerId := i
		c.wg.Add(1)
		go func(id int) {
			defer c.wg.Done()
			c.processMessages(workerCtx, id)
		}(workerId)
	}

	// Monitor main context for shutdown
	go func() {
		<-ctx.Done()
		c.log.Info("Initiating consumer shutdown")
		cancel()
		c.wg.Wait()
		c.cleanup()
		c.log.Info("Consumer shutdown complete")
	}()

	return nil
}

// processMessages handles messages from the channel
func (c *TickConsumer) processMessages(ctx context.Context, workerId int) {
	c.log.Info("Started message processor", map[string]interface{}{
		"worker_id": workerId,
	})

	drainTimer := time.NewTicker(5 * time.Millisecond)
	defer drainTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Message processor shutting down", map[string]interface{}{
				"worker_id": workerId,
			})
			return
		case <-drainTimer.C:
			drainCount := 0
			for drainCount < 100 { // Process up to 100 messages at once
				select {
				case msg := <-c.msgChan:
					c.handleMessage(msg, workerId)
					drainCount++
				default:
					goto done
				}
			}
		done:
			if drainCount > 0 {
				c.log.Debug("Processed messages", map[string]interface{}{
					"worker_id": workerId,
					"count":     drainCount,
				})
			}
		}
	}
}

// handleMessage processes incoming NATS messages
func (c *TickConsumer) handleMessage(msg *nats.Msg, workerId int) {
	c.log.Debug("Received tick", map[string]interface{}{
		"subject":   msg.Subject,
		"worker_id": workerId,
	})

	tick := &pb.TickData{}
	if err := proto.Unmarshal(msg.Data, tick); err != nil {
		c.log.Error("Failed to unmarshal tick data", map[string]interface{}{
			"worker_id": workerId,
			"error":     err.Error(),
		})
		c.incrementErrorCount()
		return
	}

	// Store data in Redis
	if err := c.storeTickInRedis(tick); err != nil {
		c.log.Error("Failed to store tick in Redis", map[string]interface{}{
			"worker_id": workerId,
			"error":     err.Error(),
		})
		c.incrementErrorCount()
		return
	}

	// Write to tick store
	if err := c.writeTickToStore(tick, workerId); err != nil {
		c.log.Error("Failed to write tick to store", map[string]interface{}{
			"worker_id": workerId,
			"index":     tick.InstrumentToken,
			"error":     err.Error(),
		})
		c.incrementErrorCount()
		return
	}

	c.incrementProcessedCount(1)
	c.incrementReceivedCount()
}

// storeTickInRedis handles storing tick data in Redis
func (c *TickConsumer) storeTickInRedis(tick *pb.TickData) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		return fmt.Errorf("LTP Redis DB is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	instrumentToken := fmt.Sprintf("%d", tick.InstrumentToken)

	requiredKeys := map[string]interface{}{
		fmt.Sprintf("%s_ltp", instrumentToken):                  tick.LastPrice,
		fmt.Sprintf("%s_volume", instrumentToken):               tick.VolumeTraded,
		fmt.Sprintf("%s_oi", instrumentToken):                   tick.OpenInterest,
		fmt.Sprintf("%s_average_traded_price", instrumentToken): tick.AverageTradePrice,
	}

	redisData := make(map[string]interface{})
	for key, value := range requiredKeys {
		if !isZeroValue(value) {
			redisData[key] = value
		}
	}

	if len(redisData) > 0 {
		pipe := ltpDB.Pipeline()
		for key, value := range redisData {
			strValue := convertToString(value)
			if strValue != "" {
				pipe.Set(ctx, key, strValue, 12*time.Hour)
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("failed to execute Redis pipeline for token %s: %w", instrumentToken, err)
		}
	}

	return nil
}

// writeTickToStore handles writing tick data to the file store
func (c *TickConsumer) writeTickToStore(tick *pb.TickData, workerId int) error {
	// First convert string to uint64 (as strconv.ParseUint returns uint64)
	bankNiftyToken, err := strconv.ParseUint(core.GetIndices().BANKNIFTY.InstrumentToken, 10, 32)
	if err != nil {
		// Handle error appropriately
		c.log.Error("Error converting BankNifty token", map[string]interface{}{
			"error": err.Error(),
		})
		return nil
	}

	// Now compare with the tick's instrument token
	if tick.InstrumentToken == uint32(bankNiftyToken) {
		return nil
	}

	tickStore := filestore.GetTickStore()
	if err := tickStore.WriteTick(tick); err != nil {
		return fmt.Errorf("failed to write tick to store: %w", err)
	}
	return nil
}

// Helper functions for Redis operations
func isZeroValue(v interface{}) bool {
	switch v := v.(type) {
	case int32:
		return v == 0
	case int64:
		return v == 0
	case float64:
		return v == 0
	case string:
		return v == ""
	default:
		return v == nil
	}
}

func convertToString(v interface{}) string {
	switch v := v.(type) {
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case string:
		return v
	default:
		return ""
	}
}

// cleanup handles graceful shutdown
func (c *TickConsumer) cleanup() {
	c.log.Info("Starting cleanup")

	// Unsubscribe all subscriptions
	c.subMu.Lock()
	for _, sub := range c.subscriptions {
		if err := sub.Unsubscribe(); err != nil {
			c.log.Error("Failed to unsubscribe", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}
	c.subscriptions = nil
	c.subMu.Unlock()

	// Close tick store
	tickStore := filestore.GetTickStore()
	if err := tickStore.Close(); err != nil {
		c.log.Error("Failed to close tick store", map[string]interface{}{
			"error": err.Error(),
		})
	}

	c.log.Info("Cleanup complete")
}

// Metric management functions
func (c *TickConsumer) incrementReceivedCount() {
	c.metrics.mu.Lock()
	c.metrics.receivedCount++
	c.metrics.mu.Unlock()
}

func (c *TickConsumer) incrementProcessedCount(count uint64) {
	c.metrics.mu.Lock()
	c.metrics.processedCount += count
	c.metrics.mu.Unlock()
}

func (c *TickConsumer) incrementErrorCount() {
	c.metrics.mu.Lock()
	c.metrics.errorCount++
	c.metrics.mu.Unlock()
}

// logMetrics periodically logs consumer metrics
func (c *TickConsumer) logMetrics(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-ticker.C:
			c.metrics.mu.RLock()
			pending := len(c.msgChan)

			c.log.Info("Consumer metrics", map[string]interface{}{
				"received_count":  c.metrics.receivedCount,
				"processed_count": c.metrics.processedCount,
				"error_count":     c.metrics.errorCount,
				"worker_count":    c.workerCount,
				"channel_pending": pending,
			})
			c.metrics.mu.RUnlock()
		}
	}
}
