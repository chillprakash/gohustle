package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/db"
	"gohustle/logger"
	pb "gohustle/proto"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultConsumerWorkers = 10
	ChannelBuffer          = 50000 // Increased from 10000 to 100000 for high-frequency data
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
	const MaxBatchSize = 100
	const BatchTimeout = 25 * time.Millisecond

	c.log.Info("Started message processor", map[string]interface{}{
		"worker_id": workerId,
	})

	batch := make([]*pb.TickData, 0, MaxBatchSize)
	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()

	flushBatch := func() {
		if len(batch) == 0 {
			return
		}
		ctx := context.Background()
		// Group ticks by table name
		ticksByTable := make(map[string][]*pb.TickData)
		for _, tick := range batch {
			tableName := ""
			if tableName == "" {
				c.log.Error("Invalid table name", map[string]interface{}{
					"worker_id": workerId,
					"token":     tick.InstrumentToken,
				})
				continue
			}
			ticksByTable[tableName] = append(ticksByTable[tableName], tick)
		}
		// Write each group to DB
		totalProcessed := 0
		for tableName, ticks := range ticksByTable {
			if err := db.GetTimescaleDB().WriteTicks(ctx, tableName, ticks); err != nil {
				c.log.Error("Failed to batch write ticks to TimescaleDB", map[string]interface{}{
					"worker_id": workerId,
					"batch_len": len(ticks),
					"table":     tableName,
					"error":     err.Error(),
				})
				c.incrementErrorCount()
			} else {
				totalProcessed += len(ticks)
			}
		}
		c.incrementProcessedCount(uint64(totalProcessed))
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flushBatch()
			c.log.Info("Message processor shutting down", map[string]interface{}{
				"worker_id": workerId,
			})
			return
		case <-ticker.C:
			flushBatch()
		case msg := <-c.msgChan:
			tick := &pb.TickData{}
			if err := proto.Unmarshal(msg.Data, tick); err != nil {
				c.log.Error("Failed to unmarshal tick data", map[string]interface{}{
					"worker_id": workerId,
					"error":     err.Error(),
				})
				c.incrementErrorCount()
				continue
			}

			batch = append(batch, tick)
			if len(batch) >= MaxBatchSize {
				flushBatch()
			}
		}
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
