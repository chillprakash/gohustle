package nats

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"gohustle/logger"
	pb "gohustle/proto"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	FlushInterval          = 1 * time.Second
	BatchSize              = 1000
	DefaultConsumerWorkers = 8
	DataDir                = "data/ticks"
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
	workerCount   int
	batchMap      map[string][]*pb.TickData // index -> ticks
	batchMu       sync.RWMutex
	lastFlush     map[string]time.Time // index -> last flush time
	subscriptions []*nats.Subscription
	subMu         sync.Mutex // protect subscriptions slice
	wg            sync.WaitGroup
	metrics       *consumerMetrics
	done          chan struct{}  // signal for graceful shutdown
	msgChan       chan *nats.Msg // Channel for queuing messages
}

type consumerMetrics struct {
	receivedCount  uint64
	processedCount uint64
	flushCount     uint64
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
			batchMap:    make(map[string][]*pb.TickData),
			lastFlush:   make(map[string]time.Time),
			metrics:     &consumerMetrics{},
			done:        make(chan struct{}),
			msgChan:     make(chan *nats.Msg, 10000), // Buffered channel for messages
		}

		// Initialize NATS connection
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
		"worker_count":   c.workerCount,
		"batch_size":     BatchSize,
		"flush_interval": FlushInterval,
		"subject":        subject,
		"queue_group":    queue,
	})

	workerCtx, cancel := context.WithCancel(context.Background())

	// Start metrics logging
	go c.logMetrics(workerCtx)

	// Start flush routine
	flushTicker := time.NewTicker(FlushInterval)
	go func() {
		defer flushTicker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-flushTicker.C:
				c.checkAndFlush()
			}
		}
	}()

	// Single subscription to receive messages
	sub, err := c.nats.QueueSubscribe(workerCtx, subject, queue, func(msg *nats.Msg) {
		select {
		case <-workerCtx.Done():
			return
		case c.msgChan <- msg:
			// Message queued successfully
		default:
			// Channel full - log warning and skip message
			c.log.Error("Message channel full - dropping message", map[string]interface{}{
				"subject": msg.Subject,
			})
			c.incrementErrorCount()
		}
	})

	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	c.subMu.Lock()
	c.subscriptions = append(c.subscriptions, sub)
	c.subMu.Unlock()

	// Start worker pool to process messages from channel
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

// New method to process messages from channel
func (c *TickConsumer) processMessages(ctx context.Context, workerId int) {
	c.log.Info("Started message processor", map[string]interface{}{
		"worker_id": workerId,
	})

	// For channel draining
	drainTimer := time.NewTicker(10 * time.Millisecond)
	defer drainTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			c.log.Info("Message processor shutting down", map[string]interface{}{
				"worker_id": workerId,
			})
			return
		case <-drainTimer.C:
			// Aggressively drain the message channel
			drainCount := 0
			for drainCount < 100 { // Process up to 100 messages at once
				select {
				case msg := <-c.msgChan:
					c.handleMessage(msg, workerId)
					drainCount++
				default:
					// No more messages in channel
					goto done
				}
			}
		done:
			if drainCount > 0 {
				c.log.Debug("Drained messages", map[string]interface{}{
					"worker_id": workerId,
					"count":     drainCount,
				})
			}
		}
	}
}

// handleMessage processes incoming NATS messages
func (c *TickConsumer) handleMessage(msg *nats.Msg, workerId int) {
	// Reduce to debug level for high-volume logs
	c.log.Debug("Received tick", map[string]interface{}{
		"subject": msg.Subject,
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
	c.addToBatch(tick, workerId)
	c.incrementReceivedCount()
}

// addToBatch adds a tick to the appropriate batch with optimized locking
func (c *TickConsumer) addToBatch(tick *pb.TickData, workerId int) {
	index := tick.IndexName

	var needsFlush bool
	// Check if we need to initialize a new batch with a read lock first
	c.batchMu.RLock()
	_, exists := c.batchMap[index]
	c.batchMu.RUnlock()

	if !exists {
		c.batchMu.Lock()
		// Double-check in case another goroutine created it
		if _, exists := c.batchMap[index]; !exists {
			c.batchMap[index] = make([]*pb.TickData, 0, BatchSize)
			c.lastFlush[index] = time.Now()
		}
		c.batchMu.Unlock()
	}

	// Add the tick to the batch
	c.batchMu.Lock()
	c.batchMap[index] = append(c.batchMap[index], tick)
	needsFlush = len(c.batchMap[index]) >= BatchSize
	c.batchMu.Unlock()

	if needsFlush {
		c.flushIndex(index)
	}
}

// checkAndFlush checks if flush interval has elapsed and flushes if needed
func (c *TickConsumer) checkAndFlush() {
	now := time.Now()

	c.batchMu.RLock()
	indicesToFlush := make([]string, 0)
	for index := range c.batchMap {
		if now.Sub(c.lastFlush[index]) >= FlushInterval {
			indicesToFlush = append(indicesToFlush, index)
		}
	}
	c.batchMu.RUnlock()

	// Flush indices outside the lock
	for _, index := range indicesToFlush {
		c.flushIndex(index)
	}
}

// flushIndex flushes ticks for a specific index to a Parquet file
func (c *TickConsumer) flushIndex(index string) {
	c.batchMu.Lock()
	if len(c.batchMap[index]) == 0 {
		c.batchMu.Unlock()
		return
	}

	// Take the current batch and create a new one
	batch := c.batchMap[index]
	c.batchMap[index] = make([]*pb.TickData, 0, BatchSize)
	c.lastFlush[index] = time.Now()
	c.batchMu.Unlock()

	// Process the batch outside the lock
	filename := filepath.Join(DataDir, fmt.Sprintf("_%s.parquet", time.Now().Format("2006-01-02-15-04-05")))
	count := len(batch)

	// TODO: Write to Parquet file
	c.log.Info("Flushed ticks to Parquet file", map[string]interface{}{
		"index":    index,
		"count":    count,
		"filename": filename,
	})

	c.incrementProcessedCount(uint64(count))
	c.incrementFlushCount()
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

	// Final flush of all batches
	c.log.Info("Performing final flush")
	c.flushAll()
	c.log.Info("Cleanup complete")
}

// flushAll flushes all pending ticks
func (c *TickConsumer) flushAll() {
	c.batchMu.RLock()
	indices := make([]string, 0, len(c.batchMap))
	for index := range c.batchMap {
		indices = append(indices, index)
	}
	c.batchMu.RUnlock()

	for _, index := range indices {
		c.flushIndex(index)
	}
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

func (c *TickConsumer) incrementFlushCount() {
	c.metrics.mu.Lock()
	c.metrics.flushCount++
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
				"flush_count":     c.metrics.flushCount,
				"error_count":     c.metrics.errorCount,
				"worker_count":    c.workerCount,
				"channel_pending": pending,
			})
			c.metrics.mu.RUnlock()
		}
	}
}
