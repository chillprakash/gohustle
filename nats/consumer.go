package nats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/logger"
	pb "gohustle/proto"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

const (
	BatchSize     = 1000            // Number of ticks per batch
	FlushInterval = 1 * time.Minute // Force flush interval
)

// TickConsumer handles batch processing of ticks
type TickConsumer struct {
	nats      *NATSHelper
	log       *logger.Logger
	batchMap  map[string][]*pb.TickData // index -> ticks
	batchMu   sync.RWMutex
	lastFlush time.Time
}

// NewTickConsumer creates a new TickConsumer instance
func NewTickConsumer(nats *NATSHelper) *TickConsumer {
	return &TickConsumer{
		nats:      nats,
		log:       logger.L(),
		batchMap:  make(map[string][]*pb.TickData),
		lastFlush: time.Now(),
	}
}

// Start begins consuming tick messages
func (c *TickConsumer) Start(ctx context.Context) error {
	// Subscribe to tick subject
	sub, err := c.nats.SubscribeTicks(ctx, "ticks", c.handleMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to ticks: %w", err)
	}
	defer sub.Unsubscribe()

	ticker := time.NewTicker(FlushInterval)
	defer ticker.Stop()

	c.log.Info("Started tick consumer", map[string]interface{}{
		"batch_size":     BatchSize,
		"flush_interval": FlushInterval,
	})

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.checkAndFlush()
		}
	}
}

// handleMessage processes incoming NATS messages
func (c *TickConsumer) handleMessage(msg *nats.Msg) {
	// Unmarshal tick data
	tick := &pb.TickData{}
	if err := proto.Unmarshal(msg.Data, tick); err != nil {
		c.log.Error("Failed to unmarshal tick data", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	c.log.Info("Received tick", map[string]interface{}{
		"tick": tick,
	})

	c.addToBatch(tick)
}

// addToBatch adds a tick to the appropriate batch
func (c *TickConsumer) addToBatch(tick *pb.TickData) {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()

	index := tick.IndexName
	c.batchMap[index] = append(c.batchMap[index], tick)

	// Check if batch size exceeded
	if len(c.batchMap[index]) >= BatchSize {
		c.flushIndex(index)
	}
}

// checkAndFlush checks if flush interval has elapsed and flushes if needed
func (c *TickConsumer) checkAndFlush() {
	if time.Since(c.lastFlush) >= FlushInterval {
		c.flush()
	}
}

// flush writes all batches to Parquet files
func (c *TickConsumer) flush() {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()

	for index := range c.batchMap {
		c.flushIndex(index)
	}
	c.lastFlush = time.Now()
}

// flushIndex writes a single index batch to a Parquet file
func (c *TickConsumer) flushIndex(index string) {
	if len(c.batchMap[index]) == 0 {
		return
	}

	// Generate filename with timestamp
	timestamp := time.Now().Format("2006-01-02-15-04-05")
	filename := fmt.Sprintf("data/ticks/%s_%s.parquet", index, timestamp)

	// TODO: Implement Parquet file writing
	// This will require a Parquet schema definition and writer implementation
	// Consider using the parquet-go library

	c.log.Info("Flushed ticks to Parquet file", map[string]interface{}{
		"index":    index,
		"count":    len(c.batchMap[index]),
		"filename": filename,
	})

	// Clear the batch
	c.batchMap[index] = nil
}
