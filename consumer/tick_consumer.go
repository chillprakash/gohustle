package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/zerodha"

	"github.com/hibiken/asynq"
	googleproto "google.golang.org/protobuf/proto"
)

type TickConsumer struct {
	baseDir string
	kite    *zerodha.KiteConnect
	mu      sync.Mutex
}

func NewTickConsumer(cfg *config.Config, kite *zerodha.KiteConnect) *TickConsumer {
	return &TickConsumer{
		baseDir: "data/ticks",
		kite:    kite,
	}
}

func (c *TickConsumer) handleTickFile(ctx context.Context, t *asynq.Task) error {
	// Unmarshal the tick from the task payload
	tick := &proto.TickData{}
	if err := googleproto.Unmarshal(t.Payload(), tick); err != nil {
		return fmt.Errorf("failed to unmarshal tick: %w", err)
	}

	// Get the token info from cache
	tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
	tokenInfo, exists := c.kite.GetInstrumentInfo(tokenStr)
	if !exists {
		return fmt.Errorf("token not found in lookup: %s", tokenStr)
	}

	// Generate filename
	tickTime := time.Unix(tick.Timestamp, 0)
	filename := c.generateFilename(tokenInfo, tickTime)

	// Write the tick to file
	return c.writeTick(tick, filename)
}

func (c *TickConsumer) generateFilename(tokenInfo *zerodha.TokenInfo, tickTime time.Time) string {
	if tokenInfo.IsIndex {
		return fmt.Sprintf("%s/%s_%02d%02d%d.pb",
			c.baseDir,
			tokenInfo.Index,
			tickTime.Day(), tickTime.Month(), tickTime.Year())
	}

	return fmt.Sprintf("%s/%s_%s_%02d%02d%d.pb",
		c.baseDir,
		tokenInfo.Index,
		tokenInfo.Expiry.Format("20060102"),
		tickTime.Day(), tickTime.Month(), tickTime.Year())
}

func (c *TickConsumer) writeTick(tick *proto.TickData, filename string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create directory if not exists
	if err := os.MkdirAll(c.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Read existing batch if file exists
	var batch proto.TickBatch
	if data, err := os.ReadFile(filename); err == nil {
		if err := googleproto.Unmarshal(data, &batch); err == nil {
			batch.Ticks = append(batch.Ticks, tick)
		}
	} else {
		// Create new batch if file doesn't exist
		batch.Ticks = []*proto.TickData{tick}
	}

	// Update metadata
	batch.Metadata = &proto.BatchMetadata{
		Timestamp:  time.Now().Unix(),
		BatchSize:  int32(len(batch.Ticks)),
		RetryCount: 0,
	}

	// Marshal and write
	data, err := googleproto.Marshal(&batch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	logger.GetLogger().Debug("Wrote tick to file", map[string]interface{}{
		"filename":   filename,
		"batch_size": len(batch.Ticks),
		"token":      tick.InstrumentToken,
		"price":      tick.LastPrice,
		"time":       time.Unix(tick.Timestamp, 0),
	})

	return nil
}

func StartTickConsumer(cfg *config.Config, kite *zerodha.KiteConnect) {
	log := logger.GetLogger()
	log.Info("Starting File Tick Consumer", nil)

	consumer := NewTickConsumer(cfg, kite)

	// Register handler for file processing queue
	kite.GetAsynqQueue().HandleFunc("process_tick_file", consumer.handleTickFile)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
