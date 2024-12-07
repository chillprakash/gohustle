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
	"gohustle/db"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/zerodha"
)

type TimescaleConsumer struct {
	db          *db.TimescaleDB
	batchSize   int
	batchMutex  sync.Mutex
	tickBatch   []*proto.TickData
	tokenBatch  []zerodha.TokenInfo
	flushTicker *time.Ticker
}

func NewTimescaleConsumer(db *db.TimescaleDB, batchSize int, flushInterval time.Duration) *TimescaleConsumer {
	tc := &TimescaleConsumer{
		db:          db,
		batchSize:   batchSize,
		tickBatch:   make([]*proto.TickData, 0, batchSize),
		tokenBatch:  make([]zerodha.TokenInfo, 0, batchSize),
		flushTicker: time.NewTicker(flushInterval),
	}
	return tc
}

func (tc *TimescaleConsumer) addToBatch(tick *proto.TickData, tokenInfo zerodha.TokenInfo) error {
	tc.batchMutex.Lock()
	defer tc.batchMutex.Unlock()

	tc.tickBatch = append(tc.tickBatch, tick)
	tc.tokenBatch = append(tc.tokenBatch, tokenInfo)

	if len(tc.tickBatch) >= tc.batchSize {
		return tc.flushBatch()
	}
	return nil
}

func (tc *TimescaleConsumer) flushBatch() error {
	if len(tc.tickBatch) == 0 {
		return nil
	}

	// Create batch query
	query := `INSERT INTO ticks (
		instrument_token, timestamp, last_price, volume, oi, symbol, expiry
	) VALUES ($1, $2, $3, $4, $5, $6, $7)`

	// Begin transaction
	tx, err := tc.db.GetPool().Begin(context.Background())
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	// Execute for each tick
	for i, tick := range tc.tickBatch {
		_, err = tx.Exec(context.Background(), query,
			tick.InstrumentToken,
			time.Unix(tick.Timestamp, 0),
			tick.LastPrice,
			tick.VolumeTraded,
			tick.Oi,
			tc.tokenBatch[i].Symbol,
			tc.tokenBatch[i].Expiry,
		)
		if err != nil {
			return fmt.Errorf("failed to execute insert: %w", err)
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	tc.tickBatch = tc.tickBatch[:0]
	tc.tokenBatch = tc.tokenBatch[:0]
	return nil
}

func StartTimescaleConsumer(cfg *config.Config, kite *zerodha.KiteConnect, timescaleDB *db.TimescaleDB) {
	log := logger.GetLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := NewTimescaleConsumer(timescaleDB, 1000, 5*time.Second)
	defer consumer.flushTicker.Stop()

	// Start periodic flush
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-consumer.flushTicker.C:
				consumer.batchMutex.Lock()
				if err := consumer.flushBatch(); err != nil {
					log.Error("Failed to flush batch", map[string]interface{}{
						"error": err.Error(),
					})
				}
				consumer.batchMutex.Unlock()
			}
		}
	}()

	// Register consumer with ctx
	kite.ProcessTickTask(func(ctx context.Context, token uint32, tick *proto.TickData) error {
		tokenStr := fmt.Sprintf("%d", token)
		tokenInfo, exists := kite.GetInstrumentInfo(tokenStr)
		if !exists {
			log.Error("Token not found in lookup cache", map[string]interface{}{
				"token": tokenStr,
			})
			return fmt.Errorf("token not found in lookup: %s", tokenStr)
		}

		// Add to batch
		if err := consumer.addToBatch(tick, tokenInfo); err != nil {
			log.Error("Failed to add tick to batch", map[string]interface{}{
				"error": err.Error(),
				"token": token,
			})
			return err
		}

		return nil
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Final flush on shutdown
	consumer.batchMutex.Lock()
	if err := consumer.flushBatch(); err != nil {
		log.Error("Failed to flush final batch", map[string]interface{}{
			"error": err.Error(),
		})
	}
	consumer.batchMutex.Unlock()
}
