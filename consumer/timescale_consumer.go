package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/zerodha"

	"github.com/hibiken/asynq"
	googleproto "google.golang.org/protobuf/proto"
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
	log.Info("Starting Timescale Consumer", nil)

	// Initialize all required tables
	if err := InitializeTables(context.Background(), timescaleDB, kite, cfg); err != nil {
		log.Fatal("Failed to initialize tables", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Register handler for timescale processing queue
	kite.GetAsynqQueue().HandleFunc("process_tick_timescale", func(ctx context.Context, t *asynq.Task) error {
		tick := &proto.TickData{}
		if err := googleproto.Unmarshal(t.Payload(), tick); err != nil {
			return fmt.Errorf("failed to unmarshal tick: %w", err)
		}

		// Get the token info
		tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
		tokenInfo, exists := kite.GetInstrumentInfo(tokenStr)
		if !exists {
			return fmt.Errorf("token not found in lookup: %s", tokenStr)
		}

		// Generate table name using the same function
		var tableName string
		if tokenInfo.IsIndex {
			tableName = generateTableName(tokenInfo.Index, nil)
		} else {
			tableName = generateTableName(tokenInfo.Index, &tokenInfo.Expiry)
		}

		// Store tick in TimescaleDB
		if err := timescaleDB.StoreTick(tableName, tick); err != nil {
			log.Error("Failed to store tick", map[string]interface{}{
				"error":     err.Error(),
				"token":     tokenStr,
				"index":     tokenInfo.Index,
				"expiry":    tokenInfo.Expiry.Format("20060102"),
				"timestamp": time.Unix(tick.Timestamp, 0),
				"table":     tableName,
				"is_index":  tokenInfo.IsIndex,
			})
			return err
		}

		log.Info("Stored tick successfully", map[string]interface{}{
			"token":     tokenStr,
			"index":     tokenInfo.Index,
			"expiry":    tokenInfo.Expiry.Format("20060102"),
			"timestamp": time.Unix(tick.Timestamp, 0),
			"table":     tableName,
			"is_index":  tokenInfo.IsIndex,
		})

		return nil
	})

	// Add heartbeat to confirm consumer is running
	heartbeat := time.NewTicker(30 * time.Second)
	go func() {
		for range heartbeat.C {
			log.Info("TimescaleConsumer heartbeat", map[string]interface{}{
				"status": "running",
			})
		}
	}()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	heartbeat.Stop()
	log.Info("Shutting down timescale consumer", nil)
}

// Helper function to check if a table exists
func checkTableExists(ctx context.Context, db *db.TimescaleDB, tableName string) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT FROM pg_tables
			WHERE schemaname = 'public'
			AND tablename = $1
		)`, tableName).Scan(&exists)
	return exists, err
}

// generateTableName creates consistent table names for both spot and tick indices
func generateTableName(index string, expiry *time.Time) string {
	if expiry == nil {
		return fmt.Sprintf("ticks_%s", strings.ToLower(index))
	}
	return fmt.Sprintf("ticks_%s_%s",
		strings.ToLower(index),
		expiry.Format("20060102"),
	)
}

// checkAndCreateTables checks if tables exist and creates them if they don't
func checkAndCreateTables(ctx context.Context, db *db.TimescaleDB, index string) error {
	log := logger.GetLogger()

	query := `SELECT * FROM create_tick_tables($1)`
	var tableName, depthTable, minuteView string
	err := db.QueryRow(ctx, query, index).Scan(&tableName, &depthTable, &minuteView)
	if err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	log.Info("Tables created successfully", map[string]interface{}{
		"tick_table":  tableName,
		"depth_table": depthTable,
		"minute_view": minuteView,
	})

	return nil
}

// InitializeTables creates all necessary tables for spot and derived indices
func InitializeTables(ctx context.Context, db *db.TimescaleDB, kite *zerodha.KiteConnect, cfg *config.Config) error {
	log := logger.GetLogger()
	log.Info("Initializing TimescaleDB tables", nil)

	// Use a map to track which indices we've already processed
	processedIndices := make(map[string]bool)

	// Create tables for spot indices
	for _, index := range cfg.Indices.SpotIndices {
		if processedIndices[index] {
			continue
		}
		if err := checkAndCreateTables(ctx, db, index); err != nil {
			return fmt.Errorf("failed to create spot index table: %w", err)
		}
		processedIndices[index] = true
	}

	// Create tables for derived indices
	for _, index := range cfg.Indices.DerivedIndices {
		if processedIndices[index] {
			continue
		}
		if err := checkAndCreateTables(ctx, db, index); err != nil {
			return fmt.Errorf("failed to create derived index table: %w", err)
		}
		processedIndices[index] = true
	}

	log.Info("Successfully initialized all tables", map[string]interface{}{
		"processed_indices": len(processedIndices),
	})
	return nil
}
