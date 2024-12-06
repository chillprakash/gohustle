package zerodha

import (
	"context"
	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/queue"
	"os"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

type KiteConnect struct {
	Kite           *kiteconnect.Client
	Tickers        []*kiteticker.Ticker
	db             *db.TimescaleDB
	config         *config.Config
	fileStore      filestore.FileStore
	tickWorkerPool chan struct{}
	tickProcessor  *TickProcessor
	asynqQueue     *queue.AsynqQueue
}

const (
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

func NewKiteConnect(database *db.TimescaleDB, cfg *config.Config, asynqQueue *queue.AsynqQueue) *KiteConnect {
	log := logger.GetLogger()
	ctx := context.Background()

	kite := &KiteConnect{
		config: cfg,

		db:            database,
		fileStore:     filestore.NewDiskFileStore(),
		Tickers:       make([]*kiteticker.Ticker, MaxConnections),
		tickProcessor: NewTickProcessor(database.GetPool()), // Initialize TickProcessor
		asynqQueue:    asynqQueue,
	}

	// Start Asynq server in a goroutine
	go func() {
		if err := asynqQueue.Start(); err != nil {
			log.Error("Failed to start Asynq server", map[string]interface{}{
				"error": err.Error(),
			})
			os.Exit(1)
		}
	}()

	log.Info("Started Asynq server", nil)

	log.Info("Initializing KiteConnect client", map[string]interface{}{
		"api_key": cfg.Kite.APIKey,
	})

	kite.Kite = kiteconnect.New(cfg.Kite.APIKey)

	token, err := kite.GetValidToken(ctx)
	if err != nil {
		log.Error("Failed to get valid token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	kite.Kite.SetAccessToken(token)

	// Initialize worker pool
	workers := cfg.Kite.TickWorkers
	if workers <= 0 {
		workers = 500 // default if not set
	}
	kite.tickWorkerPool = make(chan struct{}, workers)
	log.Info("Initialized tick worker pool", map[string]interface{}{
		"workers": workers,
	})

	// Initialize tickers
	for i := 0; i < MaxConnections; i++ {
		kite.Tickers[i] = kiteticker.New(cfg.Kite.APIKey, token)
	}

	log.Info("Successfully initialized KiteConnect", map[string]interface{}{
		"token_length": len(token),
	})

	return kite
}

// Add cleanup method
func (k *KiteConnect) Close() {

	// Add TickProcessor cleanup
	ctx := context.Background()
	k.tickProcessor.FlushTimeScale(ctx) // Final flush of DB batch
	k.tickProcessor.FlushProtobuf(ctx)  // Final flush of file batch

	// Close channels
	close(k.tickProcessor.timescaleChan)
	close(k.tickProcessor.protobufChan)
}
