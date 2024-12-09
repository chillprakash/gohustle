package zerodha

import (
	"context"
	"fmt"
	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/queue"
	"os"
	"sync"

	proto "gohustle/proto"

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
	processorFunc  func(context.Context, uint32, *proto.TickData) error
	processorMu    sync.RWMutex
}

const (
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

// Add a new type to track processor types
type ProcessorType int

const (
	FileProcessor ProcessorType = iota
	TimescaleProcessor
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
	// Close all tickers
	for _, ticker := range k.Tickers {
		if ticker != nil {
			ticker.Close()
		}
	}

	// Close worker pool
	if k.tickWorkerPool != nil {
		close(k.tickWorkerPool)
	}

	// Final cleanup of tick processor
	if k.tickProcessor != nil {
		k.tickProcessor.Close()
	}

	// Close channels
	if k.asynqQueue != nil {
		k.asynqQueue.Close()
	}
}

func (k *KiteConnect) GetFileStore() filestore.FileStore {
	return k.fileStore
}

// Modify the RegisterTickProcessor to handle processor type
func (k *KiteConnect) RegisterTickProcessor(processorFunc func(context.Context, uint32, *proto.TickData) error, useQueue bool, processorType ProcessorType) {
	if useQueue {
		k.asynqQueue.ProcessTickTask(func(ctx context.Context, token uint32, tick *proto.TickData) error {
			switch processorType {
			case FileProcessor, TimescaleProcessor:
				return processorFunc(ctx, token, tick)
			default:
				return fmt.Errorf("unknown processor type")
			}
		})
	} else {
		k.processorFunc = processorFunc
	}
}

// SetTickProcessor sets the tick processor function for direct processing
func (k *KiteConnect) SetTickProcessor(processor func(context.Context, uint32, *proto.TickData) error) {
	k.processorMu.Lock()
	defer k.processorMu.Unlock()
	k.processorFunc = processor
}

// ProcessTickTask processes a single tick directly (used by both direct and queue processing)
func (k *KiteConnect) ProcessTickTask(ctx context.Context, token uint32, tick *proto.TickData) error {
	k.processorMu.RLock()
	processor := k.processorFunc
	k.processorMu.RUnlock()

	if processor != nil {
		return processor(ctx, token, tick)
	}
	return nil
}

func (k *KiteConnect) GetAsynqQueue() *queue.AsynqQueue {
	return k.asynqQueue
}
