package zerodha

import (
	"context"
	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"os"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

type KiteConnect struct {
	Kite           *kiteconnect.Client
	Tickers        []*kiteticker.Ticker
	db             *db.TimescaleDB
	config         *config.KiteConfig
	fileStore      filestore.FileStore
	tickWorkerPool chan struct{}
}

const (
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

func NewKiteConnect(database *db.TimescaleDB, cfg *config.KiteConfig) *KiteConnect {
	log := logger.GetLogger()
	ctx := context.Background()

	kite := &KiteConnect{
		config:    cfg,
		db:        database,
		fileStore: filestore.NewDiskFileStore(),
		Tickers:   make([]*kiteticker.Ticker, MaxConnections),
	}

	log.Info("Initializing KiteConnect client", map[string]interface{}{
		"api_key": cfg.APIKey,
	})

	kite.Kite = kiteconnect.New(cfg.APIKey)

	token, err := kite.GetValidToken(ctx)
	if err != nil {
		log.Error("Failed to get valid token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	kite.Kite.SetAccessToken(token)

	// Initialize worker pool
	workers := cfg.TickWorkers
	if workers <= 0 {
		workers = 500 // default if not set
	}
	kite.tickWorkerPool = make(chan struct{}, workers)
	log.Info("Initialized tick worker pool", map[string]interface{}{
		"workers": workers,
	})

	// Initialize tickers
	for i := 0; i < MaxConnections; i++ {
		kite.Tickers[i] = kiteticker.New(cfg.APIKey, token)
	}

	log.Info("Successfully initialized KiteConnect", map[string]interface{}{
		"token_length": len(token),
	})

	return kite
}
