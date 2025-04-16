package zerodha

import (
	"context"
	"gohustle/config"
	"gohustle/logger"
	"sync"
	"time"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

const (
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

type KiteConnect struct {
	Kite        *kiteconnect.Client
	Tickers     []*kiteticker.Ticker
	tokens      []uint32
	accessToken string
}

var (
	instance *KiteConnect
	initOnce sync.Once
	mu       sync.RWMutex
)

// GetKiteConnect returns the singleton instance of KiteConnect
func GetKiteConnect() *KiteConnect {
	mu.RLock()
	if instance != nil {
		mu.RUnlock()
		return instance
	}
	mu.RUnlock()

	mu.Lock()
	defer mu.Unlock()

	// Double-check after acquiring write lock
	if instance != nil {
		return instance
	}

	// Initialize only once
	initOnce.Do(func() {
		instance = initializeKiteConnect()
	})

	return instance
}

// initializeKiteConnect creates a new KiteConnect instance
func initializeKiteConnect() *KiteConnect {
	log.Info("Starting KiteConnect initialization", map[string]interface{}{
		"max_connections":           MaxConnections,
		"max_tokens_per_connection": MaxTokensPerConnection,
	})

	// Get config
	cfg := config.GetConfig()
	log.Info("Loaded configuration", map[string]interface{}{
		"api_key": cfg.Kite.APIKey,
	})

	kite := &KiteConnect{
		Tickers: make([]*kiteticker.Ticker, MaxConnections),
	}

	// Create new Kite client
	kite.Kite = kiteconnect.New(cfg.Kite.APIKey)
	if kite.Kite == nil {
		log.Error("Failed to create KiteConnect instance", nil)
		return nil
	}

	// Get valid token and handle error
	token, err := kite.GetValidToken(context.Background())
	if err != nil {
		log.Error("Failed to get valid token", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		kite.accessToken = token
		kite.Kite.SetAccessToken(token)
	}

	log.Info("Successfully initialized KiteConnect singleton", map[string]interface{}{
		"api_key":     cfg.Kite.APIKey,
		"connections": len(kite.Tickers),
		"status":      "ready",
	})

	return kite
}

func (k *KiteConnect) InitializeTickersWithTokens(tokens []uint32) error {
	log.Info("Initializing tickers with tokens", map[string]interface{}{
		"tokens_count": len(tokens),
	})

	// Store tokens for subscription
	k.tokens = tokens

	// Connect tickers
	return k.ConnectTickers()
}

// Close closes all connections
func (k *KiteConnect) Close() {
	log.Info("Starting KiteConnect shutdown", map[string]interface{}{
		"total_tickers": len(k.Tickers),
	})

	// Close all tickers
	for i, ticker := range k.Tickers {
		if ticker != nil {
			log.Info("Closing ticker connection", map[string]interface{}{
				"connection_index": i,
			})
			ticker.Close()
			log.Info("Successfully closed ticker", map[string]interface{}{
				"connection": i + 1,
				"status":     "closed",
			})
		} else {
			log.Info("Skipping nil ticker", map[string]interface{}{
				"connection_index": i,
			})
		}
	}

	log.Info("Completed KiteConnect shutdown", map[string]interface{}{
		"status": "closed",
	})
}

func (k *KiteConnect) DownloadInstrumentData(ctx context.Context) error {
	log.Info("Starting instrument download", nil)

	// Add request ID to context
	ctx = context.WithValue(ctx, logger.RequestIDKey, "download-"+time.Now().String())

	log.Info("Starting instrument download", map[string]interface{}{
		"operation": "download",
	})

	// If there's an error
	if err != nil {
		log.Error("Download failed", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}
