package zerodha

import (
	"context"
	"gohustle/config"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

const (
	MaxConnections         = 3
	MaxTokensPerConnection = 3000
)

type KiteConnect struct {
	Kite    *kiteconnect.Client
	Tickers []*kiteticker.Ticker
}

func NewKiteConnect(connectTicker bool) *KiteConnect {
	log := logger.GetLogger()

	// Get config directly
	cfg := config.GetConfig()

	kite := &KiteConnect{
		Tickers: make([]*kiteticker.Ticker, MaxConnections),
	}

	log.Info("Initializing KiteConnect client", map[string]interface{}{
		"api_key": cfg.Kite.APIKey,
	})

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
		log.Info("Setting access token", map[string]interface{}{
			"token": token,
		})
		kite.Kite.SetAccessToken(token)
	}

	// Connect ticker if requested
	if connectTicker {
		if err := kite.ConnectTicker(); err != nil {
			log.Error("Failed to connect ticker", map[string]interface{}{
				"error": err.Error(),
			})
			return nil
		}
	}

	log.Info("Successfully initialized KiteConnect", map[string]interface{}{
		"connect_ticker": connectTicker,
		"api_key":        cfg.Kite.APIKey,
		"connections":    len(kite.Tickers),
	})
	return kite
}

// Close closes all connections
func (k *KiteConnect) Close() {
	log := logger.GetLogger()

	// Close all tickers
	for i, ticker := range k.Tickers {
		if ticker != nil {
			ticker.Close()
			log.Info("Ticker closed", map[string]interface{}{
				"connection": i + 1,
			})
		}
	}
}
