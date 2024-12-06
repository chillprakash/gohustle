package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/zerodha"
)

func main() {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Load config
	cfg := config.GetConfig()

	// Initialize TimescaleDB
	database := db.InitDB(&cfg.Timescale)
	defer database.Close()

	// Initialize KiteConnect with interface
	kiteConnect := zerodha.NewKiteConnect(database, &cfg.Kite)

	// Download instrument data
	var err error
	err = kiteConnect.DownloadInstrumentData(ctx)
	if err != nil {
		log.Error("Failed to download instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	err = kiteConnect.SyncInstrumentExpiriesFromFileToDB(ctx)
	if err != nil {
		// handle error
	}

	// Get instrument expiry symbol map
	// symbolMap, err := kiteConnect.GetInstrumentExpirySymbolMap(ctx)
	// if err != nil {
	// 	log.Error("Failed to get instrument expiry symbol map", map[string]interface{}{
	// 		"error": err.Error(),
	// 	})
	// 	os.Exit(1)
	// }

	// Get upcoming expiry tokens for NIFTY and SENSEX
	tokens, err := kiteConnect.GetUpcomingExpiryTokens(ctx, []string{"NIFTY", "SENSEX"})
	if err != nil {
		log.Error("Failed to get upcoming expiry tokens", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Info("Got tokens for upcoming NIFTY and SENSEX expiry", map[string]interface{}{
		"tokens_count": len(tokens),
		"tokens":       tokens,
	})

	// Connect ticker
	if err := kiteConnect.ConnectTicker(); err != nil {
		log.Error("Failed to connect ticker", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Ensure ticker is closed on exit
	defer func() {
		log.Info("Closing ticker connections", nil)
		if err := kiteConnect.CloseTicker(); err != nil {
			log.Error("Error closing ticker", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Subscribe to all tokens
	if err := kiteConnect.Subscribe(tokens); err != nil {
		log.Error("Failed to subscribe to tokens", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt signal
	sig := <-sigChan
	log.Info("Received shutdown signal", map[string]interface{}{
		"signal": sig.String(),
	})

	// Cancel context to initiate shutdown
	cancel()

	// Give some time for cleanup
	time.Sleep(time.Second)

	log.Info("Shutdown complete", nil)
}
