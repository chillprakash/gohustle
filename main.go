package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/zerodha"
)

func main() {
	log := logger.GetLogger()
	log.Info("Application starting...", nil)
	cfg := config.GetConfig()

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	kiteConnect := zerodha.NewKiteConnect(true)
	if kiteConnect == nil {
		log.Fatal("Failed to initialize KiteConnect", nil)
		return
	}

	defer func() {
		if kiteConnect != nil {
			kiteConnect.Close()
		}
	}()

	// Download instrument data
	if err := kiteConnect.DownloadInstrumentData(context.Background()); err != nil {
		log.Fatal("Failed to download instrument data", map[string]interface{}{
			"error": err.Error(),
		})
	}

	kiteConnect.SyncInstrumentExpiriesFromFileToCache(ctx)

	// Get upcoming expiry tokens for configured indices
	tokens, err := kiteConnect.GetUpcomingExpiryTokens(ctx, cfg.Indices.DerivedIndices)
	if err != nil {
		log.Error("Failed to get upcoming expiry tokens", map[string]interface{}{
			"error":   err.Error(),
			"indices": cfg.Indices.DerivedIndices,
		})
		return
	}

	// Add index tokens for spot indices
	indexTokens := kiteConnect.GetIndexTokens()
	var indexTokenSlice []string
	for _, token := range indexTokens {
		indexTokenSlice = append(indexTokenSlice, token)
	}

	// Combine both token lists for subscription
	allTokens := append(tokens, indexTokenSlice...)

	// Wait a bit for tickers to be fully connected
	time.Sleep(2 * time.Second)

	// Now subscribe to tokens
	if err := kiteConnect.Subscribe(allTokens); err != nil {
		log.Error("Failed to subscribe to tokens", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Info("Successfully subscribed to tokens", map[string]interface{}{
		"total_tokens": len(allTokens),
	})

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Graceful shutdown
	log.Info("Initiating graceful shutdown", nil)
	kiteConnect.Close()
	log.Info("Shutdown complete", nil)
}
