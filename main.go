package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/consumer"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/queue"
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

	// Initialize AsynqQueue
	asynqQueue := queue.InitAsynqQueue(&cfg.Asynq)

	// Initialize KiteConnect
	kiteConnect := zerodha.NewKiteConnect(database, cfg, asynqQueue)
	defer kiteConnect.Close()

	// Download instrument data
	if err := kiteConnect.DownloadInstrumentData(context.Background()); err != nil {
		log.Fatal("Failed to download instrument data", map[string]interface{}{
			"error": err.Error(),
		})
	}

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
	// Convert index tokens map to slice
	var indexTokenSlice []string
	for _, token := range indexTokens {
		indexTokenSlice = append(indexTokenSlice, token)
	}

	// Combine both token lists for subscription
	allTokens := append(tokens, indexTokenSlice...)

	log.Info("Retrieved tokens for subscription", map[string]interface{}{
		"spot_indices":    cfg.Indices.SpotIndices,
		"derived_indices": cfg.Indices.DerivedIndices,
		"expiry_tokens":   tokens,
		"index_tokens":    indexTokenSlice,
		"total_tokens":    len(allTokens),
	})

	// Convert string tokens to uint32 for Subscribe
	tokenInts := make([]uint32, len(allTokens))
	for i, token := range allTokens {
		t, err := strconv.ParseUint(token, 10, 32)
		if err != nil {
			log.Error("Failed to parse token", map[string]interface{}{
				"error": err.Error(),
				"token": token,
			})
			return
		}
		tokenInts[i] = uint32(t)
	}

	// Connect ticker first and ensure it's ready
	if err := kiteConnect.ConnectTicker(); err != nil {
		log.Error("Failed to connect ticker", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Add a small delay to ensure connection is established
	time.Sleep(time.Second)

	// Then subscribe to all tokens
	if err := kiteConnect.Subscribe(allTokens); err != nil {
		log.Error("Failed to subscribe to tokens", map[string]interface{}{
			"error": err.Error(),
		})
		kiteConnect.CloseTicker()
		return
	}

	// Start consumer in a goroutine
	go consumer.StartTickConsumer(cfg, kiteConnect)
	go consumer.StartTimescaleConsumer(cfg, kiteConnect, database)

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
