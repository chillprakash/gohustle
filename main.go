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

	// Get upcoming expiry tokens
	tokens, err := kiteConnect.GetUpcomingExpiryTokens(ctx, []string{"NIFTY", "SENSEX"})
	if err != nil {
		log.Error("Failed to get upcoming expiry tokens", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Add index tokens
	indexTokens := kiteConnect.GetIndexTokens()
	for _, token := range indexTokens {
		tokens = append(tokens, token)
	}

	log.Info("Combined tokens", map[string]interface{}{
		"total_tokens": len(tokens),
		"index_tokens": len(indexTokens),
	})

	// Convert string tokens to uint32
	tokenInts := make([]uint32, len(tokens))
	for i, token := range tokens {
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

	// Convert uint32 tokens back to strings for Subscribe
	tokenStrs := make([]string, len(tokenInts))
	for i, t := range tokenInts {
		tokenStrs[i] = strconv.FormatUint(uint64(t), 10)
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

	// Then subscribe
	if err := kiteConnect.Subscribe(tokenStrs); err != nil {
		log.Error("Failed to subscribe to tokens", map[string]interface{}{
			"error": err.Error(),
		})
		kiteConnect.CloseTicker() // Close before returning on error
		return
	}

	// Start consumer in a goroutine
	go consumer.StartTickConsumer(cfg, kiteConnect)

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
