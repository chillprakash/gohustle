package main

import (
	"context"
	"fmt"
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

	// Write PID to file at the start
	pid := os.Getpid()
	if err := os.WriteFile("app.pid", []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		log.Fatal("Failed to write PID file", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer os.Remove("app.pid")

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize components
	kiteConnect := zerodha.NewKiteConnect(true)
	if kiteConnect == nil {
		log.Fatal("Failed to initialize KiteConnect", nil)
		return
	}

	cfg := config.GetConfig()

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

	// // Initialize and start scheduler
	// sched := scheduler.NewScheduler()
	// sched.Start()

	// // Initialize writer pool early
	// writerPool := zerodha.NewWriterPool()
	// writerPool.Start()
	// defer writerPool.Stop()

	// // Initialize consumer with writer pool
	// consumer := cache.NewConsumer(writerPool)
	// consumer.Start()

	// Block until we receive a signal
	sig := <-sigChan
	log.Info("Received shutdown signal", map[string]interface{}{
		"signal": sig.String(),
	})

	// Start graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Create a channel to track shutdown completion
	shutdownComplete := make(chan struct{})

	go func() {
		// Cancel the main context
		cancel()

		// Stop components in reverse order
		log.Info("Stopping consumer...", nil)
		consumer.Stop()

		log.Info("Closing KiteConnect...", nil)
		if kiteConnect != nil {
			kiteConnect.Close()
		}

		close(shutdownComplete)
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-shutdownComplete:
		log.Info("Clean shutdown successful", nil)
	case <-shutdownCtx.Done():
		log.Error("Shutdown timed out, forcing exit", nil)
	}

	// Force exit after logging
	os.Exit(0)
}
