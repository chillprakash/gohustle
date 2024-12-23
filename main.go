package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gohustle/cache"
	"gohustle/config"
	"gohustle/logger"
	"gohustle/scheduler"
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

	// Initialize KiteConnect with writer pool
	kiteConnect := zerodha.GetKiteConnect()
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
	kiteConnect.CreateLookupMapWithExpiryVSTokenMap(ctx)

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

	// Convert both token slices to uint32
	normalTokens, err := convertTokensToUint32(tokens)
	if err != nil {
		log.Error("Failed to convert normal tokens", map[string]interface{}{
			"error":  err.Error(),
			"tokens": tokens,
		})
		return
	}

	indexTokensUint32, err := convertTokensToUint32(indexTokenSlice)
	if err != nil {
		log.Error("Failed to convert index tokens", map[string]interface{}{
			"error":  err.Error(),
			"tokens": indexTokenSlice,
		})
		return
	}

	// Combine both uint32 token lists for subscription
	allTokens := append(normalTokens, indexTokensUint32...)

	log.Info("Initializing tickers with tokens", map[string]interface{}{
		"normal_tokens_count": len(normalTokens),
		"index_tokens_count":  len(indexTokensUint32),
		"total_tokens":        len(allTokens),
	})

	kiteConnect.InitializeTickersWithTokens(allTokens)

	// Initialize and start scheduler
	scheduler := scheduler.NewScheduler(
		ctx,
		"data/exports",
		&cfg.Telegram,
	)
	scheduler.Start()

	// Initialize consumer with writer pool
	consumer := cache.NewConsumer()
	consumer.Start()

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
		// consumer.Stop()

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

// Helper function to convert string tokens to uint32
func convertTokensToUint32(tokens []string) ([]uint32, error) {
	result := make([]uint32, 0, len(tokens))

	for _, token := range tokens {
		val, err := strconv.ParseUint(token, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid token %s: %w", token, err)
		}
		result = append(result, uint32(val))
	}

	return result, nil
}
