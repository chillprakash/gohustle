package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/zerodha"
)

func startDataProcessing(ctx context.Context, cfg *config.Config) error {
	// Initialize KiteConnect with writer pool
	kiteConnect := zerodha.GetKiteConnect()
	if kiteConnect == nil {
		return fmt.Errorf("failed to initialize KiteConnect")
	}

	indices := core.GetIndices()
	intradayIndices := indices.GetIndicesToSubscribeForIntraday()

	// Download instrument data
	if err := kiteConnect.DownloadInstrumentData(ctx, intradayIndices); err != nil {
		return fmt.Errorf("failed to download instrument data: %w", err)
	}

	// Sync instrument expiries from file to cache
	kiteConnect.SyncInstrumentExpiriesFromFileToCache(ctx)

	// Create lookup map with expiry vs token map
	kiteConnect.CreateLookUpforStoringFileFromWebsockets(ctx)

	kiteConnect.CreateLookUpOfExpiryVsAllDetailsInSingleString(ctx, indices.GetIndicesToSubscribeForIntraday())

	// Get upcoming expiry tokens for configured indices
	tokens, err := kiteConnect.GetUpcomingExpiryTokensForIndices(ctx, indices.GetIndicesToSubscribeForIntraday())
	if err != nil {
		return fmt.Errorf("failed to get upcoming expiry tokens: %w", err)
	}
	logger.L().Info("Upcoming expiry tokens", map[string]interface{}{
		"tokens": len(tokens),
	})

	// Get upcoming expiry tokens for configured indices
	// tokens, err := kiteConnect.GetUpcomingExpiryTokensForIndices(ctx, indices.GetIndicesToSubscribeForIntraday())
	// if err != nil {
	// 	return fmt.Errorf("failed to get upcoming expiry tokens: %w", err)
	// }

	// Add index tokens for spot indices
	indexTokens := kiteConnect.GetIndexTokens()
	var indexTokenSlice []string
	for _, token := range indexTokens {
		indexTokenSlice = append(indexTokenSlice, token)
	}

	// Convert both token slices to uint32
	normalTokens, err := convertTokensToUint32(tokens)
	if err != nil {
		return fmt.Errorf("failed to convert normal tokens: %w", err)
	}

	indexTokensUint32, err := convertTokensToUint32(indexTokenSlice)
	if err != nil {
		return fmt.Errorf("failed to convert index tokens: %w", err)
	}

	// Combine both uint32 token lists for subscription
	allTokens := append(normalTokens, indexTokensUint32...)

	logger.L().Info("Initializing tickers with tokens", map[string]interface{}{
		"normal_tokens_count": len(normalTokens),
		"index_tokens_count":  len(indexTokensUint32),
		"total_tokens":        len(allTokens),
	})

	kiteConnect.InitializeTickersWithTokens(allTokens)

	// // Initialize and start scheduler
	// scheduler := scheduler.NewScheduler(
	// 	ctx,
	// 	"data/exports",
	// 	&cfg.Telegram,
	// )
	// scheduler.Start()

	// Block until context is cancelled
	<-ctx.Done()
	return nil
}

func initializeProcess() (context.Context, context.CancelFunc, chan os.Signal, error) {
	// No need to pass logger anymore
	pid := os.Getpid()
	if err := os.WriteFile("app.pid", []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to write PID file: %w", err)
	}

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel, sigChan, nil
}

func main() {
	logger.L().Info("Application starting...", nil)

	ctx, cancel, sigChan, err := initializeProcess()
	if err != nil {
		logger.L().Fatal("Failed to initialize process", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer cancel()
	defer os.Remove("app.pid")

	cfg := config.GetConfig()

	// Start data processing in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := startDataProcessing(ctx, cfg); err != nil {
			errChan <- err
		}
	}()

	// Wait for either error or shutdown signal
	select {
	case err := <-errChan:
		logger.L().Error("Data processing error", map[string]interface{}{
			"error": err.Error(),
		})
	case sig := <-sigChan:
		logger.L().Info("Received shutdown signal", map[string]interface{}{
			"signal": sig.String(),
		})
	}

	// Start graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Create a channel to track shutdown completion
	shutdownComplete := make(chan struct{})

	go func() {
		// Cancel the main context
		cancel()
		close(shutdownComplete)
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-shutdownComplete:
		logger.L().Info("Clean shutdown successful", nil)
	case <-shutdownCtx.Done():
		logger.L().Error("Shutdown timed out, forcing exit", nil)
	}

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
