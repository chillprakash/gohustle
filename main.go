package main

import (
	"context"
	"fmt"
	"gohustle/api"
	"gohustle/auth"
	"gohustle/cache"
	"gohustle/config"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/nats"
	"gohustle/scheduler"
	"gohustle/zerodha"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
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

	if err := kiteConnect.SyncAllInstrumentDataToCache(ctx); err != nil {
		return fmt.Errorf("failed to sync instrument data to cache: %w", err)
	}

	// Get upcoming expiry tokens for configured indices
	tokens, err := kiteConnect.GetUpcomingExpiryTokensForIndices(ctx, indices.GetAllIndices())
	if err != nil {
		return fmt.Errorf("failed to get upcoming expiry tokens: %w", err)
	}
	logger.L().Info("Upcoming expiry tokens", map[string]interface{}{
		"tokens": len(tokens),
	})

	// Add index tokens for spot indices
	indexTokens := kiteConnect.GetIndexTokens()
	var indexTokenSlice []uint32
	for _, token := range indexTokens {
		indexTokenSlice = append(indexTokenSlice, token)
	}

	// Combine both uint32 token lists for subscription
	allTokens := append(tokens, indexTokenSlice...)

	logger.L().Info("Initializing tickers with tokens", map[string]interface{}{
		"normal_tokens_count": len(tokens),
		"index_tokens_count":  len(indexTokenSlice),
		"total_tokens":        len(allTokens),
	})

	// temp_tokens := []uint32{256265}
	kiteConnect.InitializeTickersWithTokens(allTokens)

	scheduler.InitializePositionPolling(ctx)

	// // Initialize strategy P&L tracking
	scheduler.InitializeStrategyPnLTracking(ctx)

	// scheduler.InitializeIndexOptionChainPolling(ctx)

	// // Initialize tick data archiving (hourly) and consolidation (outside market hours)
	// archive.InitializeTickDataArchiving(ctx)
	// archive.InitializeTickDataConsolidation(ctx)
	// logger.L().Info("Initialized tick data archiving and consolidation", nil)

	// Block until context is cancelled
	<-ctx.Done()
	return nil
}

func startAPIServer(ctx context.Context, cfg *config.Config) error {
	// Skip starting API server if not enabled in config
	if !cfg.API.Enabled {
		logger.L().Info("API server disabled in config", nil)
		return nil
	}

	// Get API server singleton
	apiServer := api.GetAPIServer()

	// Set port from config
	if cfg.API.Port != "" {
		apiServer.SetPort(cfg.API.Port)
	}

	// Start the API server
	if err := apiServer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start API server: %w", err)
	}

	logger.L().Info("API server started", map[string]interface{}{
		"port": cfg.API.Port,
	})

	return nil
}

// initialize performs initial setup for the application
func initialize() error {
	// Write PID file
	pid := os.Getpid()
	if err := os.WriteFile("app.pid", []byte(fmt.Sprintf("%d", pid)), 0644); err != nil {
		return fmt.Errorf("failed to write PID file: %w", err)
	}

	// Get configuration
	cfg := config.GetConfig()
	if cfg == nil {
		return fmt.Errorf("failed to load configuration")
	}

	// Initialize auth system with default credentials if not set in config
	username := cfg.Auth.Username
	if username == "" {
		username = "admin"
	}
	password := cfg.Auth.Password
	if password == "" {
		password = "admin"
	}

	_, err := auth.GetAuthManager(auth.Config{
		Username: username,
		Password: password,
	})
	if err != nil {
		return fmt.Errorf("failed to initialize auth system: %w", err)
	}

	logger.L().Info("Auth system initialized", map[string]interface{}{
		"username": username,
	})

	return nil
}

// cleanup performs cleanup operations before shutdown
func cleanup(ctx context.Context) error {
	// Get API server and shut it down
	apiServer := api.GetAPIServer()
	if err := apiServer.Shutdown(); err != nil {
		return fmt.Errorf("error shutting down API server: %w", err)
	}

	// Close NATS connections
	natsHelper := nats.GetNATSHelper()
	natsHelper.Shutdown()

	// Close Redis connections
	redisCache, err := cache.GetRedisCache()
	if err == nil && redisCache != nil {
		if err := redisCache.Close(); err != nil {
			logger.L().Error("Error closing Redis connections", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}

	// Remove PID file
	if err := os.Remove("app.pid"); err != nil {
		logger.L().Error("Error removing PID file", map[string]interface{}{
			"error": err.Error(),
		})
	}

	return nil
}

func main() {
	log := logger.L()
	log.Info("Starting application", nil)

	// Initialize process
	if err := initialize(); err != nil {
		log.Fatal("Failed to initialize", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Get configuration
	cfg := config.GetConfig()
	if cfg == nil {
		log.Fatal("Failed to load configuration", nil)
	}

	// Initialize Redis cache
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		log.Fatal("Failed to initialize Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer func() {
		log.Info("Closing Redis connections...", nil)
		if err := redisCache.Close(); err != nil {
			log.Error("Error closing Redis connections", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}()

	// Verify Redis connections
	if err := redisCache.Ping(); err != nil {
		log.Fatal("Failed to connect to Redis", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Create root context that cancels on interrupt signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create error channel with buffer for all goroutines
	errChan := make(chan error, 3) // Buffer for data processing, API server, and any other goroutines

	// Start data processing
	go func() {
		log.Info("Starting data processing...", nil)
		if err := startDataProcessing(ctx, cfg); err != nil && !isContextCanceledError(err) {
			errChan <- fmt.Errorf("data processing error: %w", err)
		}
		log.Info("Data processing stopped", nil)
	}()

	// Start API server
	apiServerDone := make(chan struct{})
	go func() {
		log.Info("Starting API server...", nil)
		if err := startAPIServer(ctx, cfg); err != nil && !isContextCanceledError(err) {
			errChan <- fmt.Errorf("API server error: %w", err)
		}
		close(apiServerDone)
		log.Info("API server stopped", nil)
	}()

	// Wait for shutdown signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Info("Shutdown signal received", map[string]interface{}{
			"signal": sig.String(),
		})

		// Start graceful shutdown with timeout
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		// Cancel the main context to signal all goroutines to stop
		cancel()

		// Wait for API server to shut down gracefully
		select {
		case <-apiServerDone:
			log.Info("API server shut down gracefully", nil)
		case <-shutdownCtx.Done():
			log.Error("API server shutdown timed out, forcing exit", map[string]interface{}{
				"timeout": "30s",
			})
		}

		// Perform cleanup
		if err := cleanup(shutdownCtx); err != nil {
			log.Error("Error during cleanup", map[string]interface{}{
				"error": err.Error(),
			})
		}

	case err := <-errChan:
		log.Error("Fatal error occurred", map[string]interface{}{
			"error": err.Error(),
		})
		// Cancel context to initiate graceful shutdown
		cancel()
		// Wait a moment for goroutines to handle the cancellation
		time.Sleep(2 * time.Second)
	}

	log.Info("Application shutdown complete", nil)
}

// isContextCanceledError checks if the error is a context.Canceled error or contains it
func isContextCanceledError(err error) bool {
	if err == context.Canceled {
		return true
	}
	// Check if the error wraps context.Canceled
	return err != nil && err.Error() == context.Canceled.Error()
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
