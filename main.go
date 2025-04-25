package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gohustle/api"
	"gohustle/auth"
	"gohustle/cache"
	"gohustle/config"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/nats"
	"gohustle/scheduler"
	"gohustle/zerodha"
)

func startDataProcessing(ctx context.Context, cfg *config.Config) error {
	// Initialize NATS
	natsConsumer, err := nats.GetTickConsumer(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize NATS: %w", err)
	}

	// Log connection status using the NATS helper
	natsHelper := nats.GetNATSHelper()
	logger.L().Info("NATS connection status", map[string]interface{}{
		"connected": natsHelper.IsConnected(),
	})

	// Start consumer with wildcard subject pattern
	if err := natsConsumer.Start(ctx, "ticks.>", "tick_consumer"); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

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
	kiteConnect.CreateLookUpforStoringFileFromWebsocketsAndAlsoStrikes(ctx)

	kiteConnect.CreateLookUpOfExpiryVsAllDetailsInSingleString(ctx, indices.GetIndicesToSubscribeForIntraday())

	// Get upcoming expiry tokens for configured indices
	tokens, err := kiteConnect.GetUpcomingExpiryTokensForIndices(ctx, indices.GetIndicesToSubscribeForIntraday())
	if err != nil {
		return fmt.Errorf("failed to get upcoming expiry tokens: %w", err)
	}
	logger.L().Info("Upcoming expiry tokens", map[string]interface{}{
		"tokens": len(tokens),
	})

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

	// temp_tokens := []uint32{256265}
	kiteConnect.InitializeTickersWithTokens(allTokens)

	scheduler.InitializePositionPolling(ctx)

	scheduler.InitializeIndexOptionChainPolling(ctx)

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

	if err := auth.Initialize(auth.Config{
		Username: username,
		Password: password,
	}); err != nil {
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

	// Verify Redis connections
	if err := redisCache.Ping(); err != nil {
		log.Fatal("Failed to connect to Redis", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Create error and done channels
	errChan := make(chan error, 2)
	done := make(chan bool)

	// Create root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start data processing
	go func() {
		if err := startDataProcessing(ctx, cfg); err != nil {
			errChan <- fmt.Errorf("failed to start data processing: %w", err)
		}
	}()

	// Start API server
	go func() {
		if err := startAPIServer(ctx, cfg); err != nil {
			errChan <- fmt.Errorf("failed to start API server: %w", err)
		}
	}()

	// Handle shutdown
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig

		log.Info("Shutdown signal received", nil)

		// Create context with timeout for graceful shutdown
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		// Cancel the main context to initiate shutdown
		cancel()

		// Cleanup and shutdown
		if err := cleanup(shutdownCtx); err != nil {
			log.Error("Error during cleanup", map[string]interface{}{
				"error": err.Error(),
			})
		}

		close(done)
	}()

	// Wait for error or done signal
	select {
	case err := <-errChan:
		log.Fatal("Application error", map[string]interface{}{
			"error": err.Error(),
		})
	case <-done:
		log.Info("Application shutdown complete", nil)
	}
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
