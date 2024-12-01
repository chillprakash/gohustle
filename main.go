package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
)

func main() {
	log := logger.GetLogger()
	ctx := context.Background()

	// Load config
	cfg, err := config.GetConfig()
	if err != nil {
		log.Error("Failed to load config", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Initialize TimescaleDB
	database, err := db.NewTimescaleDB(cfg.Timescale)
	if err != nil {
		log.Error("Failed to initialize TimescaleDB", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}
	defer database.Close()

	// Initialize KiteConnect
	kiteConnect, err := NewKiteConnect(database, &cfg.Kite)
	if err != nil {
		log.Error("Failed to initialize KiteConnect", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Refresh access token
	_, err = kiteConnect.RefreshAccessToken(ctx)
	if err != nil {
		log.Error("Failed to refresh access token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Initialize KiteData
	kiteData, err := NewKiteData(database, &cfg.Kite)
	if err != nil {
		log.Error("Failed to initialize KiteData", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create ticker for periodic updates
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	log.Info("Starting market data collection")

	for {
		select {
		case <-ticker.C:
			// Get spot prices
			prices, err := kiteData.GetCurrentSpotPriceOfAllIndices(ctx)
			if err != nil {
				log.Error("Failed to get spot prices", map[string]interface{}{
					"error": err.Error(),
				})
				continue
			}

			log.Info("Fetched spot prices", map[string]interface{}{
				"prices": prices,
			})

			// Add more periodic tasks here

		case sig := <-sigChan:
			log.Info("Received shutdown signal", map[string]interface{}{
				"signal": sig.String(),
			})
			return
		}
	}
}

// Optional: Add helper function for initialization
func initializeServices(cfg *config.Config) (*db.TimescaleDB, *KiteConnect, *KiteData, error) {
	// Initialize database
	database, err := db.NewTimescaleDB(cfg.Timescale)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Initialize KiteConnect
	kiteConnect, err := NewKiteConnect(database, &cfg.Kite)
	if err != nil {
		database.Close()
		return nil, nil, nil, fmt.Errorf("failed to initialize KiteConnect: %w", err)
	}

	// Initialize KiteData
	kiteData, err := NewKiteData(database, &cfg.Kite)
	if err != nil {
		database.Close()
		return nil, nil, nil, fmt.Errorf("failed to initialize KiteData: %w", err)
	}

	return database, kiteConnect, kiteData, nil
}
