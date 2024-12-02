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
)

func main() {
	log := logger.GetLogger()
	ctx := context.Background()

	// Load config
	cfg := config.GetConfig()

	// Initialize TimescaleDB
	database := db.InitDB(&cfg.Timescale)
	defer database.Close()

	// Initialize KiteConnect
	kiteConnect := NewKiteConnect(database, &cfg.Kite)

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
			prices, err := kiteConnect.GetCurrentSpotPriceOfAllIndices(ctx)
			if err != nil {
				log.Error("Failed to get spot prices", map[string]interface{}{
					"error": err.Error(),
				})
				continue
			}

			log.Info("Fetched spot prices", map[string]interface{}{
				"prices": prices,
			})

		case sig := <-sigChan:
			log.Info("Received shutdown signal", map[string]interface{}{
				"signal": sig.String(),
			})
			return
		}
	}
}
