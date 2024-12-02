package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/zerodha"
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
	kiteConnect := zerodha.NewKiteConnect(database, &cfg.Kite)

	// Fetch the latest valid access token
	token, err := kiteConnect.GetToken(ctx)
	if err != nil {
		log.Error("Failed to get access token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Log the access token
	log.Info("Access Token Retrieved", map[string]interface{}{
		"access_token": token,
	})

	// Set the access token for the KiteConnect client
	kiteConnect.Kite.SetAccessToken(token)

	// Fetch current spot prices for all indices
	spotPrices, err := kiteConnect.GetCurrentSpotPriceOfAllIndices(ctx)
	if err != nil {
		log.Error("Failed to fetch spot prices", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Print the spot prices
	fmt.Println("Current Spot Prices:", spotPrices)

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info("Shutting down gracefully", nil)
}
