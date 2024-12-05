package main

import (
	"context"
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

	// Initialize KiteConnect with interface
	var kiteConnect zerodha.KiteConnector
	kiteConnect = zerodha.NewKiteConnect(database, &cfg.Kite)

	// Download instrument data
	var err error
	err = kiteConnect.DownloadInstrumentData(ctx)
	if err != nil {
		log.Error("Failed to download instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	err = kiteConnect.SyncInstrumentExpiriesFromFileToDB(ctx)
	if err != nil {
		// handle error
	}

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info("Shutting down gracefully", nil)
}
