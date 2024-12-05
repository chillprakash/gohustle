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
	kiteConnect := zerodha.NewKiteConnect(database, &cfg.Kite)

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

	// Get instrument expiry symbol map
	symbolMap, err := kiteConnect.GetInstrumentExpirySymbolMap(ctx)
	if err != nil {
		log.Error("Failed to get instrument expiry symbol map", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Find upcoming expiry (normalize dates to start of day)
	now := time.Now().Truncate(24 * time.Hour)
	var upcomingExpiry time.Time

	for expiry := range symbolMap.Data["NIFTY"] {
		// Normalize expiry to start of day
		normalizedExpiry := expiry.Truncate(24 * time.Hour)
		if normalizedExpiry.After(now) || normalizedExpiry.Equal(now) {
			if upcomingExpiry.IsZero() || normalizedExpiry.Before(upcomingExpiry) {
				upcomingExpiry = expiry
			}
		}
	}

	// Log NIFTY options for upcoming expiry
	if !upcomingExpiry.IsZero() {
		niftyOptions := symbolMap.Data["NIFTY"][upcomingExpiry]

		log.Info("NIFTY CALLS", map[string]interface{}{
			"expiry":        upcomingExpiry.Format("2006-01-02"),
			"symbols_count": len(niftyOptions.Calls),
			"symbols":       formatOptionTokenPairs(niftyOptions.Calls),
		})

		log.Info("NIFTY PUTS", map[string]interface{}{
			"expiry":        upcomingExpiry.Format("2006-01-02"),
			"symbols_count": len(niftyOptions.Puts),
			"symbols":       formatOptionTokenPairs(niftyOptions.Puts),
		})
	}

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	log.Info("Shutting down gracefully", nil)
}

func formatOptionTokenPairs(pairs []zerodha.OptionTokenPair) []string {
	formatted := make([]string, len(pairs))
	for i, pair := range pairs {
		formatted[i] = fmt.Sprintf("%s(%s)", pair.Symbol, pair.InstrumentToken)
	}
	return formatted
}
