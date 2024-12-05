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

	// Get upcoming expiry tokens for NIFTY and SENSEX
	tokens, err := kiteConnect.GetUpcomingExpiryTokens(ctx, []string{"NIFTY", "SENSEX"})
	if err != nil {
		log.Error("Failed to get upcoming expiry tokens", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Info("Got tokens for upcoming NIFTY and SENSEX expiry", map[string]interface{}{
		"tokens_count": len(tokens),
		"tokens":       tokens,
	})

	instruments := []string{"NIFTY", "SENSEX"}
	now := time.Now().Truncate(24 * time.Hour)

	for _, instrument := range instruments {
		var upcomingExpiry time.Time

		// Find upcoming expiry for each instrument
		for expiry := range symbolMap.Data[instrument] {
			normalizedExpiry := expiry.Truncate(24 * time.Hour)
			if normalizedExpiry.After(now) || normalizedExpiry.Equal(now) {
				if upcomingExpiry.IsZero() || normalizedExpiry.Before(upcomingExpiry) {
					upcomingExpiry = expiry
				}
			}
		}

		// Log options for upcoming expiry
		if !upcomingExpiry.IsZero() {
			options := symbolMap.Data[instrument][upcomingExpiry]

			log.Info(fmt.Sprintf("%s CALLS", instrument), map[string]interface{}{
				"expiry":        upcomingExpiry.Format("2006-01-02"),
				"symbols_count": len(options.Calls),
				"symbols":       formatOptionTokenPairs(options.Calls),
			})

			log.Info(fmt.Sprintf("%s PUTS", instrument), map[string]interface{}{
				"expiry":        upcomingExpiry.Format("2006-01-02"),
				"symbols_count": len(options.Puts),
				"symbols":       formatOptionTokenPairs(options.Puts),
			})
		}
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
