package main

import (
	"context"
	"time"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"
)

func main() {
	log := logger.GetLogger()

	// Load config
	cfg, err := config.GetConfig()
	if err != nil {
		log.Error("Failed to load config", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Initialize TimescaleDB for token storage
	database, err := db.NewTimescaleDB(cfg.Timescale)
	if err != nil {
		log.Error("Failed to initialize TimescaleDB", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	defer database.Close()

	// Initialize KiteConnect (now in the same package)
	kite := NewKiteConnect(&cfg.Kite, database)

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Try to get existing token first
	token, err := kite.GetToken(ctx)
	if err != nil {
		log.Info("No valid token found, refreshing...", nil)

		// Refresh token if not found or expired
		token, err = kite.RefreshAccessToken(ctx)
		if err != nil {
			log.Error("Failed to refresh access token", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
	}

	log.Info("Access token operation completed", map[string]interface{}{
		"token_exists": token != "",
		"token_length": len(token),
	})
}
