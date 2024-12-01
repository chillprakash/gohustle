package main

import (
	"context"
	"fmt"

	"gohustle/config"
	"gohustle/db"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type KiteData struct {
	kite   *kiteconnect.Client
	db     *db.TimescaleDB
	config *config.KiteConfig
}

// getStoredAccessToken retrieves the active access token from the database
func getStoredAccessToken(ctx context.Context, db *db.TimescaleDB) (string, error) {
	log := logger.GetLogger()

	query := `
        SELECT access_token 
        FROM access_tokens 
        WHERE token_type = 'kite' 
        AND is_active = TRUE 
        AND expires_at > NOW() 
        ORDER BY created_at DESC 
        LIMIT 1
    `

	var accessToken string
	err := db.QueryRow(ctx, query).Scan(&accessToken)
	if err != nil {
		log.Error("Failed to get access token", map[string]interface{}{
			"error": err.Error(),
		})
		return "", fmt.Errorf("failed to get access token: %w", err)
	}

	log.Info("Retrieved access token", map[string]interface{}{
		"token_length": len(accessToken),
	})

	return accessToken, nil
}

// NewKiteData initializes a new KiteData instance
func NewKiteData(db *db.TimescaleDB, config *config.KiteConfig) (*KiteData, error) {
	log := logger.GetLogger()

	// Get stored access token
	accessToken, err := getStoredAccessToken(context.Background(), db)
	if err != nil {
		return nil, err
	}

	// Initialize KiteConnect client
	kite := kiteconnect.New(config.APIKey)
	kite.SetAccessToken(accessToken)

	log.Info("Successfully initialized Kite client", map[string]interface{}{
		"api_key":             config.APIKey,
		"access_token_length": len(accessToken),
	})

	return &KiteData{
		kite:   kite,
		db:     db,
		config: config,
	}, nil
}

func (k *KiteData) GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error) {
	log := logger.GetLogger()

	exchangeTradingSymbols := []string{
		"NSE:NIFTY 50",
		"NSE:NIFTY BANK",
		"NSE:NIFTY FIN SERVICE",
		"NSE:NIFTY MID SELECT",
		"BSE:SENSEX",
		"BSE:BANKEX",
	}

	quotes, err := k.kite.GetQuote(exchangeTradingSymbols...)
	if err != nil {
		log.Error("Failed to fetch spot prices", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to fetch spot prices: %w", err)
	}

	indexVsSpotPrice := map[string]float64{
		"NIFTY":      quotes["NSE:NIFTY 50"].LastPrice,
		"BANKNIFTY":  quotes["NSE:NIFTY BANK"].LastPrice,
		"FINNIFTY":   quotes["NSE:NIFTY FIN SERVICE"].LastPrice,
		"MIDCPNIFTY": quotes["NSE:NIFTY MID SELECT"].LastPrice,
		"SENSEX":     quotes["BSE:SENSEX"].LastPrice,
		"BANKEX":     quotes["BSE:BANKEX"].LastPrice,
	}

	log.Info("Successfully fetched spot prices", map[string]interface{}{
		"prices": indexVsSpotPrice,
	})

	return indexVsSpotPrice, nil
}

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

	// Initialize TimescaleDB
	database, err := db.NewTimescaleDB(cfg.Timescale)
	if err != nil {
		log.Error("Failed to initialize TimescaleDB", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	defer database.Close()

	// Initialize KiteData
	kiteData, err := NewKiteData(database, &cfg.Kite)
	if err != nil {
		log.Error("Failed to initialize KiteData", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Get spot prices
	prices, err := kiteData.GetCurrentSpotPriceOfAllIndices(context.Background())
	if err != nil {
		log.Error("Failed to get spot prices", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	log.Info("Final spot prices", map[string]interface{}{
		"prices": prices,
	})
}
