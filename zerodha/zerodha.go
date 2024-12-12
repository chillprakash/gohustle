package zerodha

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"gohustle/config"
	"gohustle/logger"
)

// Verify KiteConnect implements KiteConnector at compile time
var _ KiteConnector = (*KiteConnect)(nil)

// You might want to add a helper method to check the status
func (r *LoginResponse) IsSuccess() bool {
	return r.Status == "success"
}

// GetCurrentSpotPriceOfAllIndices fetches current spot prices for all indices
func (k *KiteConnect) GetCurrentSpotPriceOfAllIndices(ctx context.Context) (map[string]float64, error) {
	log := logger.GetLogger()

	// Define exchange trading symbols
	exchangeTradingSymbols := []string{
		"NSE:NIFTY 50",
		"NSE:NIFTY BANK",
		"NSE:NIFTY FIN SERVICE",
		"NSE:NIFTY MID SELECT",
		"BSE:SENSEX",
		"BSE:BANKEX",
	}

	// Fetch quotes for all symbols
	quotes, err := k.Kite.GetQuote(exchangeTradingSymbols...)
	if err != nil {
		log.Error("Failed to fetch spot prices", map[string]interface{}{
			"error":      err.Error(),
			"error_type": reflect.TypeOf(err).String(),
			"symbols":    exchangeTradingSymbols,
		})
		return nil, errors.New("failed to fetch spot prices")
	}

	// Map exchange symbols to index names with their spot prices
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

// SetAccessToken sets the access token for the KiteConnect instance
func (k *KiteConnect) SetAccessToken(token string) {
	k.Kite.SetAccessToken(token)
}

// InstrumentData represents the structure of instrument data
type InstrumentData struct {
	InstrumentToken int     `json:"instrument_token"`
	ExchangeToken   int     `json:"exchange_token"`
	TradingSymbol   string  `json:"tradingsymbol"`
	Name            string  `json:"name"`
	LastPrice       float64 `json:"last_price"`
	Expiry          string  `json:"expiry"`
	Strike          float64 `json:"strike"`
	TickSize        float64 `json:"tick_size"`
	LotSize         int     `json:"lot_size"`
	InstrumentType  string  `json:"instrument_type"`
	Segment         string  `json:"segment"`
	Exchange        string  `json:"exchange"`
}

// Add this helper method
func (k *KiteConnect) getDataPath() string {
	log := logger.GetLogger()
	dataPath := "data"
	config := config.GetConfig()
	if config != nil && config.Kite.DataPath != "" {
		dataPath = config.Kite.DataPath
	}
	// Ensure directory exists
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		log.Error("Failed to create data directory", map[string]interface{}{
			"error": err.Error(),
			"path":  dataPath,
		})
		os.Exit(1)
	}
	return dataPath
}

// Update GetUpcomingExpiries method
func (k *KiteConnect) GetUpcomingExpiries(ctx context.Context) error {
	log := logger.GetLogger()

	// Get data path using helper method
	dataPath := k.getDataPath()

	// Get all CSV files in the data directory
	files, err := filepath.Glob(filepath.Join(dataPath, "*.csv"))
	if err != nil {
		log.Error("Failed to read data directory", map[string]interface{}{
			"error": err.Error(),
			"path":  dataPath,
		})
		return errors.New("failed to read data directory")
	}

	// Process NIFTY and BANKNIFTY files
	for _, symbol := range []string{"NIFTY", "BANKNIFTY"} {
		var symbolFile string
		for _, file := range files {
			if strings.Contains(file, symbol+"_") {
				symbolFile = file
				break
			}
		}

		if symbolFile == "" {
			log.Error("File not found for symbol", map[string]interface{}{
				"symbol": symbol,
			})
			continue
		}

		err := k.processFile(symbolFile)
		if err != nil {
			log.Error("Failed to process file", map[string]interface{}{
				"error":  err.Error(),
				"symbol": symbol,
				"file":   symbolFile,
			})
			continue
		}
	}

	return nil
}

func (k *KiteConnect) processFile(filePath string) error {
	log := logger.GetLogger()
	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Failed to open file", map[string]interface{}{
			"error": err.Error(),
			"path":  filePath,
		})
		return errors.New("failed to open file")
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		log.Error("Failed to read header", map[string]interface{}{
			"error": err.Error(),
			"path":  filePath,
		})
		return errors.New("failed to read header")
	}

	// Read and log each expiry date
	for {
		record, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break // End of file
			}
			log.Error("Failed to read record", map[string]interface{}{
				"error": err.Error(),
				"path":  filePath,
			})
			return errors.New("failed to read record")
		}

		expiryDate := record[5] // Assuming expiry date is in the 6th column
		log.Info("Found expiry date", map[string]interface{}{
			"expiry": expiryDate,
			"file":   filePath,
		})
	}

	return nil
}
