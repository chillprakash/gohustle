package zerodha

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"gohustle/backend/cache"
	"gohustle/backend/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// MarketDataManager handles all market data operations including LTP retrieval and storage
type MarketDataManager struct {
	log *logger.Logger
}

var (
	marketDataInstance *MarketDataManager
	marketDataOnce     sync.Once
)

// GetMarketDataManager returns a singleton instance of MarketDataManager
func GetMarketDataManager() *MarketDataManager {
	marketDataOnce.Do(func() {
		marketDataInstance = &MarketDataManager{
			log: logger.L(),
		}
		marketDataInstance.log.Info("Market data manager initialized", nil)
	})
	return marketDataInstance
}

// GetLTP fetches the Last Traded Price for an instrument token from Redis
// Returns the LTP as a float64 and a boolean indicating if the LTP was found
func (m *MarketDataManager) GetLTP(ctx context.Context, instrumentToken interface{}) (float64, bool) {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		m.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return 0, false
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		m.log.Error("LTP Redis DB is nil", nil)
		return 0, false
	}

	// Construct the LTP key based on the type of instrumentToken
	var ltpKey string
	switch v := instrumentToken.(type) {
	case string:
		ltpKey = fmt.Sprintf("%s_ltp", v)
	case int, int64, float64:
		ltpKey = fmt.Sprintf("%v_ltp", v)
	default:
		ltpKey = fmt.Sprintf("%v_ltp", v)
	}

	// Try to get the LTP from Redis
	ltpStr, err := ltpDB.Get(ctx, ltpKey).Result()
	if err == nil {
		ltp, err := strconv.ParseFloat(ltpStr, 64)
		if err == nil && ltp > 0 {
			m.log.Debug("Found LTP in Redis", map[string]interface{}{
				"ltp":     ltp,
				"ltp_key": ltpKey,
			})
			return ltp, true
		}
	}

	// If not found with _ltp suffix, try without suffix
	ltpKey = fmt.Sprintf("%v", instrumentToken)
	ltpStr, err = ltpDB.Get(ctx, ltpKey).Result()
	if err == nil {
		ltp, err := strconv.ParseFloat(ltpStr, 64)
		if err == nil && ltp > 0 {
			m.log.Debug("Found LTP in Redis without suffix", map[string]interface{}{
				"ltp":     ltp,
				"ltp_key": ltpKey,
			})
			return ltp, true
		}
	}

	m.log.Debug("LTP not found in Redis", map[string]interface{}{
		"instrument_token": instrumentToken,
	})
	return 0, false
}

// GetPremium fetches the premium (LTP) for an option instrument token
// This is a convenience wrapper around GetLTP with a more descriptive name
func (m *MarketDataManager) GetPremium(ctx context.Context, instrumentToken interface{}) (float64, bool) {
	return m.GetLTP(ctx, instrumentToken)
}

// StoreLTP stores the Last Traded Price for an instrument token in Redis
func (m *MarketDataManager) StoreLTP(ctx context.Context, instrumentToken interface{}, ltp float64) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		m.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		m.log.Error("LTP Redis DB is nil", nil)
		return fmt.Errorf("LTP Redis DB is nil")
	}

	// Construct the LTP key
	ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
	ltpStr := fmt.Sprintf("%f", ltp)

	// Store in Redis with a 24-hour expiry
	err = ltpDB.Set(ctx, ltpKey, ltpStr, 24*time.Hour).Err()
	if err != nil {
		m.log.Error("Failed to store LTP in Redis", map[string]interface{}{
			"error":            err.Error(),
			"instrument_token": instrumentToken,
			"ltp":              ltp,
		})
		return err
	}

	m.log.Debug("Stored LTP in Redis", map[string]interface{}{
		"instrument_token": instrumentToken,
		"ltp":              ltp,
		"ltp_key":          ltpKey,
	})
	return nil
}

// For backward compatibility with existing code

// GetLTP is a convenience function that uses the singleton MarketDataManager
func GetLTP(ctx context.Context, instrumentToken interface{}) (float64, bool) {
	return GetMarketDataManager().GetLTP(ctx, instrumentToken)
}

// GetPremium is a convenience function that uses the singleton MarketDataManager
func GetPremium(ctx context.Context, instrumentToken interface{}) (float64, bool) {
	return GetMarketDataManager().GetPremium(ctx, instrumentToken)
}

// StoreLTP is a convenience function that uses the singleton MarketDataManager
func StoreLTP(ctx context.Context, instrumentToken interface{}, ltp float64) error {
	return GetMarketDataManager().StoreLTP(ctx, instrumentToken, ltp)
}

// GetInstrumentType retrieves the instrument type (CE/PE) for a given instrument token
// Returns the instrument type and a boolean indicating if it was found
func (m *MarketDataManager) GetInstrumentType(ctx context.Context, instrumentToken interface{}) (string, bool) {
	if instrumentToken == nil {
		m.log.Error("Empty instrument token provided", nil)
		return "", false
	}

	// Get the in-memory cache instance
	inMemoryCache := cache.GetInMemoryCacheInstance()
	if inMemoryCache == nil {
		m.log.Error("Failed to get in-memory cache instance", nil)
		return "", false
	}

	// Construct the key for instrument type
	key := fmt.Sprintf("instrument_type:%s", instrumentToken)

	// Try to get the instrument type from the cache
	instrumentType, found := inMemoryCache.Get(key)
	if found {
		m.log.Debug("Found instrument type in cache", map[string]interface{}{
			"instrument_token": instrumentToken,
			"instrument_type":  instrumentType,
		})
		return fmt.Sprintf("%v", instrumentType), true
	}

	m.log.Debug("Instrument type not found for instrument token", map[string]interface{}{
		"instrument_token": instrumentToken,
	})
	return "", false
}

// GetInstrumentType is a convenience function that uses the singleton MarketDataManager
func GetInstrumentType(ctx context.Context, instrumentToken interface{}) (string, bool) {
	return GetMarketDataManager().GetInstrumentType(ctx, instrumentToken)
}

// GetExpiry retrieves the expiry date for a given instrument token
// Returns the expiry date and a boolean indicating if it was found
func (m *MarketDataManager) GetExpiry(ctx context.Context, instrumentToken interface{}) (string, bool) {
	if instrumentToken == nil {
		m.log.Error("Empty instrument token provided", nil)
		return "", false
	}

	// Get the in-memory cache instance
	inMemoryCache := cache.GetInMemoryCacheInstance()
	if inMemoryCache == nil {
		m.log.Error("Failed to get in-memory cache instance", nil)
		return "", false
	}

	// Construct the key for expiry date
	key := fmt.Sprintf("expiry:%v", instrumentToken)

	// Try to get the expiry date from the cache
	expiry, found := inMemoryCache.Get(key)
	if found {
		m.log.Debug("Found expiry date in cache", map[string]interface{}{
			"instrument_token": instrumentToken,
			"expiry":           expiry,
		})
		return fmt.Sprintf("%v", expiry), true
	}

	m.log.Debug("Expiry date not found for instrument token", map[string]interface{}{
		"instrument_token": instrumentToken,
	})
	return "", false
}

// GetExpiry is a convenience function that uses the singleton MarketDataManager
func GetExpiry(ctx context.Context, instrumentToken interface{}) (string, bool) {
	return GetMarketDataManager().GetExpiry(ctx, instrumentToken)
}

// GetQuoteForSymbols retrieves market data quotes for a list of trading symbols
// This is a wrapper around the Kite.GetQuote method with proper error handling and logging
func (m *MarketDataManager) GetQuoteForSymbols(ctx context.Context, tradingSymbols []string) (kiteconnect.Quote, error) {
	// Get the Kite instance
	kiteInstance := GetKiteConnect()
	if kiteInstance == nil || kiteInstance.Kite == nil {
		return nil, fmt.Errorf("kite client not initialized")
	}

	// Check if we have any symbols to process
	if len(tradingSymbols) == 0 {
		return nil, fmt.Errorf("no trading symbols provided")
	}

	// Check if context is cancelled before making API call
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with API call
	}
	// Transform trading symbols to match the expected format for the Kite API
	// The API expects single digit month (5) instead of two digits (05)
	transformedSymbols := make([]string, len(tradingSymbols))
	for i, symbol := range tradingSymbols {
		// Check if this is an option symbol with the date format
		if (strings.Contains(symbol, "NFO:NIFTY") || strings.Contains(symbol, "BFO:SENSEX")) && len(symbol) > 15 {
			// Extract parts: exchange:index + year + month + day + strike + type
			parts := strings.SplitN(symbol, ":", 2)
			if len(parts) == 2 {
				exchange := parts[0] + ":"
				remaining := parts[1]

				// Find where the date starts (after NIFTY or SENSEX)
				var index string
				var dateStart int
				if strings.HasPrefix(remaining, "NIFTY") {
					index = "NIFTY"
					dateStart = 5
				} else if strings.HasPrefix(remaining, "SENSEX") {
					index = "SENSEX"
					dateStart = 6
				}

				if dateStart > 0 && len(remaining) > dateStart+6 {
					// Extract date parts
					year := remaining[dateStart : dateStart+2]    // 25
					month := remaining[dateStart+2 : dateStart+4] // 05
					day := remaining[dateStart+4 : dateStart+6]   // 15

					// Convert month to single digit if it starts with 0
					if month[0] == '0' {
						month = string(month[1])
					}

					// Get the rest (strike + option type)
					rest := remaining[dateStart+6:]

					// Reconstruct the symbol
					transformedSymbols[i] = exchange + index + year + month + day + rest
				} else {
					// If we can't parse it, keep the original
					transformedSymbols[i] = symbol
				}
			} else {
				// If we can't split it, keep the original
				transformedSymbols[i] = symbol
			}
		} else {
			// Not an option symbol, keep as is
			transformedSymbols[i] = symbol
		}
	}

	m.log.Info("Requesting quotes from Kite API", map[string]interface{}{
		"symbols_count":       len(tradingSymbols),
		"original_symbols":    strings.Join(tradingSymbols[:min(5, len(tradingSymbols))], ", "),
		"transformed_symbols": strings.Join(transformedSymbols[:min(5, len(transformedSymbols))], ", "),
	})
	// Make the API call with transformed symbols
	quotes, err := kiteInstance.Kite.GetQuote(transformedSymbols...)
	if err != nil {
		m.log.Error("Failed to get quotes from Kite API", map[string]interface{}{
			"error":      err.Error(),
			"error_type": fmt.Sprintf("%T", err),
		})
		return nil, err
	}

	// Log the response details with more information
	var sampleQuoteKeys []string
	if len(quotes) > 0 {
		// Get a sample of quote keys
		i := 0
		for k := range quotes {
			sampleQuoteKeys = append(sampleQuoteKeys, k)
			i++
			if i >= 5 {
				break
			}
		}
	}

	m.log.Info("Quote response details", map[string]interface{}{
		"symbols_requested": len(tradingSymbols),
		"total_quotes":      len(quotes),
		"response_empty":    len(quotes) == 0,
		"sample_quote_keys": sampleQuoteKeys,
	})

	// Log sample quotes for verification if available
	if len(quotes) > 0 {
		sampleCount := 0
		for symbol, quoteData := range quotes {
			if sampleCount < 5 {
				m.log.Debug("Sample quote", map[string]interface{}{
					"symbol":           symbol,
					"last_price":       quoteData.LastPrice,
					"open_interest":    quoteData.OI,
					"volume":           quoteData.Volume,
					"instrument_token": quoteData.InstrumentToken,
				})
				sampleCount++
			}
		}
	} else {
		// If no quotes were returned, check if we're requesting SENSEX options
		// Our tests show that SENSEX options aren't available via the API
		if len(quotes) == 0 {
			hasSensexOptions := false
			for _, symbol := range tradingSymbols {
				if strings.Contains(symbol, "SENSEX") &&
					(strings.HasSuffix(symbol, "CE") || strings.HasSuffix(symbol, "PE")) {
					hasSensexOptions = true
					break
				}
			}
			if hasSensexOptions {
				m.log.Info("No quotes available for SENSEX options - this is a known limitation", map[string]interface{}{
					"sample_symbols": strings.Join(tradingSymbols[:min(5, len(tradingSymbols))], ", "),
				})
			}
			// Check if we have any symbols without exchange prefixes
			var missingPrefixSymbols []string
			var malformedSymbols []string

			// Sample size for checking symbols
			checkSize := 10
			if len(tradingSymbols) < checkSize {
				checkSize = len(tradingSymbols)
			}

			for _, symbol := range tradingSymbols[:checkSize] {
				if !strings.Contains(symbol, ":") {
					missingPrefixSymbols = append(missingPrefixSymbols, symbol)
				} else {
					// Check for potentially malformed symbols
					parts := strings.Split(symbol, ":")
					if len(parts) > 1 {
						exchange := parts[0]
						if exchange != "NSE" && exchange != "BSE" &&
							exchange != "NFO" && exchange != "BFO" &&
							exchange != "CDS" && exchange != "MCX" {
							malformedSymbols = append(malformedSymbols, symbol)
						}
					}
				}
			}

			// Log as debug instead of error or info, since this is expected behavior
			m.log.Debug("No quotes available for requested symbols - this is normal for some symbols", map[string]interface{}{
				"sample_symbols":       strings.Join(tradingSymbols[:min(5, len(tradingSymbols))], ", "),
				"missing_prefix_count": len(missingPrefixSymbols),
				"missing_prefix":       missingPrefixSymbols,
				"malformed_symbols":    malformedSymbols,
			})

			// Return an empty map rather than an error
			return quotes, nil
		}

		// The quotes variable is already of the correct type (map[string]kiteconnect.Quote)
		// so we can return it directly
		return quotes, nil
	}

	// The quotes variable is already of the correct type (map[string]kiteconnect.Quote)
	// so we can return it directly
	return quotes, nil
}

// GetQuoteForSymbols is a convenience function that uses the singleton MarketDataManager
func GetQuoteForSymbols(ctx context.Context, tradingSymbols []string) (kiteconnect.Quote, error) {
	return GetMarketDataManager().GetQuoteForSymbols(ctx, tradingSymbols)
}

// GetTradingSymbolFromParams retrieves the trading symbol for a given index, strike, option type, and expiry
// This uses the cache populated by CreateLookUpforStoringFileFromWebsocketsAndAlsoStrikes
func (m *MarketDataManager) GetTradingSymbolFromParams(ctx context.Context, index string, strike string, optionType string, expiry string) (string, bool) {
	// Get the in-memory cache instance
	inMemoryCache := cache.GetInMemoryCacheInstance()

	// Get the full symbol directly (exchange:symbol format)
	fullSymbolKey := fmt.Sprintf("full_symbol:%s:%s:%s:%s", index, strike, optionType, expiry)
	fullSymbol, fullSymbolFound := inMemoryCache.Get(fullSymbolKey)

	if fullSymbolFound {
		// Return the full symbol directly if found
		m.log.Debug("Found full symbol in cache", map[string]interface{}{
			"full_symbol": fullSymbol,
			"key":         fullSymbolKey,
		})
		return fmt.Sprintf("%v", fullSymbol), true
	} else {
		m.log.Error("Full symbol not found in cache", map[string]interface{}{
			"index":       index,
			"strike":      strike,
			"option_type": optionType,
			"expiry":      expiry,
			"key":         fullSymbolKey,
		})
		return "", false
	}
}

// GetTradingSymbolFromParams is a convenience function that uses the singleton MarketDataManager
func GetTradingSymbolFromParams(ctx context.Context, index string, strike string, optionType string, expiry string) (string, bool) {
	return GetMarketDataManager().GetTradingSymbolFromParams(ctx, index, strike, optionType, expiry)
}

// GetQuoteForOptionParams retrieves market data quotes for options specified by their parameters
// This is a convenience method that combines GetTradingSymbolFromParams and GetQuoteForSymbols
func (m *MarketDataManager) GetQuoteForOptionParams(ctx context.Context, index string, strikes []string, optionTypes []string, expiry string) (kiteconnect.Quote, error) {
	// Collect trading symbols for all combinations
	tradingSymbols := []string{}

	for _, strike := range strikes {
		for _, optionType := range optionTypes {
			tradingSymbol, found := m.GetTradingSymbolFromParams(ctx, index, strike, optionType, expiry)
			if found {
				tradingSymbols = append(tradingSymbols, tradingSymbol)
			}
		}
	}

	// If no trading symbols were found, return an error
	if len(tradingSymbols) == 0 {
		return nil, fmt.Errorf("no trading symbols found for the given parameters")
	}

	// Get quotes for the trading symbols
	return m.GetQuoteForSymbols(ctx, tradingSymbols)
}

// GetQuoteForOptionParams is a convenience function that uses the singleton MarketDataManager
func GetQuoteForOptionParams(ctx context.Context, index string, strikes []string, optionTypes []string, expiry string) (kiteconnect.Quote, error) {
	return GetMarketDataManager().GetQuoteForOptionParams(ctx, index, strikes, optionTypes, expiry)
}

// We'll use inline min calculation instead of a separate function to avoid redeclaration

// GetIndexFromInstrumentToken retrieves the index name (e.g., NIFTY, BANKNIFTY) associated with an instrument token from the cache.
// This is primarily used for index instrument tokens themselves.
func GetIndexFromInstrumentToken(ctx context.Context, instrumentToken string) (string, bool) {
	cache := cache.GetInMemoryCacheInstance()
	if cache == nil {
		logger.L().Error("In-memory cache not initialized", map[string]interface{}{
			"function": "GetIndexFromInstrumentToken",
		})
		return "", false
	}

	// Construct the key used for caching the index name
	instrumentNameKey := fmt.Sprintf("instrument_name_key:%s", instrumentToken)
	// The index name is cached directly with the token as the key
	val, found := cache.Get(instrumentNameKey)
	if !found {
		return "", false
	}

	indexName, ok := val.(string)
	if !ok {
		logger.L().Error("Cached value for index token is not a string", map[string]interface{}{
			"instrument_token": instrumentToken,
			"cached_value":     val,
		})
		return "", false
	}

	return indexName, true
}
