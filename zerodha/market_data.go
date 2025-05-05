package zerodha

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/logger"
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

// GetInstrumentToken retrieves the instrument token for a given trading symbol
// Returns the token and a boolean indicating if the token was found
func (m *MarketDataManager) GetInstrumentToken(ctx context.Context, tradingSymbol string) (interface{}, bool) {
	if tradingSymbol == "" {
		m.log.Error("Empty trading symbol provided", nil)
		return nil, false
	}

	// Get the in-memory cache instance
	inMemoryCache := cache.GetInMemoryCacheInstance()
	if inMemoryCache == nil {
		m.log.Error("Failed to get in-memory cache instance", nil)
		return nil, false
	}

	// Try to get the instrument token from the cache
	instrumentToken, exists := inMemoryCache.Get(tradingSymbol)
	if exists {
		m.log.Debug("Found instrument token in cache", map[string]interface{}{
			"trading_symbol":   tradingSymbol,
			"instrument_token": instrumentToken,
		})
		return instrumentToken, true
	}

	// If not found in cache, try to find it in Redis
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		m.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, false
	}

	// Try to get from Redis using the trading symbol as key
	redisDB := redisCache.GetLTPDB3() // Using LTP DB as it's commonly used for market data
	if redisDB == nil {
		m.log.Error("Redis DB is nil", nil)
		return nil, false
	}

	// Try to get the token from Redis
	tokenStr, err := redisDB.Get(ctx, tradingSymbol).Result()
	if err == nil && tokenStr != "" {
		// Try to parse as integer first
		tokenInt, err := strconv.ParseInt(tokenStr, 10, 64)
		if err == nil {
			// Store in cache for future use
			inMemoryCache.Set(tradingSymbol, tokenInt, 7*24*time.Hour)
			m.log.Debug("Found instrument token in Redis and cached it", map[string]interface{}{
				"trading_symbol":   tradingSymbol,
				"instrument_token": tokenInt,
			})
			return tokenInt, true
		}

		// If not an integer, return as string
		inMemoryCache.Set(tradingSymbol, tokenStr, 7*24*time.Hour)
		m.log.Debug("Found instrument token (string) in Redis and cached it", map[string]interface{}{
			"trading_symbol":   tradingSymbol,
			"instrument_token": tokenStr,
		})
		return tokenStr, true
	}

	m.log.Debug("Instrument token not found for trading symbol", map[string]interface{}{
		"trading_symbol": tradingSymbol,
	})
	return nil, false
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

// GetInstrumentToken is a convenience function that uses the singleton MarketDataManager
func GetInstrumentToken(ctx context.Context, tradingSymbol string) (interface{}, bool) {
	return GetMarketDataManager().GetInstrumentToken(ctx, tradingSymbol)
}
