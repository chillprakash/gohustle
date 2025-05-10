package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/logger"
	"gohustle/utils"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Constants for cache keys and expiration
const (
	// Key prefixes with hierarchical structure
	InstrumentExpiryPrefix    = "instrument:expiry:"
	InstrumentTokenPrefix     = "instrument:mapping:token:"
	InstrumentSymbolPrefix    = "instrument:mapping:symbol:"
	InstrumentExpiryListKey   = "instrument:list:expiries"
	InstrumentIndexExpiryKey  = "instrument:mapping:index:expiry"
	InstumentExpiryStrikesKey = "instrument:expiry:strikes"

	// Default expiration times
	DefaultCacheExpiry = 24 * time.Hour
	LongCacheExpiry    = 7 * 24 * time.Hour

	// Maximum number of expiries to process per index
	MaxExpiriesToProcess = 2
)

// CacheMeta provides methods for caching instrument data in Redis
type CacheMeta struct {
	client *redis.Client
	log    *logger.Logger
}

// Singleton instance of CacheMeta
var (
	cacheMetaInstance *CacheMeta
	cacheMetaOnce     sync.Once
	cacheMetaErr      error
)

// GetCacheMetaInstance returns the singleton instance of CacheMeta
func GetCacheMetaInstance() (*CacheMeta, error) {
	cacheMetaOnce.Do(func() {
		redisCache, err := GetRedisCache()
		if err != nil {
			cacheMetaErr = fmt.Errorf("failed to get Redis cache: %w", err)
			return
		}

		cacheMetaInstance = &CacheMeta{
			client: redisCache.GetCacheDB1(),
			log:    logger.L(),
		}
	})

	if cacheMetaErr != nil {
		return nil, cacheMetaErr
	}

	return cacheMetaInstance, nil
}

// NewCacheMeta creates a new CacheMeta instance (deprecated, use GetCacheMetaInstance instead)
func NewCacheMeta() (*CacheMeta, error) {
	return GetCacheMetaInstance()
}

// StoreInstrumentExpiries stores a list of expiry dates for an instrument
func (c *CacheMeta) StoreInstrumentExpiries(ctx context.Context, instrument string, expiries []string) error {
	// Use a more structured key format
	key := fmt.Sprintf("instrument:list:expiry:%s", instrument)

	pipe := c.client.Pipeline()
	pipe.Del(ctx, key)
	if len(expiries) > 0 {
		pipe.SAdd(ctx, key, utils.StringSliceToInterfaceSlice(expiries)...)
		pipe.Expire(ctx, key, LongCacheExpiry)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store instrument expiries", map[string]interface{}{
			"instrument": instrument,
			"error":      err.Error(),
		})
		return err
	}

	c.log.Info("Stored instrument expiries in Redis", map[string]interface{}{
		"instrument":   instrument,
		"expiry_count": len(expiries),
	})

	return nil
}

// GetInstrumentExpiries retrieves the list of expiry dates for an instrument
func (c *CacheMeta) GetInstrumentExpiries(ctx context.Context, instrument string) ([]string, error) {
	// Use the same structured key format as in StoreInstrumentExpiries
	key := fmt.Sprintf("instrument:list:expiry:%s", instrument)

	result, err := c.client.SMembers(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		c.log.Error("Failed to get instrument expiries", map[string]interface{}{
			"instrument": instrument,
			"error":      err.Error(),
		})
		return nil, err
	}

	return result, nil
}

// StoreNearestExpiry stores the nearest expiry date for an instrument
func (c *CacheMeta) StoreNearestExpiry(ctx context.Context, instrument string, expiry string) error {
	// Use a more structured key format
	key := "instrument:mapping:nearest_expiry"
	err := c.client.HSet(ctx, key, instrument, expiry).Err()
	if err != nil {
		c.log.Error("Failed to store nearest expiry", map[string]interface{}{
			"instrument": instrument,
			"expiry":     expiry,
			"error":      err.Error(),
		})
		return err
	}

	c.log.Info("Stored nearest expiry in Redis", map[string]interface{}{
		"instrument": instrument,
		"expiry":     expiry,
	})

	return nil
}

// GetNearestExpiry retrieves the nearest expiry date for an instrument
func (c *CacheMeta) GetNearestExpiry(ctx context.Context, instrument string) (string, error) {
	// Use the same structured key format as in StoreNearestExpiry
	key := "instrument:mapping:nearest_expiry"
	result, err := c.client.HGet(ctx, key, instrument).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		c.log.Error("Failed to get nearest expiry", map[string]interface{}{
			"instrument": instrument,
			"error":      err.Error(),
		})
		return "", err
	}

	return result, nil
}

// StoreTokenSymbolMapping stores the mapping between token and symbol
func (c *CacheMeta) StoreTokenSymbolMapping(ctx context.Context, token string, symbol string) error {
	pipe := c.client.Pipeline()

	// Store token -> symbol mapping
	tokenKey := fmt.Sprintf("%s%s", InstrumentTokenPrefix, token)
	pipe.Set(ctx, tokenKey, symbol, LongCacheExpiry)

	// Store symbol -> token mapping
	symbolKey := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, symbol)
	pipe.Set(ctx, symbolKey, token, LongCacheExpiry)

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store token-symbol mapping", map[string]interface{}{
			"token":  token,
			"symbol": symbol,
			"error":  err.Error(),
		})
		return err
	}

	return nil
}

// GetSymbolByToken retrieves the symbol for a given token
func (c *CacheMeta) GetSymbolByToken(ctx context.Context, token string) (string, error) {
	key := fmt.Sprintf("%s%s", InstrumentTokenPrefix, token)

	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		c.log.Error("Failed to get symbol by token", map[string]interface{}{
			"token": token,
			"error": err.Error(),
		})
		return "", err
	}

	return result, nil
}

// GetTokenBySymbol retrieves the token for a given symbol
func (c *CacheMeta) GetTokenBySymbol(ctx context.Context, symbol string) (string, error) {
	key := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, symbol)

	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		c.log.Error("Failed to get token by symbol", map[string]interface{}{
			"symbol": symbol,
			"error":  err.Error(),
		})
		return "", err
	}

	return result, nil
}

// StoreInstrumentsList stores the list of available instruments
func (c *CacheMeta) StoreInstrumentsList(ctx context.Context, instruments []string) error {
	pipe := c.client.Pipeline()

	// Delete existing list
	pipe.Del(ctx, InstrumentExpiryListKey)

	// Add new instruments
	if len(instruments) > 0 {
		pipe.SAdd(ctx, InstrumentExpiryListKey, utils.StringSliceToInterfaceSlice(instruments)...)
		pipe.Expire(ctx, InstrumentExpiryListKey, LongCacheExpiry)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store instruments list", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Stored instruments list in Redis", map[string]interface{}{
		"count": len(instruments),
	})

	return nil
}

// GetInstrumentsList retrieves the list of available instruments
func (c *CacheMeta) GetInstrumentsList(ctx context.Context) ([]string, error) {
	result, err := c.client.SMembers(ctx, InstrumentExpiryListKey).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		c.log.Error("Failed to get instruments list", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	return result, nil
}

// StoreExpiryMap stores a map of index to expiry dates
func (c *CacheMeta) StoreExpiryMap(ctx context.Context, expiryMap map[string][]time.Time) error {
	// Convert to string format for storage
	stringMap := make(map[string][]string)
	for index, dates := range expiryMap {
		stringDates := make([]string, 0, len(dates))
		for _, date := range dates {
			stringDates = append(stringDates, utils.FormatKiteDate(date))
		}
		stringMap[index] = stringDates
	}

	// Serialize to JSON
	jsonData, err := json.Marshal(stringMap)
	if err != nil {
		c.log.Error("Failed to marshal expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Store in Redis
	err = c.client.Set(ctx, "instrument:expiry:map", string(jsonData), LongCacheExpiry).Err()
	if err != nil {
		c.log.Error("Failed to store expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Stored expiry map in Redis", map[string]interface{}{
		"index_count": len(expiryMap),
	})

	return nil
}

// GetExpiryMap retrieves the map of index to expiry dates
func (c *CacheMeta) GetExpiryMap(ctx context.Context) (map[string][]time.Time, error) {
	// Get from Redis
	jsonData, err := c.client.Get(ctx, "instrument:expiry:map").Result()
	if err != nil {
		if err == redis.Nil {
			return make(map[string][]time.Time), nil
		}
		c.log.Error("Failed to get expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Deserialize from JSON
	stringMap := make(map[string][]string)
	err = json.Unmarshal([]byte(jsonData), &stringMap)
	if err != nil {
		c.log.Error("Failed to unmarshal expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Convert back to time.Time
	result := make(map[string][]time.Time)
	for index, stringDates := range stringMap {
		dates := make([]time.Time, 0, len(stringDates))
		for _, stringDate := range stringDates {
			date, err := utils.ParseKiteDate(stringDate)
			if err != nil {
				c.log.Error("Failed to parse date in expiry map", map[string]interface{}{
					"date":  stringDate,
					"error": err.Error(),
				})
				continue
			}
			dates = append(dates, date)
		}
		result[index] = dates
	}

	return result, nil
}

// InstrumentData represents the essential data for an instrument
type InstrumentData struct {
	Name           string
	TradingSymbol  string
	InstrumentType string
	StrikePrice    string
	Expiry         string
	Exchange       string
	Token          string
}

// SyncInstrumentExpiries syncs expiry dates for instruments to Redis cache
func (c *CacheMeta) SyncInstrumentExpiries(ctx context.Context, expiryMap map[string][]time.Time, allowedIndices []string) error {
	c.log.Info("Syncing instrument expiries to Redis cache", nil)

	// Create map of allowed indices for faster lookup
	allowedMap := make(map[string]bool)
	for _, name := range allowedIndices {
		allowedMap[name] = true
	}

	// Process expiry dates for each instrument
	today := utils.GetTodayIST()

	// Store list of instruments
	instrumentsList := make([]string, 0, len(expiryMap))

	for instrument, dates := range expiryMap {
		// Skip if not in allowed indices
		if !allowedMap[instrument] {
			continue
		}

		instrumentsList = append(instrumentsList, instrument)

		// Filter and convert valid dates to string format
		dateStrs := make([]string, 0, len(dates))
		for _, date := range dates {
			// Only include dates equal to or after today
			if !date.Before(today) {
				dateStrs = append(dateStrs, utils.FormatKiteDate(date))
			}
		}

		// Store expiries for this instrument
		if len(dateStrs) > 0 {
			if err := c.StoreInstrumentExpiries(ctx, instrument, dateStrs); err != nil {
				c.log.Error("Failed to store instrument expiries", map[string]interface{}{
					"instrument": instrument,
					"error":      err.Error(),
				})
				continue
			}

			// Find and store nearest expiry
			nearestExpiry, err := utils.GetNearestFutureDate(dateStrs)
			if err == nil && nearestExpiry != "" {
				if err := c.StoreNearestExpiry(ctx, instrument, nearestExpiry); err != nil {
					c.log.Error("Failed to store nearest expiry", map[string]interface{}{
						"instrument": instrument,
						"expiry":     nearestExpiry,
						"error":      err.Error(),
					})
				}
			}
		}
	}

	// Store list of instruments
	if err := c.StoreInstrumentsList(ctx, instrumentsList); err != nil {
		c.log.Error("Failed to store instruments list", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Store the expiry map for future reference
	if err := c.StoreExpiryMap(ctx, expiryMap); err != nil {
		c.log.Error("Failed to store expiry map", map[string]interface{}{
			"error": err.Error(),
		})
	}

	c.log.Info("Successfully synced instrument expiries to Redis cache", map[string]interface{}{
		"instruments_count": len(instrumentsList),
	})

	return nil
}

// StoreExpiryStrikes stores unique strikes for each expiry
func (c *CacheMeta) StoreExpiryStrikes(ctx context.Context, indexStrikeMap map[string]map[string]map[string]bool) error {
	c.log.Info("Storing unique strikes for each expiry", nil)

	// indexStrikeMap structure: map[indexName]map[expiry]map[strike]bool
	pipe := c.client.Pipeline()

	// Clear existing data
	pipe.Del(ctx, InstumentExpiryStrikesKey)

	// Store the data as a JSON string for each index and expiry
	for indexName, expiryMap := range indexStrikeMap {
		for expiry, strikesMap := range expiryMap {
			// Convert map of strikes to a sorted slice
			strikes := make([]string, 0, len(strikesMap))
			for strike := range strikesMap {
				strikes = append(strikes, strike)
			}

			// Sort strikes for consistent ordering
			sort.Strings(strikes)

			// Store as a field in a hash: instrument:expiry:strikes -> {index}:{expiry} -> [strikes]
			strikesJSON, err := json.Marshal(strikes)
			if err != nil {
				c.log.Error("Failed to marshal strikes to JSON", map[string]interface{}{
					"index":  indexName,
					"expiry": expiry,
					"error":  err.Error(),
				})
				continue
			}

			// Use a field name that combines index and expiry
			fieldName := fmt.Sprintf("%s:%s", indexName, expiry)
			pipe.HSet(ctx, InstumentExpiryStrikesKey, fieldName, string(strikesJSON))
		}
	}

	// Set expiration
	pipe.Expire(ctx, InstumentExpiryStrikesKey, LongCacheExpiry)

	// Execute the pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store expiry strikes", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Successfully stored expiry strikes", map[string]interface{}{
		"indices_count": len(indexStrikeMap),
	})

	return nil
}

// GetExpiryStrikes retrieves unique strikes for a specific index and expiry
func (c *CacheMeta) GetExpiryStrikes(ctx context.Context, indexName, expiry string) ([]string, error) {
	// Construct the field name
	fieldName := fmt.Sprintf("%s:%s", indexName, expiry)

	// Get the JSON string from Redis
	strikesJSON, err := c.client.HGet(ctx, InstumentExpiryStrikesKey, fieldName).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		c.log.Error("Failed to get expiry strikes", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return nil, err
	}

	// Unmarshal the JSON string
	var strikes []string
	if err := json.Unmarshal([]byte(strikesJSON), &strikes); err != nil {
		c.log.Error("Failed to unmarshal strikes JSON", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return nil, err
	}

	return strikes, nil
}

// SyncInstrumentMetadata syncs instrument metadata to Redis cache
func (c *CacheMeta) SyncInstrumentMetadata(ctx context.Context, instruments []InstrumentData) error {
	c.log.Info("Syncing instrument metadata to Redis cache", nil)

	// Pipeline for batch operations
	pipe := c.client.Pipeline()

	// Process each instrument
	for _, inst := range instruments {
		// Use a consistent naming convention with hierarchical structure
		// Format: instrument:{category}:{subcategory}:{identifier}

		// Cache token -> symbol and symbol -> token mapping
		tokenKey := fmt.Sprintf("instrument:mapping:token:%s", inst.Token)
		symbolKey := fmt.Sprintf("instrument:mapping:symbol:%s", inst.TradingSymbol)
		pipe.Set(ctx, tokenKey, inst.TradingSymbol, LongCacheExpiry)
		pipe.Set(ctx, symbolKey, inst.Token, LongCacheExpiry)

		// Cache instrument name
		instrumentNameKey := fmt.Sprintf("instrument:metadata:%s:name", inst.Token)
		pipe.Set(ctx, instrumentNameKey, inst.Name, LongCacheExpiry)

		// If this is an option (CE or PE), cache additional metadata
		if inst.InstrumentType == "CE" || inst.InstrumentType == "PE" {
			// Parse and format strike price
			strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
			if err == nil {
				strikeStr := fmt.Sprintf("%d", int(strike))

				// Cache strike price
				strikeKey := fmt.Sprintf("instrument:metadata:%s:strike", inst.Token)
				pipe.Set(ctx, strikeKey, strikeStr, LongCacheExpiry)

				// Cache instrument type (CE/PE)
				instrumentTypeKey := fmt.Sprintf("instrument:metadata:%s:type", inst.Token)
				pipe.Set(ctx, instrumentTypeKey, inst.InstrumentType, LongCacheExpiry)

				// Cache expiry date
				expiryKey := fmt.Sprintf("instrument:metadata:%s:expiry", inst.Token)
				pipe.Set(ctx, expiryKey, inst.Expiry, LongCacheExpiry)

				// Cache lookup for next move - organized by strike, type and expiry
				nextMoveLookupKey := fmt.Sprintf("instrument:lookup:next_move:%s:%s:%s",
					strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, nextMoveLookupKey, inst.Token, LongCacheExpiry)

				// Cache trading symbol lookup - organized by instrument components
				tradingSymbolKey := fmt.Sprintf("instrument:lookup:symbol:%s:%s:%s:%s",
					inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, tradingSymbolKey, inst.TradingSymbol, LongCacheExpiry)

				// Cache exchange
				exchangeKey := fmt.Sprintf("instrument:lookup:exchange:%s:%s:%s:%s",
					inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, exchangeKey, inst.Exchange, LongCacheExpiry)

				// Cache full symbol (exchange:symbol)
				fullSymbolKey := fmt.Sprintf("instrument:lookup:full_symbol:%s:%s:%s:%s",
					inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
				fullSymbol := fmt.Sprintf("%s:%s", inst.Exchange, inst.TradingSymbol)
				pipe.Set(ctx, fullSymbolKey, fullSymbol, LongCacheExpiry)
			}
		}
	}

	// Execute the pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to execute Redis pipeline for instrument metadata", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Successfully synced instrument metadata to Redis cache", map[string]interface{}{
		"instruments_count": len(instruments),
	})

	return nil
}
