package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/logger"
	"gohustle/utils"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Constants for cache keys and expiration
const (
	// Expiry related keys
	InstrumentExpiryMapKey                                         = "instrument:expiry:json_map"
	InstumentExpiryStrikesKey                                      = "instrument:expiry:strikes"
	InstrumentExpiryStrikesWithExchangeAndInstrumentSymbol         = "instrument:expiry:strikes_with_exchange_and_instrument_symbol"
	InstrumentExpiryStrikesWithExchangeAndInstrumentSymbolFiltered = "instrument:expiry:strikes_with_exchange_and_instrument_symbol:filtered"
	InstrumentExpiryStrikesWithTokenKey                            = "instrument:expiry:strikes_with_token"
	InstrumentExpiryListKey                                        = "instrument:list:expiries"
	InstrumentIndexExpiryKey                                       = "instrument:mapping:index:expiry"
	InstrumentNearestExpiryKey                                     = "instrument:mapping:nearest_expiry"

	// Mapping keys
	InstrumentTokenPrefix  = "instrument:mapping:token:"
	InstrumentSymbolPrefix = "instrument:mapping:symbol:"

	// Metadata keys
	InstrumentMetadataPrefix       = "instrument:metadata:"
	InstrumentMetadataNameSuffix   = ":name"
	InstrumentMetadataStrikeSuffix = ":strike"
	InstrumentMetadataTypeSuffix   = ":type"
	InstrumentMetadataExpirySuffix = ":expiry"

	// Lookup keys
	InstrumentLookupPrefix           = "instrument:lookup:"
	InstrumentLookupNextMovePrefix   = "instrument:lookup:next_move:"
	InstrumentLookupSymbolPrefix     = "instrument:lookup:symbol:"
	InstrumentLookupExchangePrefix   = "instrument:lookup:exchange:"
	InstrumentLookupFullSymbolPrefix = "instrument:lookup:full_symbol:"

	// Default expiration times
	DefaultCacheExpiry = 24 * time.Hour
	LongCacheExpiry    = 7 * 24 * time.Hour

	// Maximum number of expiries to process per index
	MaxExpiriesToProcess = 1
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

func (c *CacheMeta) GetExpiryStrikesWithExchangeAndInstrumentSymbol(ctx context.Context, index string, expiry string) ([]string, error) {
	key := fmt.Sprintf("%s:%s_%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbolFiltered, index, expiry)
	result, err := c.client.SMembers(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return []string{}, nil
		}
		c.log.Error("Failed to get expiry strikes with exchange and instrument symbol", map[string]interface{}{
			"index":  index,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return nil, err
	}

	return result, nil
}

func (c *CacheMeta) StoreFilteredInstrumentTokens(ctx context.Context, indexName string, expiry string, tokens []string) {
	key := fmt.Sprintf("%s:%s_%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbolFiltered, indexName, expiry)

	pipe := c.client.Pipeline()
	pipe.Del(ctx, key)
	if len(tokens) > 0 {
		pipe.SAdd(ctx, key, utils.StringSliceToInterfaceSlice(tokens)...)
		pipe.Expire(ctx, key, LongCacheExpiry)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store instrument tokens", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return
	}

	c.log.Info("Stored instrument tokens in Redis", map[string]interface{}{
		"index":       indexName,
		"expiry":      expiry,
		"token_count": len(tokens),
	})

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
	// Use the constant for the nearest expiry key
	key := InstrumentNearestExpiryKey
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

func (c *CacheMeta) GetNearestExpiry(ctx context.Context, instrument string) (string, error) {
	// Use InstrumentExpiryMapKey instead of InstrumentNearestExpiryKey
	key := InstrumentExpiryMapKey
	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		c.log.Error("Failed to get expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return "", err
	}

	// Parse the JSON response
	var expiryMap map[string][]string
	if err := json.Unmarshal([]byte(result), &expiryMap); err != nil {
		c.log.Error("Failed to unmarshal expiry map", map[string]interface{}{
			"error": err.Error(),
		})
		return "", err
	}

	// Get the expiries for the requested instrument
	expiries, ok := expiryMap[instrument]
	if !ok || len(expiries) == 0 {
		return "", nil
	}

	// Return the first (nearest) expiry
	return expiries[0], nil
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

func (c *CacheMeta) GetIndexNameFromSymbol(ctx context.Context, symbol string) (string, error) {
	if strings.Contains(symbol, "BANKNIFTY") {
		return "BANKNIFTY", nil
	}
	if strings.Contains(symbol, "NIFTY") {
		return "NIFTY", nil
	}
	if strings.Contains(symbol, "SENSEX") {
		return "SENSEX", nil
	}
	return "", nil
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
	err = c.client.Set(ctx, InstrumentExpiryMapKey, string(jsonData), LongCacheExpiry).Err()
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
	jsonData, err := c.client.Get(ctx, InstrumentExpiryMapKey).Result()
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

// GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol retrieves instrument data for a specific index and expiry
func (c *CacheMeta) GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol(ctx context.Context, indexName string, expiry string) ([]string, error) {
	c.log.Info("Getting instrument expiry strikes with exchange and instrument symbol", map[string]interface{}{
		"index":  indexName,
		"expiry": expiry,
	})

	// Construct the key
	key := fmt.Sprintf("%s:%s_%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbol, indexName, expiry)

	// Get the data from Redis
	jsonData, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			c.log.Info("No data found for key", map[string]interface{}{
				"key": key,
			})
			return nil, nil
		}
		c.log.Error("Failed to get instrument data from Redis", map[string]interface{}{
			"error": err.Error(),
			"key":   key,
		})
		return nil, err
	}

	// Parse the JSON data
	var formattedValues []string
	if err := json.Unmarshal([]byte(jsonData), &formattedValues); err != nil {
		c.log.Error("Failed to unmarshal JSON data", map[string]interface{}{
			"error": err.Error(),
			"key":   key,
		})
		return nil, err
	}

	c.log.Info("Successfully retrieved instrument expiry strikes", map[string]interface{}{
		"index":        indexName,
		"expiry":       expiry,
		"values_count": len(formattedValues),
	})

	return formattedValues, nil
}

// SyncInstrumentExpiries syncs expiry dates for instruments to Redis cache
func (c *CacheMeta) SyncInstrumentExpiries(ctx context.Context, expiryMap map[string][]time.Time) error {
	c.log.Info("Syncing instrument expiries to Redis cache", nil)

	// Store the expiry map for future reference
	if err := c.StoreExpiryMap(ctx, expiryMap); err != nil {
		c.log.Error("Failed to store expiry map", map[string]interface{}{
			"error": err.Error(),
		})
	}

	c.log.Info("Successfully synced instrument expiries to Redis cache")

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
// SyncInstrumentExpiryStrikesWithSymbols syncs instrument expiry strikes with exchange and instrument symbols to Redis cache
func (c *CacheMeta) SyncInstrumentExpiryStrikesWithSymbols(ctx context.Context, instruments []InstrumentData) error {
	c.log.Info("Syncing instrument expiry strikes with symbols to Redis cache", nil)

	// Pipeline for batch operations
	pipe := c.client.Pipeline()

	// Create a nested map to organize instruments by name, expiry, and strike
	// Structure: map[name]map[expiry]map[strike]map[type]instrumentData
	instrumentMap := make(map[string]map[string]map[string]map[string]InstrumentData)

	// Process each instrument to populate the map
	for _, inst := range instruments {
		// Only process options (CE/PE)
		if inst.InstrumentType != "CE" && inst.InstrumentType != "PE" {
			continue
		}

		// Parse and format strike price
		strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
		if err != nil {
			c.log.Error("Failed to parse strike price", map[string]interface{}{
				"symbol":       inst.TradingSymbol,
				"strike_price": inst.StrikePrice,
				"error":        err.Error(),
			})
			continue
		}

		// Format strike as whole number
		strikeStr := fmt.Sprintf("%d", int(strike))

		// Initialize nested maps if needed
		if instrumentMap[inst.Name] == nil {
			instrumentMap[inst.Name] = make(map[string]map[string]map[string]InstrumentData)
		}
		if instrumentMap[inst.Name][inst.Expiry] == nil {
			instrumentMap[inst.Name][inst.Expiry] = make(map[string]map[string]InstrumentData)
		}
		if instrumentMap[inst.Name][inst.Expiry][strikeStr] == nil {
			instrumentMap[inst.Name][inst.Expiry][strikeStr] = make(map[string]InstrumentData)
		}

		// Store the instrument data
		instrumentMap[inst.Name][inst.Expiry][strikeStr][inst.InstrumentType] = inst
	}

	// Now process the instrumentMap to create the formatted data for Redis
	incompleteDataCount := 0
	for name, expiryMap := range instrumentMap {
		for expiry, strikeMap := range expiryMap {
			// Create key
			key := fmt.Sprintf("%s_%s", name, expiry)

			// Create formatted values
			formattedValues := make([]string, 0, len(strikeMap))

			for strike, typeMap := range strikeMap {
				// Get CE and PE symbols
				ceSymbol := ""
				peSymbol := ""
				exchange := ""

				if ceInst, ok := typeMap["CE"]; ok {
					ceSymbol = ceInst.TradingSymbol
					exchange = ceInst.Exchange
				}

				if peInst, ok := typeMap["PE"]; ok {
					peSymbol = peInst.TradingSymbol
					if exchange == "" {
						exchange = peInst.Exchange
					}
				}

				// Log error if data is incomplete
				if ceSymbol == "" || peSymbol == "" {
					c.log.Debug("Incomplete option data for strike", map[string]interface{}{
						"name":      name,
						"expiry":    expiry,
						"strike":    strike,
						"ce_symbol": ceSymbol,
						"pe_symbol": peSymbol,
					})
					incompleteDataCount++
				}

				// Format the value
				formattedValue := fmt.Sprintf("strike:%s||ce:%s||pe:%s||exchange:%s",
					strike, ceSymbol, peSymbol, exchange)

				formattedValues = append(formattedValues, formattedValue)
			}

			// Sort values for consistent ordering
			sort.Strings(formattedValues)

			// Store in Redis
			jsonData, err := json.Marshal(formattedValues)
			if err != nil {
				c.log.Error("Failed to marshal formatted values", map[string]interface{}{
					"name":   name,
					"expiry": expiry,
					"error":  err.Error(),
				})
				continue
			}

			redisKey := fmt.Sprintf("%s:%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbol, key)
			pipe.Set(ctx, redisKey, string(jsonData), LongCacheExpiry)

			c.log.Debug("Added strike data for index and expiry", map[string]interface{}{
				"key":           key,
				"strikes_count": len(formattedValues),
			})
		}
	}

	// Execute the pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to execute Redis pipeline for instrument expiry strikes", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Successfully synced instrument expiry strikes with symbols to Redis cache", map[string]interface{}{
		"instruments_count":     len(instruments),
		"incomplete_data_count": incompleteDataCount,
	})

	return nil
}

// QuoteData represents the essential data from a market quote needed for OI filtering
type QuoteData struct {
	Symbol          string
	OI              float64
	LastPrice       float64
	InstrumentToken string
}

// QuoteFetcher is a function type that fetches quotes for a list of symbols
type QuoteFetcher func(ctx context.Context, symbols []string) (map[string]QuoteData, error)

// FilterInstrumentsByOI retrieves instruments from cache and filters them based on Open Interest criteria
// Parameters:
// - ctx: Context for cancellation and timeout
// - indexName: The name of the index (e.g., "NIFTY", "BANKNIFTY")
// - expiry: The expiry date in the format used in the cache key (e.g., "2025-05-15")
// - minOI: Minimum Open Interest threshold
// - maxStrikes: Maximum number of strikes to return (0 for unlimited)
// - optionTypes: Option types to include ("CE", "PE", or both)
// - fetchQuotes: A function that fetches quotes for symbols
func (c *CacheMeta) FilterInstrumentsByOI(ctx context.Context, indexName string, expiry string, minOI int, maxStrikes int, optionTypes []string, fetchQuotes QuoteFetcher) ([]InstrumentData, error) {
	c.log.Info("Filtering instruments by Open Interest", map[string]interface{}{
		"index":        indexName,
		"expiry":       expiry,
		"min_oi":       minOI,
		"max_strikes":  maxStrikes,
		"option_types": optionTypes,
	})

	// Validate inputs
	if indexName == "" || expiry == "" {
		return nil, fmt.Errorf("index name and expiry are required")
	}

	if len(optionTypes) == 0 {
		// Default to both CE and PE if not specified
		optionTypes = []string{"CE", "PE"}
	}

	// Create a map to check valid option types
	validOptionTypes := make(map[string]bool)
	for _, ot := range optionTypes {
		validOptionTypes[ot] = true
	}

	// Construct the Redis key
	key := fmt.Sprintf("%s:%s_%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbol, indexName, expiry)

	// Get the data from Redis
	jsonData, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			c.log.Error("No data found for the given index and expiry", map[string]interface{}{
				"index":  indexName,
				"expiry": expiry,
				"key":    key,
			})
			return nil, fmt.Errorf("no data found for %s with expiry %s", indexName, expiry)
		}
		c.log.Error("Failed to get data from Redis", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
			"key":    key,
			"error":  err.Error(),
		})
		return nil, err
	}

	// Parse the JSON data
	var formattedValues []string
	if err := json.Unmarshal([]byte(jsonData), &formattedValues); err != nil {
		c.log.Error("Failed to unmarshal JSON data", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return nil, err
	}

	// Extract trading symbols for quotes
	var ceSymbols []string
	var peSymbols []string
	strikeToSymbols := make(map[string]map[string]string) // map[strike]map[optionType]symbol

	for _, value := range formattedValues {
		// Parse the formatted value: "strike:21000||ce:NIFTY25MAY21000CE||pe:NIFTY25MAY21000PE||exchange:NFO"
		parts := strings.Split(value, "||")
		if len(parts) < 4 {
			c.log.Debug("Invalid formatted value", map[string]interface{}{
				"value": value,
			})
			continue
		}

		// Extract strike, CE symbol, PE symbol, and exchange
		var strike, ceSymbol, peSymbol, exchange string
		for _, part := range parts {
			keyValue := strings.SplitN(part, ":", 2)
			if len(keyValue) != 2 {
				continue
			}

			switch keyValue[0] {
			case "strike":
				strike = keyValue[1]
			case "ce":
				ceSymbol = keyValue[1]
			case "pe":
				peSymbol = keyValue[1]
			case "exchange":
				exchange = keyValue[1]
			}
		}

		// Skip if strike is empty
		if strike == "" {
			continue
		}

		// Initialize the map for this strike if needed
		if strikeToSymbols[strike] == nil {
			strikeToSymbols[strike] = make(map[string]string)
		}

		// Add CE symbol if it exists and is requested
		if ceSymbol != "" && validOptionTypes["CE"] {
			ceSymbols = append(ceSymbols, fmt.Sprintf("%s:%s", exchange, ceSymbol))
			strikeToSymbols[strike]["CE"] = ceSymbol
		}

		// Add PE symbol if it exists and is requested
		if peSymbol != "" && validOptionTypes["PE"] {
			peSymbols = append(peSymbols, fmt.Sprintf("%s:%s", exchange, peSymbol))
			strikeToSymbols[strike]["PE"] = peSymbol
		}
	}

	// Combine CE and PE symbols
	allSymbols := append(ceSymbols, peSymbols...)

	// If no symbols were found, return an empty result
	if len(allSymbols) == 0 {
		c.log.Error("No symbols found for the given index and expiry", map[string]interface{}{
			"index":  indexName,
			"expiry": expiry,
		})
		return []InstrumentData{}, nil
	}

	// Get quotes for all symbols
	c.log.Info("Fetching quotes for symbols", map[string]interface{}{
		"symbols_count": len(allSymbols),
		"sample":        strings.Join(allSymbols[:min(5, len(allSymbols))], ", "),
	})

	// Use the provided function to fetch quotes
	quotes, err := fetchQuotes(ctx, allSymbols)
	if err != nil {
		c.log.Error("Failed to get quotes", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Process quotes and filter by OI
	type StrikeOI struct {
		Strike string
		OI     float64
		Type   string // CE or PE
	}

	// Collect OI data for each strike and option type
	var strikeOIData []StrikeOI
	for symbol, quote := range quotes {
		// Extract the option type (CE/PE) from the symbol
		var optionType string
		if strings.HasSuffix(symbol, "CE") {
			optionType = "CE"
		} else if strings.HasSuffix(symbol, "PE") {
			optionType = "PE"
		} else {
			continue // Skip if not an option
		}

		// Find the strike for this symbol
		var strike string
		for s, typeMap := range strikeToSymbols {
			for t, sym := range typeMap {
				if strings.HasSuffix(symbol, sym) && t == optionType {
					strike = s
					break
				}
			}
			if strike != "" {
				break
			}
		}

		if strike == "" {
			continue // Skip if strike not found
		}

		// Check if OI meets the minimum threshold
		if int(quote.OI) >= minOI {
			strikeOIData = append(strikeOIData, StrikeOI{
				Strike: strike,
				OI:     quote.OI,
				Type:   optionType,
			})
		}
	}

	// Sort by OI (descending)
	sort.Slice(strikeOIData, func(i, j int) bool {
		return strikeOIData[i].OI > strikeOIData[j].OI
	})

	// Limit the number of strikes if maxStrikes is specified
	if maxStrikes > 0 && len(strikeOIData) > maxStrikes {
		strikeOIData = strikeOIData[:maxStrikes]
	}

	// Create a set of selected strikes
	selectedStrikes := make(map[string]bool)
	for _, data := range strikeOIData {
		selectedStrikes[data.Strike] = true
	}

	// Build the result
	var result []InstrumentData
	for _, data := range strikeOIData {
		// Get the trading symbol for this strike and option type
		symbol, ok := strikeToSymbols[data.Strike][data.Type]
		if !ok {
			continue
		}

		// Create an InstrumentData object
		inst := InstrumentData{
			Name:           indexName,
			TradingSymbol:  symbol,
			InstrumentType: data.Type,
			StrikePrice:    data.Strike,
			Expiry:         expiry,
			Exchange:       "NFO", // Assuming NFO for options
			// Token is not available from the quote data
		}

		result = append(result, inst)
	}

	c.log.Info("Filtered instruments by OI", map[string]interface{}{
		"index":            indexName,
		"expiry":           expiry,
		"total_symbols":    len(allSymbols),
		"filtered_symbols": len(result),
		"min_oi":           minOI,
	})

	return result, nil
}

// Helper function to find the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *CacheMeta) SyncInstrumentMetadata(ctx context.Context, instruments []InstrumentData) error {
	c.log.Info("Syncing instrument metadata to Redis cache", nil)

	// Pipeline for batch operations
	pipe := c.client.Pipeline()

	// Process each instrument
	for _, inst := range instruments {
		// Use a consistent naming convention with hierarchical structure
		// Format: instrument:{category}:{subcategory}:{identifier}

		// Cache token -> symbol and symbol -> token mapping
		tokenKey := fmt.Sprintf("%s%s", InstrumentTokenPrefix, inst.Token)
		symbolKey := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, inst.TradingSymbol)
		pipe.Set(ctx, tokenKey, inst.TradingSymbol, LongCacheExpiry)
		pipe.Set(ctx, symbolKey, inst.Token, LongCacheExpiry)

		// Cache instrument name
		instrumentNameKey := fmt.Sprintf("%s%s%s", InstrumentMetadataPrefix, inst.Token, InstrumentMetadataNameSuffix)
		pipe.Set(ctx, instrumentNameKey, inst.Name, LongCacheExpiry)

		// If this is an option (CE or PE), cache additional metadata
		if inst.InstrumentType == "CE" || inst.InstrumentType == "PE" {
			// Parse and format strike price
			strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
			if err == nil {
				strikeStr := fmt.Sprintf("%d", int(strike))

				// Note: Expiry strikes with symbols are now handled in SyncInstrumentExpiryStrikesWithSymbols

				// Cache strike price
				strikeKey := fmt.Sprintf("%s%s%s", InstrumentMetadataPrefix, inst.Token, InstrumentMetadataStrikeSuffix)
				pipe.Set(ctx, strikeKey, strikeStr, LongCacheExpiry)

				// Cache instrument type (CE/PE)
				instrumentTypeKey := fmt.Sprintf("%s%s%s", InstrumentMetadataPrefix, inst.Token, InstrumentMetadataTypeSuffix)
				pipe.Set(ctx, instrumentTypeKey, inst.InstrumentType, LongCacheExpiry)

				// Cache expiry date
				expiryKey := fmt.Sprintf("%s%s%s", InstrumentMetadataPrefix, inst.Token, InstrumentMetadataExpirySuffix)
				pipe.Set(ctx, expiryKey, inst.Expiry, LongCacheExpiry)

				// Cache lookup for next move - organized by strike, type and expiry
				nextMoveLookupKey := fmt.Sprintf("%s%s:%s:%s",
					InstrumentLookupNextMovePrefix, strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, nextMoveLookupKey, inst.Token, LongCacheExpiry)

				// Cache trading symbol lookup - organized by instrument components
				tradingSymbolKey := fmt.Sprintf("%s%s:%s:%s:%s",
					InstrumentLookupSymbolPrefix, inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, tradingSymbolKey, inst.TradingSymbol, LongCacheExpiry)

				// Cache exchange
				exchangeKey := fmt.Sprintf("%s%s:%s:%s:%s",
					InstrumentLookupExchangePrefix, inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
				pipe.Set(ctx, exchangeKey, inst.Exchange, LongCacheExpiry)

				// Cache full symbol (exchange:symbol)
				fullSymbolKey := fmt.Sprintf("%s%s:%s:%s:%s",
					InstrumentLookupFullSymbolPrefix, inst.Name, strikeStr, inst.InstrumentType, inst.Expiry)
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
