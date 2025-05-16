package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"gohustle/logger"
	"gohustle/utils"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Constants for cache keys and expiration
const (
	ReferenceSubscribedKeys = "reference:subscribed_keys"
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
	InstrumentTokenPrefix                = "instrument:mapping:token:"
	InstrumentSymbolPrefix               = "instrument:mapping:symbol:"
	InstruemntStrikeWithOptionTypePrefix = "instrument:mapping:strike_with_option_type:"

	// Metadata keys
	InstrumentMetadataPrefix = "instrument:metadata:"

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
	LTPSuffix            = "_ltp"
	OISuffix             = "_oi"
	VolumeSuffix         = "_volume"
)

// CacheMeta provides methods for caching instrument data in Redis
type CacheMeta struct {
	client *redis.Client
	log    *logger.Logger
	ltpDB  *redis.Client
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
			ltpDB:  redisCache.GetLTPDB3(),
		}
	})

	if cacheMetaErr != nil {
		return nil, cacheMetaErr
	}

	return cacheMetaInstance, nil
}

func (c *CacheMeta) GetInstrumentTokenForSymbol(ctx context.Context, tradingSymbol string) (interface{}, bool) {
	key := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, tradingSymbol)
	result, err := c.client.Get(ctx, key).Result()
	if err == nil {
		return result, true
	}
	return nil, false
}

func (c *CacheMeta) GetInstrumentSymbolForToken(ctx context.Context, token string) (interface{}, bool) {
	key := fmt.Sprintf("%s%s", InstrumentTokenPrefix, token)
	result, err := c.client.Get(ctx, key).Result()
	if err == nil {
		return result, true
	}
	return nil, false
}

// NewCacheMeta creates a new CacheMeta instance (deprecated, use GetCacheMetaInstance instead)
func NewCacheMeta() (*CacheMeta, error) {
	return GetCacheMetaInstance()
}

func (c *CacheMeta) StoreSubscribedKeys(ctx context.Context, tokens []interface{}) error {
	key := ReferenceSubscribedKeys
	return c.client.SAdd(ctx, key, tokens...).Err()
}

func (c *CacheMeta) GetExpiryStrikesWithExchangeAndInstrumentSymbol(ctx context.Context, index string, expiry string) ([]uint32, error) {
	key := fmt.Sprintf("%s:%s_%s", InstrumentExpiryStrikesWithExchangeAndInstrumentSymbolFiltered, index, expiry)
	result, err := c.client.SMembers(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return []uint32{}, nil
		}
		c.log.Error("Failed to get expiry strikes with exchange and instrument symbol", map[string]interface{}{
			"index":  index,
			"expiry": expiry,
			"error":  err.Error(),
		})
		return nil, err
	}

	return utils.StringSliceToUint32Slice(result), nil
}

func (c *CacheMeta) GetLTPforInstrumentToken(ctx context.Context, token string) (LTPData, error) {
	ltpDB := c.ltpDB
	// Process results
	data := &LTPData{
		instrumentToken: utils.StringToUint32(token),
	}

	// Create a map to store command results
	cmdMap := make(map[string]*redis.StringCmd)

	// Create pipeline
	ltpPipe := ltpDB.Pipeline()

	// Store commands in map for later retrieval
	cmdMap[fmt.Sprintf("%s_ltp", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_ltp", token))
	cmdMap[fmt.Sprintf("%s_oi", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_oi", token))
	cmdMap[fmt.Sprintf("%s_volume", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_volume", token))

	// Execute pipeline
	_, err := ltpPipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		c.log.Error("Failed to execute LTP pipeline", map[string]interface{}{
			"error": err.Error(),
			"token": token,
		})
		// Continue execution even if there's an error, as some commands might have succeeded
	}

	// Get LTP
	ltpCmd := cmdMap[fmt.Sprintf("%s_ltp", token)]
	if ltpCmd != nil {
		ltp, err := ltpCmd.Float64()
		if err == nil {
			data.LTP = ltp
		} else if err != redis.Nil {
			c.log.Error("Failed to get LTP", map[string]interface{}{
				"token": token,
				"error": err.Error(),
			})
		}
	}

	// Get OI
	oiCmd := cmdMap[fmt.Sprintf("%s_oi", token)]
	if oiCmd != nil {
		oi, err := oiCmd.Float64()
		if err == nil {
			data.OI = oi
		} else if err != redis.Nil {
			c.log.Error("Failed to get OI", map[string]interface{}{
				"token": token,
				"error": err.Error(),
			})
		}
	}

	// Get Volume
	volumeCmd := cmdMap[fmt.Sprintf("%s_volume", token)]
	if volumeCmd != nil {
		volume, err := volumeCmd.Float64()
		if err == nil {
			data.Volume = volume
		} else if err != redis.Nil {
			c.log.Error("Failed to get Volume", map[string]interface{}{
				"token": token,
				"error": err.Error(),
			})
		}
	}

	return *data, nil

}

func (c *CacheMeta) GetLTPforInstrumentTokensList(ctx context.Context, tokens []string) ([]LTPData, error) {
	ltpDB := c.ltpDB
	ltpdataList := make([]LTPData, 0, len(tokens))

	// Create a map to store command results
	cmdMap := make(map[string]*redis.StringCmd)

	// Create pipeline
	ltpPipe := ltpDB.Pipeline()

	// Queue all the commands
	for _, token := range tokens {
		// Store commands in map for later retrieval
		cmdMap[fmt.Sprintf("%s_ltp", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_ltp", token))
		cmdMap[fmt.Sprintf("%s_oi", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_oi", token))
		cmdMap[fmt.Sprintf("%s_volume", token)] = ltpPipe.Get(ctx, fmt.Sprintf("%s_volume", token))
	}

	// Execute pipeline
	_, err := ltpPipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		c.log.Error("Failed to execute LTP pipeline", map[string]interface{}{
			"error":  err.Error(),
			"tokens": tokens,
		})
		// Continue execution even if there's an error, as some commands might have succeeded
	}

	// Process results
	for _, token := range tokens {
		data := &LTPData{
			instrumentToken: utils.StringToUint32(token),
		}

		// Get LTP
		ltpCmd := cmdMap[fmt.Sprintf("%s_ltp", token)]
		if ltpCmd != nil {
			ltp, err := ltpCmd.Float64()
			if err == nil {
				data.LTP = ltp
			} else if err != redis.Nil {
				c.log.Error("Failed to get LTP", map[string]interface{}{
					"token": token,
					"error": err.Error(),
				})
			}
		}

		// Get OI
		oiCmd := cmdMap[fmt.Sprintf("%s_oi", token)]
		if oiCmd != nil {
			oi, err := oiCmd.Float64()
			if err == nil {
				data.OI = oi
			} else if err != redis.Nil {
				c.log.Error("Failed to get OI", map[string]interface{}{
					"token": token,
					"error": err.Error(),
				})
			}
		}

		// Get Volume
		volumeCmd := cmdMap[fmt.Sprintf("%s_volume", token)]
		if volumeCmd != nil {
			volume, err := volumeCmd.Float64()
			if err == nil {
				data.Volume = volume
			} else if err != redis.Nil {
				c.log.Error("Failed to get Volume", map[string]interface{}{
					"token": token,
					"error": err.Error(),
				})
			}
		}

		ltpdataList = append(ltpdataList, *data)
	}
	// Only log a sample of the data to avoid huge logs
	sampleSize := 3
	if len(ltpdataList) < sampleSize {
		sampleSize = len(ltpdataList)
	}

	c.log.Debug("Successfully retrieved LTP for instrument tokens", map[string]interface{}{
		"token_count": len(tokens),
		"data_count":  len(ltpdataList),
		"data_sample": ltpdataList[:sampleSize],
	})
	return ltpdataList, nil
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

// StoreInstrumentOIData stores OI data for instruments in Redis
func (c *CacheMeta) StoreInstrumentOIData(ctx context.Context, tokenOIMap map[string]float64) error {
	if len(tokenOIMap) == 0 {
		return nil
	}

	// Use pipeline for better performance
	pipe := c.ltpDB.Pipeline()

	// Store OI data for each token
	for token, oi := range tokenOIMap {
		// Create key with OI suffix
		key := fmt.Sprintf("%s%s", token, OISuffix)

		// Store OI value in Redis
		pipe.Set(ctx, key, oi, DefaultCacheExpiry)
	}

	// Execute pipeline
	_, err := pipe.Exec(ctx)
	if err != nil {
		c.log.Error("Failed to store instrument OI data", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	c.log.Info("Successfully stored OI data in Redis", map[string]interface{}{
		"count": len(tokenOIMap),
	})

	return nil
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
	Name            string
	TradingSymbol   string
	InstrumentType  string
	StrikePrice     string
	Expiry          string
	Exchange        string
	Token           string
	InstrumentToken uint32
}

type LTPData struct {
	instrumentToken uint32
	LTP             float64
	OI              float64
	Volume          float64
}

// GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol retrieves instrument data for a specific index and expiry
func (c *CacheMeta) GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol(ctx context.Context, indexName string, expiry string) ([]string, error) {
	c.log.Debug("Getting instrument expiry strikes with exchange and instrument symbol", map[string]interface{}{
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

	c.log.Debug("Successfully retrieved instrument expiry strikes", map[string]interface{}{
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

type OptionChainStrikeStruct struct {
	Strike   int64
	CEToken  uint32
	PEToken  uint32
	CELTP    float64
	PELTP    float64
	CEOI     uint32
	PEOI     uint32
	CEVolume uint32
	PEVolume uint32
}

func (c *CacheMeta) GetOptionChainStrikeStructList(ctx context.Context, indexName, expiry string, strikes []string) (map[int64]OptionChainStrikeStruct, error) {
	strikesWithExchangeSymbolAndJsonStringArray, err := c.GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol(ctx, indexName, expiry)
	if err != nil {
		return nil, err
	}
	instrumentTokenToLtpMap := make(map[uint32]float64)
	instumentTokenList := make([]uint32, 0)
	mapToReturn := make(map[int64]OptionChainStrikeStruct)

	var optionChainStrikeStructList []OptionChainStrikeStruct
	for _, strikeWithExchangeSymbolAndJsonString := range strikesWithExchangeSymbolAndJsonStringArray {
		c.log.Debug("Processing strike data string", map[string]interface{}{
			"data_string": strikeWithExchangeSymbolAndJsonString,
		})

		// Parse the delimited string format: strike:65600||ce:SENSEX2551365600CE||pe:SENSEX2551365600PE||exchange:BFO||ce_token:283567621||pe_token:283215365
		parts := strings.Split(strikeWithExchangeSymbolAndJsonString, "||")
		if len(parts) < 6 {
			c.log.Error("Invalid format for strike data string", map[string]interface{}{
				"data_string": strikeWithExchangeSymbolAndJsonString,
				"parts_count": len(parts),
				"index":       indexName,
				"expiry":      expiry,
			})
			return nil, fmt.Errorf("invalid format for strike data string: %s", strikeWithExchangeSymbolAndJsonString)
		}
		// Extract strike, CE token, and PE token
		strikeStr := strings.TrimPrefix(parts[0], "strike:")
		_ = strings.TrimPrefix(parts[1], "ce:")
		_ = strings.TrimPrefix(parts[2], "pe:")
		_ = strings.TrimPrefix(parts[3], "exchange:")
		ceToken := strings.TrimPrefix(parts[4], "ce_token:")
		peToken := strings.TrimPrefix(parts[5], "pe_token:")

		// Convert strike to int64
		strike, err := strconv.ParseInt(strikeStr, 10, 64)
		if err != nil {
			c.log.Error("Failed to parse strike to int64", map[string]interface{}{
				"error":      err.Error(),
				"strike_str": strikeStr,
				"index":      indexName,
				"expiry":     expiry,
			})
			return nil, fmt.Errorf("failed to parse strike to int64: %w", err)
		}

		// Create OptionChainStrikeStruct
		optionChainStrikeStruct := OptionChainStrikeStruct{
			Strike:  strike,
			CEToken: utils.StringToUint32(ceToken),
			PEToken: utils.StringToUint32(peToken),
		}

		if slices.Contains(strikes, strikeStr) {
			optionChainStrikeStructList = append(optionChainStrikeStructList, optionChainStrikeStruct)
		}
		instumentTokenList = append(instumentTokenList, utils.StringToUint32(ceToken))
		instumentTokenList = append(instumentTokenList, utils.StringToUint32(peToken))
	}

	ltpDataList, err := c.GetLTPforInstrumentTokensList(ctx, utils.Uint32SliceToStringSlice(instumentTokenList))
	if err != nil {
		return nil, err
	}

	for _, ltpData := range ltpDataList {
		instrumentTokenToLtpMap[ltpData.instrumentToken] = ltpData.LTP
	}

	// Create a map to store OI and volume data
	instrumentTokenToOIMap := make(map[string]uint32)
	instrumentTokenToVolumeMap := make(map[string]uint32)

	// Extract OI and volume from the LTP data
	for _, ltpData := range ltpDataList {
		instrumentTokenToOIMap[strconv.FormatUint(uint64(ltpData.instrumentToken), 10)] = uint32(ltpData.OI)
		instrumentTokenToVolumeMap[strconv.FormatUint(uint64(ltpData.instrumentToken), 10)] = uint32(ltpData.Volume)
	}

	// Populate the option chain strike struct with all data
	for _, optionChainStrikeStruct := range optionChainStrikeStructList {
		// Set LTP values
		if ceLtp, exists := instrumentTokenToLtpMap[optionChainStrikeStruct.CEToken]; exists {
			optionChainStrikeStruct.CELTP = ceLtp
		}
		if peLtp, exists := instrumentTokenToLtpMap[optionChainStrikeStruct.PEToken]; exists {
			optionChainStrikeStruct.PELTP = peLtp
		}

		// Set OI values
		if ceOI, exists := instrumentTokenToOIMap[utils.Uint32ToString(optionChainStrikeStruct.CEToken)]; exists {
			optionChainStrikeStruct.CEOI = ceOI
		}
		if peOI, exists := instrumentTokenToOIMap[utils.Uint32ToString(optionChainStrikeStruct.PEToken)]; exists {
			optionChainStrikeStruct.PEOI = peOI
		}

		// Set Volume values
		if ceVolume, exists := instrumentTokenToVolumeMap[utils.Uint32ToString(optionChainStrikeStruct.CEToken)]; exists {
			optionChainStrikeStruct.CEVolume = ceVolume
		}
		if peVolume, exists := instrumentTokenToVolumeMap[utils.Uint32ToString(optionChainStrikeStruct.PEToken)]; exists {
			optionChainStrikeStruct.PEVolume = peVolume
		}

		mapToReturn[optionChainStrikeStruct.Strike] = optionChainStrikeStruct
	}
	c.log.Debug("Successfully retrieved option chain strike struct list", map[string]interface{}{
		"index":       indexName,
		"expiry":      expiry,
		"token_count": len(instumentTokenList),
		"map_count":   len(mapToReturn),
	})
	return mapToReturn, nil
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
				ceToken := uint32(0)
				peToken := uint32(0)

				if ceInst, ok := typeMap["CE"]; ok {
					ceSymbol = ceInst.TradingSymbol
					exchange = ceInst.Exchange
					ceToken = ceInst.InstrumentToken
				}

				if peInst, ok := typeMap["PE"]; ok {
					peSymbol = peInst.TradingSymbol
					if exchange == "" {
						exchange = peInst.Exchange
					}
					peToken = peInst.InstrumentToken
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
				formattedValue := fmt.Sprintf("strike:%s||ce:%s||pe:%s||exchange:%s||ce_token:%d||pe_token:%d",
					strike, ceSymbol, peSymbol, exchange, ceToken, peToken)

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

func (c *CacheMeta) GetMetadataOfToken(ctx context.Context, token string) (InstrumentData, error) {
	instrumentMetadataKey := fmt.Sprintf("%s%s", InstrumentMetadataPrefix, token)
	result, err := c.client.Get(ctx, instrumentMetadataKey).Result()
	if err != nil {
		if err == redis.Nil {
			return InstrumentData{Token: token, InstrumentToken: utils.StringToUint32(token)}, nil
		}
		c.log.Error("Failed to get instrument metadata", map[string]interface{}{
			"token": token,
			"error": err.Error(),
		})
		return InstrumentData{}, err
	}

	// Parse the metadata string and map it to InstrumentData
	return convertMetadataToInstrumentData(result, token)
}

func convertMetadataToInstrumentData(metadata string, token string) (InstrumentData, error) {
	// The metadata string format is "{name}:{strikePrice}:{instrumentType}:{expiry}"
	parts := strings.Split(metadata, ":")

	// Create a new InstrumentData instance
	inst := InstrumentData{
		Token:           token,
		InstrumentToken: utils.StringToUint32(token),
	}

	// Map the parts to the InstrumentData struct fields
	if len(parts) >= 1 {
		inst.Name = parts[0]
	}

	if len(parts) >= 2 {
		inst.StrikePrice = parts[1]
	}

	if len(parts) >= 3 {
		inst.InstrumentType = parts[2]
	}

	if len(parts) >= 4 {
		inst.Expiry = parts[3]
	}

	if len(parts) >= 5 {
		inst.TradingSymbol = parts[4]
	}

	return inst, nil
}

func (c *CacheMeta) GetInstrumentTokenForStrike(ctx context.Context, strike float64, instrumentType, expiry string) (string, error) {
	// Format the strike as an integer string without decimal
	strikeStr := utils.RemoveDecimal(fmt.Sprintf("%v", strike))

	// Create the lookup key using the new pattern
	lookupKey := fmt.Sprintf("%s%s_%s_%s", InstruemntStrikeWithOptionTypePrefix, strikeStr, instrumentType, expiry)

	// Get the token from Redis
	token, err := c.client.Get(ctx, lookupKey).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("instrument token not found for strike %v %s %s", strike, instrumentType, expiry)
		}
		return "", fmt.Errorf("error getting instrument token: %w", err)
	}

	return token, nil
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
		// tokenKey := fmt.Sprintf("%s%s", InstrumentTokenPrefix, inst.Token)
		// symbolKey := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, inst.TradingSymbol)
		// pipe.Set(ctx, tokenKey, inst.TradingSymbol, LongCacheExpiry)
		// pipe.Set(ctx, symbolKey, inst.Token, LongCacheExpiry)

		// Cache instrument name
		instrumentNameKey := fmt.Sprintf("%s%s", InstrumentMetadataPrefix, inst.Token)
		pipe.Set(ctx, instrumentNameKey, inst.Name, LongCacheExpiry)

		// If this is an option (CE or PE), cache additional metadata
		if inst.InstrumentType == "CE" || inst.InstrumentType == "PE" {
			// Parse and format strike price
			strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
			if err == nil {
				strikeStr := fmt.Sprintf("%d", int(strike))

				// Note: Expiry strikes with symbols are now handled in SyncInstrumentExpiryStrikesWithSymbols

				// Cache strike price
				strikeKey := fmt.Sprintf("%s%s", InstrumentMetadataPrefix, inst.Token)
				value := fmt.Sprintf("%s:%s:%s:%s:%s", inst.Name, strikeStr, inst.InstrumentType, inst.Expiry, inst.TradingSymbol)
				pipe.Set(ctx, strikeKey, value, LongCacheExpiry)

				tokenKey := fmt.Sprintf("%s%s", InstrumentTokenPrefix, inst.Token)
				pipe.Set(ctx, tokenKey, inst.TradingSymbol, LongCacheExpiry)

				symbolKey := fmt.Sprintf("%s%s", InstrumentSymbolPrefix, inst.TradingSymbol)
				pipe.Set(ctx, symbolKey, inst.Token, LongCacheExpiry)

				strikeInt := utils.RemoveDecimal(inst.StrikePrice)
				strikeWithTypeKey := fmt.Sprintf("%s%s_%s_%s",
					InstruemntStrikeWithOptionTypePrefix,
					strikeInt,
					inst.InstrumentType,
					inst.Expiry)

				pipe.Set(ctx, strikeWithTypeKey, inst.Token, LongCacheExpiry)

				c.log.Debug("Cached strike with option type", map[string]interface{}{
					"key":    strikeWithTypeKey,
					"token":  inst.Token,
					"strike": strikeInt,
					"type":   inst.InstrumentType,
					"expiry": inst.Expiry,
				})
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
