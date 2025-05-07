package zerodha

import (
	"container/list"
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/utils"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	"google.golang.org/protobuf/proto"
)

// OptionTokenPair pairs a trading symbol with its instrument token
type OptionTokenPair struct {
	Symbol          string
	InstrumentToken string
	Strike          string
}

// ExpiryOptions contains calls and puts for a specific expiry
type ExpiryOptions struct {
	Calls []OptionTokenPair
	Puts  []OptionTokenPair
}

// InstrumentExpiryMap organizes options by instrument and expiry

type InstrumentExpiryMap struct {
	Data map[string]map[time.Time]ExpiryOptions
}

type TokenInfo struct {
	Expiry                 time.Time
	Symbol                 string
	Index                  string
	IsIndex                bool
	TargetFile             string
	StrikeWithAbbreviation string
}

var (
	tokensCache                  map[string]string
	reverseLookupCacheWithStrike map[string]string
	reverseLookupCache           map[string]TokenInfo
	instrumentMutex              sync.RWMutex
	once                         sync.Once
	expiryCache                  *cache.InMemoryCache
)

func init() {
	expiryCache = cache.GetInMemoryCacheInstance()
}

func (k *KiteConnect) GetTentativeATMBasedonLTP(index core.Index, strikes []string) string {
	log := logger.L()
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return ""
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		log.Error("Failed to get LTP DB", map[string]interface{}{
			"error": "LTP DB is nil",
		})
		return ""
	}
	ltp, err := ltpDB.Get(context.Background(), fmt.Sprintf("%s_ltp", index.InstrumentToken)).Float64()
	if err != nil {
		log.Error("Failed to get LTP for index", map[string]interface{}{
			"error": err.Error(),
			"index": index.InstrumentToken,
		})
		return ""
	}

	// Find nearest strike to LTP
	var nearestStrike string
	minDiff := math.MaxFloat64

	for _, strike := range strikes {
		strikePrice, err := strconv.ParseFloat(strike, 64)
		if err != nil {
			log.Error("Failed to parse strike price", map[string]interface{}{
				"error":  err.Error(),
				"strike": strike,
			})
			continue
		}

		diff := math.Abs(ltp - strikePrice)
		if diff < minDiff {
			minDiff = diff
			nearestStrike = strike
		}
	}

	return nearestStrike
}

func (k *KiteConnect) GetInstrumentInfoWithStrike(strikes []string) map[string]string {
	log := logger.L()

	instrumentMutex.RLock()
	defer instrumentMutex.RUnlock()

	result := make(map[string]string)

	// Debug print the cache contents
	log.Info("Current strike cache contents", map[string]interface{}{
		"cache_size": len(reverseLookupCacheWithStrike),
	})

	for _, strike := range strikes {
		// Check for CE
		ceStrike := fmt.Sprintf("%sCE", strike)
		if token, exists := reverseLookupCacheWithStrike[ceStrike]; exists {
			result[ceStrike] = token
			log.Info("Found CE strike", map[string]interface{}{
				"strike": ceStrike,
				"token":  token,
			})
		} else {
			log.Info("CE strike not found", map[string]interface{}{
				"strike": ceStrike,
			})
		}

		// Check for PE
		peStrike := fmt.Sprintf("%sPE", strike)
		if token, exists := reverseLookupCacheWithStrike[peStrike]; exists {
			result[peStrike] = token
			log.Debug("Found PE strike", map[string]interface{}{
				"strike": peStrike,
				"token":  token,
			})
		} else {
			log.Debug("PE strike not found", map[string]interface{}{
				"strike": peStrike,
			})
		}
	}

	// Print what we found
	log.Info("Strike lookup results", map[string]interface{}{
		"input_strikes": strikes,
		"found_strikes": len(result),
		"results":       result,
	})

	return result
}

// DownloadInstrumentData downloads and saves instrument data
func (k *KiteConnect) DownloadInstrumentData(ctx context.Context, instrumentNames []core.Index) error {
	log := logger.L()
	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()

	// Check if file already exists for today
	exists := fileStore.FileExists("instruments", currentDate)
	if exists {
		log.Info("Instruments file already exists for today, skipping download", map[string]interface{}{
			"date": currentDate,
		})
		return nil
	}
	// Get all instruments
	allInstruments, err := k.Kite.GetInstruments()
	if err != nil {
		log.Error("Failed to download instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Filter instruments
	filteredInstruments := filterInstruments(allInstruments, instrumentNames)

	log.Info("Filtered instruments", map[string]interface{}{
		"total_count":    len(allInstruments),
		"filtered_count": len(filteredInstruments),
		"instruments":    instrumentNames,
	})

	// Save filtered data
	return k.saveInstrumentsToFile(filteredInstruments)
}

func (k *KiteConnect) SyncInstrumentExpiriesFromFileToCache(ctx context.Context) error {
	log := logger.L()

	// Read expiries from file
	expiries, err := k.readInstrumentExpiriesFromFile(ctx)
	if err != nil {
		log.Error("Failed to read expiries from file", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	cache := cache.GetInMemoryCacheInstance()
	now := utils.NowIST().Truncate(24 * time.Hour)

	// Get allowed indices from core package
	allowedIndices := core.GetIndices().GetAllNames()

	// Store expiries in memory cache
	for instrument, dates := range expiries {
		// Filter: only process instruments in our allowed list
		if !slices.Contains(allowedIndices, instrument) {
			log.Info("Skipping instrument not in allowed list", map[string]interface{}{
				"instrument": instrument,
			})
			continue
		}

		// Get today's date truncated to start of day
		today := utils.NowIST().Truncate(24 * time.Hour)

		// Filter and convert valid dates to string format
		dateStrs := make([]string, 0, len(dates))
		for _, date := range dates {
			// Only include dates equal to or after today
			if !date.Before(today) {
				dateStrs = append(dateStrs, date.Format("2006-01-02"))
			}
		}

		// Only proceed if we have valid future dates
		if len(dateStrs) > 0 {
			// Key for instrument expiries
			key := fmt.Sprintf("instrument:expiries:%s", instrument)

			// Store dates as string slice
			cache.Set(key, dateStrs, 7*24*time.Hour) // Cache for 7 days

			// Find and store nearest expiry
			var nearestExpiry string
			for _, dateStr := range dateStrs {
				date, _ := time.Parse("2006-01-02", dateStr)
				if date.Before(now) {
					continue
				}
				if nearestExpiry == "" || date.Before(now) {
					nearestExpiry = dateStr
				}
			}

			if nearestExpiry != "" {
				nearestKey := fmt.Sprintf("instrument:nearest_expiry:%s", instrument)
				cache.Set(nearestKey, nearestExpiry, 7*24*time.Hour)
				log.Info("Stored nearest expiry for instrument", map[string]interface{}{
					"instrument": instrument,
					"expiry":     nearestExpiry,
				})
			}
		}
	}

	// Store list of instruments (only allowed ones)
	instrumentsKey := "instrument:expiries:list"
	instruments := make([]string, 0)
	for instrument := range expiries {
		if slices.Contains(allowedIndices, instrument) {
			instruments = append(instruments, instrument)
		}
	}
	cache.Set(instrumentsKey, instruments, 7*24*time.Hour)

	log.Info("Successfully stored expiries in memory cache", map[string]interface{}{
		"instruments_count": len(instruments),
		"expiries":          formatExpiryMapForLog(expiries),
	})

	return nil
}

// GetCachedExpiries retrieves expiries for an instrument from the cache
func (k *KiteConnect) GetCachedExpiries(instrument string) ([]time.Time, bool) {
	key := fmt.Sprintf("instrument:expiries:%s", instrument)
	if value, exists := expiryCache.Get(key); exists {
		if dates, ok := value.([]time.Time); ok {
			return dates, true
		}
	}
	return nil, false
}

// GetCachedInstruments retrieves the list of instruments from the cache
func (k *KiteConnect) GetCachedInstruments() ([]string, bool) {
	if value, exists := expiryCache.Get("instrument:expiries:list"); exists {
		if instruments, ok := value.([]string); ok {
			return instruments, true
		}
	}
	return nil, false
}

// GetInstrumentExpiries reads the gzipped instrument data and returns expiry dates
func (k *KiteConnect) readInstrumentExpiriesFromFile(ctx context.Context) (map[string][]time.Time, error) {
	log := logger.L()
	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()

	// Read the gzipped data
	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return nil, err
	}

	// Unmarshal the protobuf data
	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Map to store unique expiries for each instrument
	expiryMap := make(map[string]map[time.Time]bool)

	// Get allowed indices from core package
	indices := core.GetIndices()
	allowedIndices := make(map[string]bool)
	for _, name := range indices.GetAllNames() {
		allowedIndices[name] = true
	}

	// Process each instrument
	for _, inst := range instrumentList.Instruments {
		// Skip if not in allowed indices
		if !allowedIndices[inst.Name] {
			continue
		}

		// Parse expiry date
		expiry, err := time.Parse("02-01-2006", inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date", map[string]interface{}{
				"error":         err.Error(),
				"instrument":    inst.Name,
				"tradingsymbol": inst.Tradingsymbol,
				"expiry":        inst.Expiry,
			})
			continue
		}

		// Skip invalid dates (year before 2024)
		if expiry.Year() < 2024 {
			continue
		}

		// Add expiry to map
		if expiryMap[inst.Name] == nil {
			expiryMap[inst.Name] = make(map[time.Time]bool)
		}
		expiryMap[inst.Name][expiry] = true
	}

	result := convertExpiryMapToSortedSlices(expiryMap)

	return result, nil
}

// Helper function to format expiry map for logging
func formatExpiryMapForLog(m map[string][]time.Time) map[string][]string {
	formatted := make(map[string][]string)
	for instrument, dates := range m {
		formatted[instrument] = make([]string, len(dates))
		for i, date := range dates {
			formatted[instrument][i] = date.Format("2006-01-02")
		}
	}
	return formatted
}

func (k *KiteConnect) GetUpcomingExpiryTokensForIndices(ctx context.Context, indices []core.Index) ([]string, error) {
	log := logger.L()
	cache := cache.GetInMemoryCacheInstance()
	tokens := make([]string, 0)

	for _, index := range indices {
		// Get nearest expiry for this index from cache
		nearestKey := fmt.Sprintf("instrument:nearest_expiry:%s", index.NameInOptions)
		value, exists := cache.Get(nearestKey)
		if !exists {
			log.Error("No nearest expiry found in cache for index", map[string]interface{}{
				"index": index.NameInOptions,
			})
			continue
		}

		nearestExpiry, ok := value.(string)
		if !ok {
			log.Error("Invalid data type in cache for nearest expiry", map[string]interface{}{
				"index": index.NameInOptions,
			})
			continue
		}

		// Get strikes for this index and expiry
		strikesKey := fmt.Sprintf("strikes:%s_%s", index.NameInOptions, nearestExpiry)
		strikesValue, exists := cache.Get(strikesKey)
		if !exists {
			log.Error("No strikes found in cache for index and expiry", map[string]interface{}{
				"index":  index.NameInOptions,
				"expiry": nearestExpiry,
			})
			continue
		}

		strikes, ok := strikesValue.([]string)
		if !ok {
			log.Error("Invalid data type in cache for strikes", map[string]interface{}{
				"index":  index.NameInOptions,
				"expiry": nearestExpiry,
			})
			continue
		}

		log.Info("Found strikes for index", map[string]interface{}{
			"index":         index.NameInOptions,
			"expiry":        nearestExpiry,
			"strikes_count": len(strikes),
		})

		// For each strike, get CE/PE tokens
		for _, strike := range strikes {
			detailsKey := fmt.Sprintf("%s_%s_%s", index.NameInOptions, nearestExpiry, strike)
			detailsValue, exists := cache.Get(detailsKey)
			if !exists {
				log.Debug("No details found for strike", map[string]interface{}{
					"strike": strike,
					"key":    detailsKey,
				})
				continue
			}

			details, ok := detailsValue.(string)
			if !ok {
				log.Error("Invalid data type for strike details", map[string]interface{}{
					"strike": strike,
					"key":    detailsKey,
				})
				continue
			}

			// Format: p_19126786|p_BANKNIFTY25APR48300PE||c_19126530|c_BANKNIFTY25APR48300CE
			// Split by || to separate PE and CE
			parts := strings.Split(details, "||")
			if len(parts) != 2 {
				log.Error("Invalid details format", map[string]interface{}{
					"details": details,
					"strike":  strike,
				})
				continue
			}

			// Extract PE token
			peTokenParts := strings.Split(parts[0], "|")
			if len(peTokenParts) == 2 {
				peToken := strings.TrimPrefix(strings.Split(peTokenParts[0], "_")[1], "")
				tokens = append(tokens, peToken)
			}

			// Extract CE token
			ceTokenParts := strings.Split(parts[1], "|")
			if len(ceTokenParts) == 2 {
				ceToken := strings.TrimPrefix(strings.Split(ceTokenParts[0], "_")[1], "")
				tokens = append(tokens, ceToken)
			}
		}
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("no tokens found for upcoming expiries")
	}

	log.Info("Retrieved tokens for upcoming expiries", map[string]interface{}{
		"indices_count": len(indices),
		"tokens_count":  len(tokens),
		"sample_tokens": tokens[:min(5, len(tokens))],
	})

	return tokens, nil
}

func (k *KiteConnect) CreateLookUpOfExpiryVsAllDetailsInSingleString(ctx context.Context, indices []core.Index) ([]string, error) {
	log := logger.L()
	log.Info("Initializing lookup maps for File Store", nil)

	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()
	cache := cache.GetInMemoryCacheInstance()

	// First get nearest expiry for each index from cache
	indexExpiries := make(map[string]string)
	for _, index := range indices {
		nearestKey := fmt.Sprintf("instrument:nearest_expiry:%s", index.NameInOptions)
		value, exists := cache.Get(nearestKey)
		if !exists {
			log.Error("No nearest expiry found in cache for index", map[string]interface{}{
				"index": index.NameInOptions,
			})
			continue
		}

		nearestExpiry, ok := value.(string)
		if !ok {
			log.Error("Invalid data type in cache for nearest expiry", map[string]interface{}{
				"index": index.NameInOptions,
			})
			continue
		}

		// Convert cache date format (2025-04-24) to instrument format (24-04-2025)
		t, err := time.Parse("2006-01-02", nearestExpiry)
		if err != nil {
			log.Error("Failed to parse nearest expiry date", map[string]interface{}{
				"error":  err.Error(),
				"expiry": nearestExpiry,
			})
			continue
		}
		indexExpiries[index.NameInOptions] = t.Format("02-01-2006")
	}

	if len(indexExpiries) == 0 {
		return nil, fmt.Errorf("no nearest expiries found in cache, please run SyncInstrumentExpiriesFromFileToCache first")
	}

	log.Info("Found nearest expiries from cache", map[string]interface{}{
		"expiries": indexExpiries,
	})

	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return nil, err
	}

	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Map to store expiry_strike -> put/call details
	strikeDetails := make(map[string]map[string]string)
	expiryStrikeMap := make(map[string]*list.List)
	uniqueStrikes := make(map[string]map[string]bool) // To track unique strikes per expiry

	// Process instruments only for nearest expiry
	for _, inst := range instrumentList.Instruments {
		// Skip if not CE or PE
		if inst.InstrumentType != "CE" && inst.InstrumentType != "PE" {
			continue
		}

		// Check if this is the nearest expiry for this index
		nearestExpiry, exists := indexExpiries[inst.Name]
		if !exists || inst.Expiry != nearestExpiry {
			log.Debug("Skipping instrument", map[string]interface{}{
				"instrument":     inst.Name,
				"inst_expiry":    inst.Expiry,
				"nearest_expiry": nearestExpiry,
				"match":          inst.Expiry == nearestExpiry,
			})
			continue
		}

		// Convert strike price to whole number
		strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
		if err != nil {
			log.Error("Failed to parse strike price", map[string]interface{}{
				"error":        err.Error(),
				"strike_price": inst.StrikePrice,
				"symbol":       inst.Tradingsymbol,
			})
			continue
		}
		strikeStr := fmt.Sprintf("%d", int(strike))

		// Create expiry_strike key using YYYY-MM-DD format for consistency
		expiryDate, _ := time.Parse("02-01-2006", inst.Expiry)
		expiryKey := fmt.Sprintf("%s_%s_%s", inst.Name, expiryDate.Format("2006-01-02"), strikeStr)

		if strikeDetails[expiryKey] == nil {
			strikeDetails[expiryKey] = make(map[string]string)
		}

		// Initialize list and unique strikes map for this expiry if not exists
		expiryMapKey := fmt.Sprintf("%s_%s", inst.Name, expiryDate.Format("2006-01-02"))
		if expiryStrikeMap[expiryMapKey] == nil {
			expiryStrikeMap[expiryMapKey] = list.New()
			uniqueStrikes[expiryMapKey] = make(map[string]bool)
		}

		// Add strike to list if not already present
		if !uniqueStrikes[expiryMapKey][strikeStr] {
			expiryStrikeMap[expiryMapKey].PushBack(strikeStr)
			uniqueStrikes[expiryMapKey][strikeStr] = true
		}

		// Store instrument details
		prefix := "p"
		if inst.InstrumentType == "CE" {
			prefix = "c"
		}
		details := fmt.Sprintf("%s_%s|%s_%s", prefix, inst.InstrumentToken, prefix, inst.Tradingsymbol)
		strikeDetails[expiryKey][inst.InstrumentType] = details
	}

	// Now combine CE and PE details for each expiry_strike and store in cache
	processedStrikes := make([]string, 0)
	for expiryKey, details := range strikeDetails {
		ce, hasCE := details["CE"]
		pe, hasPE := details["PE"]

		if !hasCE || !hasPE {
			log.Debug("Incomplete option pair", map[string]interface{}{
				"expiry_strike": expiryKey,
				"has_ce":        hasCE,
				"has_pe":        hasPE,
			})
			continue
		}

		// Combine CE and PE details
		combinedDetails := fmt.Sprintf("%s||%s", pe, ce)
		cache.Set(expiryKey, combinedDetails, 7*24*time.Hour)
		processedStrikes = append(processedStrikes, expiryKey)
	}

	// Store expiry-strike mapping in cache
	for expiryKey, strikes := range expiryStrikeMap {
		// Convert linked list to sorted slice
		strikeSlice := make([]string, 0, strikes.Len())
		for e := strikes.Front(); e != nil; e = e.Next() {
			strikeSlice = append(strikeSlice, e.Value.(string))
		}
		sort.Strings(strikeSlice) // Sort strikes numerically

		// Store in cache with prefix to distinguish from other keys
		cacheKey := fmt.Sprintf("strikes:%s", expiryKey)
		cache.Set(cacheKey, strikeSlice, 7*24*time.Hour)

		log.Info("Stored strikes for expiry", map[string]interface{}{
			"expiry":  expiryKey,
			"strikes": len(strikeSlice),
		})
	}
	log.Info("Created expiry-strike lookup for nearest expiry", map[string]interface{}{
		"processed_strikes":  len(processedStrikes),
		"sample_expiry_keys": processedStrikes[:min(3, len(processedStrikes))],
		"total_instruments":  len(instrumentList.Instruments),
	})

	return processedStrikes, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetInstrumentDetailsByToken retrieves instrument details (trading symbol, exchange) from an instrument token
func (k *KiteConnect) GetInstrumentDetailsByToken(ctx context.Context, instrumentToken string) (string, string, error) {
	log := logger.L()

	// Check if we have the instrument details in cache
	instrumentMutex.RLock()
	info, exists := reverseLookupCache[instrumentToken]
	instrumentMutex.RUnlock()

	if exists {
		return info.Symbol, "NFO", nil // Most instruments are from NFO exchange
	}

	// If not in cache, try to load from instrument data file
	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()

	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return "", "", fmt.Errorf("failed to read instrument data: %w", err)
	}

	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return "", "", fmt.Errorf("failed to unmarshal instrument data: %w", err)
	}

	// Look for the instrument token in the list
	for _, inst := range instrumentList.Instruments {
		if inst.InstrumentToken == instrumentToken {
			// Found it - return the details
			return inst.Tradingsymbol, inst.Exchange, nil
		}
	}

	return "", "", fmt.Errorf("instrument token not found: %s", instrumentToken)
}

// CreateLookUpforFileStore creates a lookup map for index tokens vs Index name for lookup during websocke
func (k *KiteConnect) CreateLookUpforStoringFileFromWebsocketsAndAlsoStrikes(ctx context.Context) {
	log := logger.L()
	log.Info("Initializing lookup maps for File Store", nil)

	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()
	cache := cache.GetInMemoryCacheInstance()

	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return
	}

	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}
	// Process each instrument
	for _, inst := range instrumentList.Instruments {
		// Skip if not an option
		if inst.InstrumentType != "CE" && inst.InstrumentType != "PE" {
			continue
		}
		// Cache keys for instrument metadata
		strike_key := fmt.Sprintf("strike:%s", inst.InstrumentToken)
		expiry_key := fmt.Sprintf("expiry:%s", inst.InstrumentToken)
		instrument_type_key := fmt.Sprintf("instrument_type:%s", inst.InstrumentToken) // Corrected key
		strike, err := strconv.ParseFloat(inst.StrikePrice, 64)
		if err != nil {
			log.Error("Failed to parse strike price", map[string]interface{}{
				"error":        err.Error(),
				"strike_price": inst.StrikePrice,
				"symbol":       inst.Tradingsymbol,
			})
			continue
		}
		strikeStr := fmt.Sprintf("%d", int(strike))
		next_move_lookup_key := fmt.Sprintf("next_move:%s:%s:%s", strikeStr, inst.InstrumentType, inst.Expiry)
		log.Debug("Looking up instrument token", map[string]interface{}{
			"key":         next_move_lookup_key,
			"strike":      strikeStr,
			"option_type": inst.InstrumentType,
			"expiry":      inst.Expiry,
		})
		if slices.Contains(core.GetIndices().GetAllNames(), inst.Name) {
			// Cache the index name for this instrument token with prefix
			instrumentNameKey := fmt.Sprintf("instrument_name_key:%s", inst.InstrumentToken)
			cache.Set(instrumentNameKey, inst.Name, 7*24*time.Hour)

			// Also store with direct key format for backward compatibility
			cache.Set(inst.InstrumentToken, inst.Name, 7*24*time.Hour)

			// Cache strike price
			cache.Set(strike_key, inst.StrikePrice, 7*24*time.Hour)

			// Cache instrument type
			cache.Set(instrument_type_key, inst.InstrumentType, 7*24*time.Hour)

			// Cache expiry date
			if inst.Expiry != "" {
				cache.Set(expiry_key, inst.Expiry, 7*24*time.Hour)
			}
			cache.Set(next_move_lookup_key, inst.InstrumentToken, 7*24*time.Hour)

			// Cache reverse lookup (trading symbol to instrument token)
			cache.Set(inst.Tradingsymbol, inst.InstrumentToken, 7*24*time.Hour)
			cache.Set(instrumentNameKey, inst.Name, 7*24*time.Hour)
			continue
		}
	}

	for _, index := range core.GetIndices().GetIndicesToSubscribeForIntraday() {
		// Store with prefix format
		instrumentNameKey := fmt.Sprintf("instrument_name_key:%s", index.InstrumentToken)
		cache.Set(instrumentNameKey, index.NameInOptions, 7*24*time.Hour)

		// Also store with direct key format for backward compatibility
		cache.Set(index.InstrumentToken, index.NameInOptions, 7*24*time.Hour)

		log.Debug("Cached index lookup", map[string]interface{}{
			"token": index.InstrumentToken,
			"name":  index.NameInOptions,
		})
	}
}

// CreateLookupMapWithExpiryVSTokenMap extracts instrument tokens and creates a reverse lookup map
func (k *KiteConnect) CreateLookupMapWithExpiryVSTokenMap(ctx context.Context) (map[string]string, map[string]TokenInfo, map[string]string) {
	once.Do(func() {
		log := logger.L()
		log.Info("Initializing lookup maps", nil)
		// Get the full instrument map
		instrumentMap, err := k.GetInstrumentExpirySymbolMap(ctx)
		if err != nil {
			log.Error("Failed to get instrument map", map[string]interface{}{
				"error": err.Error(),
			})

		}
		// Initialize maps
		tokensCache = make(map[string]string)
		reverseLookupCache = make(map[string]TokenInfo)
		reverseLookupCacheWithStrike = make(map[string]string)

		// Add options (IsIndex = false)
		for _, expiryMap := range instrumentMap.Data {
			for expiry, options := range expiryMap {
				for _, call := range options.Calls {
					index := extractIndex(call.Symbol)
					targetFile := generateTargetFileName(expiry, index)
					tokensCache[call.InstrumentToken] = call.Symbol
					strikeWithAbbreviation := extractStrikeAndType(call.Symbol, call.Strike)
					reverseLookupCache[call.InstrumentToken] = TokenInfo{
						Expiry:                 expiry,
						Symbol:                 call.Symbol,
						Index:                  index,
						IsIndex:                false,
						TargetFile:             targetFile,
						StrikeWithAbbreviation: strikeWithAbbreviation,
					}
					reverseLookupCacheWithStrike[strikeWithAbbreviation] = call.InstrumentToken
				}
				for _, put := range options.Puts {
					index := extractIndex(put.Symbol)
					targetFile := generateTargetFileName(expiry, index)
					tokensCache[put.InstrumentToken] = put.Symbol
					strikeWithAbbreviation := extractStrikeAndType(put.Symbol, put.Strike)
					reverseLookupCache[put.InstrumentToken] = TokenInfo{
						Expiry:                 expiry,
						Symbol:                 put.Symbol,
						Index:                  index,
						IsIndex:                false,
						TargetFile:             targetFile,
						StrikeWithAbbreviation: strikeWithAbbreviation,
					}
					reverseLookupCacheWithStrike[strikeWithAbbreviation] = put.InstrumentToken
				}
			}
		}

		// Add indices
		indexTokens := k.GetIndexTokens()
		for name, token := range indexTokens {
			targetFile := generateTargetFileName(time.Time{}, name)
			tokensCache[token] = name
			reverseLookupCache[token] = TokenInfo{
				Expiry:     time.Time{},
				Symbol:     name,
				Index:      name,
				IsIndex:    true,
				TargetFile: targetFile,
			}
		}

		log.Info("Lookup maps initialized", map[string]interface{}{
			"tokens_cache_size":   len(tokensCache),
			"reverse_lookup_size": len(reverseLookupCache),
			"strike_lookup_size":  len(reverseLookupCacheWithStrike),
		})

		for strike, token := range reverseLookupCacheWithStrike {
			fmt.Println(strike, token)
		}
	})
	k.PrintStrikeCache()
	return tokensCache, reverseLookupCache, reverseLookupCacheWithStrike
}

func generateTargetFileName(expiry time.Time, index string) string {
	currentDate := utils.NowIST().Format("020106") // ddmmyy format

	// For indices (when expiry is zero time)
	if expiry.IsZero() {
		return fmt.Sprintf("%s_%s.parquet", index, currentDate)
	}

	// For options
	expiryDate := expiry.Format("020106") // ddmmyy format
	return fmt.Sprintf("%s_%s_%s.parquet", index, expiryDate, currentDate)
}

func convertExpiryMapToSortedSlices(expiryMap map[string]map[time.Time]bool) map[string][]time.Time {
	result := make(map[string][]time.Time)

	for name, uniqueExpiries := range expiryMap {
		expiries := make([]time.Time, 0, len(uniqueExpiries))
		for expiry := range uniqueExpiries {
			expiries = append(expiries, expiry)
		}

		// Sort expiries
		sort.Slice(expiries, func(i, j int) bool {
			return expiries[i].Before(expiries[j])
		})

		result[name] = expiries
	}

	return result
}

func filterInstruments(allInstruments []kiteconnect.Instrument, targetNames []core.Index) []kiteconnect.Instrument {
	filtered := make([]kiteconnect.Instrument, 0)
	for _, inst := range allInstruments {
		for _, name := range targetNames {
			if inst.Name == name.NameInOptions || inst.Name == name.NameInIndices {
				filtered = append(filtered, inst)
				break
			}
		}
	}
	return filtered
}

func (k *KiteConnect) saveInstrumentsToFile(instruments []kiteconnect.Instrument) error {
	log := logger.L()
	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()

	// Convert to proto message
	data, err := proto.Marshal(convertToProtoInstruments(instruments))
	if err != nil {
		log.Error("Failed to marshal instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save using filestore
	if err := fileStore.SaveGzippedProto("instruments", currentDate, data); err != nil {
		log.Error("Failed to save instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	return nil
}

func convertToProtoInstruments(instruments []kiteconnect.Instrument) *InstrumentList {
	protoInstruments := make([]*Instrument, 0, len(instruments))

	for _, inst := range instruments {
		protoInst := &Instrument{
			InstrumentToken: fmt.Sprintf("%d", inst.InstrumentToken),
			ExchangeToken:   fmt.Sprintf("%d", inst.ExchangeToken),
			Tradingsymbol:   inst.Tradingsymbol,
			Name:            inst.Name,
			LastPrice:       fmt.Sprintf("%.2f", inst.LastPrice),
			Expiry:          inst.Expiry.Time.Format("02-01-2006"),
			StrikePrice:     fmt.Sprintf("%.2f", inst.StrikePrice),
			TickSize:        fmt.Sprintf("%.2f", inst.TickSize),
			LotSize:         fmt.Sprintf("%d", int(inst.LotSize)),
			InstrumentType:  inst.InstrumentType,
			Segment:         inst.Segment,
			Exchange:        inst.Exchange,
		}
		protoInstruments = append(protoInstruments, protoInst)
	}

	return &InstrumentList{
		Instruments: protoInstruments,
	}
}

// GetInstrumentExpirySymbolMap reads instrument data and organizes trading symbols
func (k *KiteConnect) GetInstrumentExpirySymbolMap(ctx context.Context) (*InstrumentExpiryMap, error) {
	log := logger.L()
	currentDate := utils.NowIST().Format("02-01-2006")
	fileStore := filestore.NewDiskFileStore()

	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return nil, err
	}

	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	result := &InstrumentExpiryMap{
		Data: make(map[string]map[time.Time]ExpiryOptions),
	}

	// Process each instrument
	for _, inst := range instrumentList.Instruments {
		// Skip if not an option
		if inst.InstrumentType != "CE" && inst.InstrumentType != "PE" {
			continue
		}

		// Parse expiry date
		expiry, err := time.Parse("02-01-2006", inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date", map[string]interface{}{
				"error":         err.Error(),
				"tradingsymbol": inst.Tradingsymbol,
			})
			continue
		}

		// Initialize maps if needed
		if result.Data[inst.Name] == nil {
			result.Data[inst.Name] = make(map[time.Time]ExpiryOptions)
		}
		if _, exists := result.Data[inst.Name][expiry]; !exists {
			result.Data[inst.Name][expiry] = ExpiryOptions{
				Calls: make([]OptionTokenPair, 0),
				Puts:  make([]OptionTokenPair, 0),
			}
		}

		// Create option pair
		pair := OptionTokenPair{
			Symbol:          inst.Tradingsymbol,
			InstrumentToken: inst.InstrumentToken,
			Strike:          inst.StrikePrice,
		}

		// Add to appropriate slice based on option type
		options := result.Data[inst.Name][expiry]
		if inst.InstrumentType == "CE" {
			options.Calls = append(options.Calls, pair)
		} else {
			options.Puts = append(options.Puts, pair)
		}
		result.Data[inst.Name][expiry] = options
	}

	// Sort the options for each expiry
	for instrument := range result.Data {
		for expiry := range result.Data[instrument] {
			options := result.Data[instrument][expiry]

			// Sort calls
			sort.Slice(options.Calls, func(i, j int) bool {
				return options.Calls[i].Symbol < options.Calls[j].Symbol
			})

			// Sort puts
			sort.Slice(options.Puts, func(i, j int) bool {
				return options.Puts[i].Symbol < options.Puts[j].Symbol
			})

			result.Data[instrument][expiry] = options
		}
	}

	return result, nil
}

func countOptions(m *InstrumentExpiryMap) map[string]map[string]int {
	counts := make(map[string]map[string]int)
	for inst := range m.Data {
		counts[inst] = make(map[string]int)
		for _, options := range m.Data[inst] {
			counts[inst]["calls"] += len(options.Calls)
			counts[inst]["puts"] += len(options.Puts)
		}
	}
	return counts
}

// Helper function to format sample data
func formatSampleData(m *InstrumentExpiryMap) map[string]map[string]interface{} {
	sample := make(map[string]map[string]interface{})

	for inst, expiryMap := range m.Data {
		sample[inst] = make(map[string]interface{})

		for expiry, options := range expiryMap {
			dateKey := expiry.Format("2006-01-02")
			sample[inst][dateKey] = map[string]interface{}{
				"calls_count":  len(options.Calls),
				"puts_count":   len(options.Puts),
				"sample_calls": formatOptionSample(options.Calls, 3),
				"sample_puts":  formatOptionSample(options.Puts, 3),
			}
		}
	}
	return sample
}

func formatOptionSample(options []OptionTokenPair, limit int) []map[string]string {
	if len(options) == 0 {
		return nil
	}

	if limit > len(options) {
		limit = len(options)
	}

	sample := make([]map[string]string, limit)
	for i := 0; i < limit; i++ {
		sample[i] = map[string]string{
			"symbol": options[i].Symbol,
			"token":  options[i].InstrumentToken,
		}
	}
	return sample
}

func (k *KiteConnect) GetUpcomingExpiryTokens(ctx context.Context, indices []string) ([]string, error) {
	log := logger.L()

	// Get the full instrument map
	symbolMap, err := k.GetInstrumentExpirySymbolMap(ctx)
	if err != nil {
		log.Error("Failed to get instrument map", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// // Get token lookup maps (using cached version)
	// _, _, _ = k.CreateLookupMapWithExpiryVSTokenMap(ctx)

	// Find upcoming expiry
	now := utils.NowIST().Truncate(24 * time.Hour)
	upcomingExpiry := make(map[string]time.Time)

	for _, index := range indices {
		if expiryMap, exists := symbolMap.Data[index]; exists {
			var minExpiry time.Time
			for expiry := range expiryMap {
				normalizedExpiry := expiry.Truncate(24 * time.Hour)
				if normalizedExpiry.After(now) || normalizedExpiry.Equal(now) {
					if minExpiry.IsZero() || normalizedExpiry.Before(minExpiry) {
						minExpiry = normalizedExpiry
					}
				}
			}
			if !minExpiry.IsZero() {
				upcomingExpiry[index] = minExpiry
			}
		}
	}

	// Collect tokens for upcoming expiry
	tokens := make([]string, 0)
	for token, info := range reverseLookupCache {
		for _, index := range indices {
			if expiry, exists := upcomingExpiry[index]; exists {
				if info.Expiry.Equal(expiry) {
					tokens = append(tokens, token)
				}
			}
		}
	}

	log.Info("Retrieved upcoming expiry tokens", map[string]interface{}{
		"indices":      indices,
		"tokens_count": len(tokens),
	})

	return tokens, nil
}

func extractIndex(symbol string) string {
	// Common indices
	indices := []string{"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX"}

	for _, index := range indices {
		if strings.HasPrefix(symbol, index) {
			return index
		}
	}
	return "UNKNOWN"
}

// GetIndexTokens returns a map of index names to their instrument tokens
func (k *KiteConnect) GetIndexTokens() map[string]string {
	indices := core.GetIndices()
	tokens := make(map[string]string)

	// Use NameInIndices as key since that's what Zerodha API uses
	for _, index := range indices.GetAllIndices() {
		tokens[index.NameInIndices] = index.InstrumentToken
	}
	return tokens
}

func (k *KiteConnect) GetIndexVsExpiryMap(ctx context.Context) (map[string][]time.Time, error) {
	log := logger.L()

	// Read expiries from file
	expiries, err := k.readInstrumentExpiriesFromFile(ctx)
	if err != nil {
		log.Error("Failed to read expiries from file", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Filter only for indices we're interested in
	indices := []string{"NIFTY", "SENSEX"}
	filteredMap := make(map[string][]time.Time)

	for index, dates := range expiries {
		// Only include if it's one of our target indices
		for _, targetIndex := range indices {
			if index == targetIndex {
				// Filter only future expiries
				now := utils.NowIST().Truncate(24 * time.Hour)
				futureExpiries := make([]time.Time, 0)

				for _, date := range dates {
					if date.Equal(now) || date.After(now) {
						futureExpiries = append(futureExpiries, date)
					}
				}

				if len(futureExpiries) > 0 {
					filteredMap[index] = futureExpiries
				}
				break
			}
		}
	}

	log.Info("Retrieved index expiry mapping", map[string]interface{}{
		"indices_count": len(filteredMap),
		"mapping":       formatExpiryMap(filteredMap),
	})

	return filteredMap, nil
}

// Helper function to format expiry map for logging
func formatExpiryMap(m map[string][]time.Time) map[string][]string {
	formatted := make(map[string][]string)
	for index, dates := range m {
		formatted[index] = make([]string, len(dates))
		for i, date := range dates {
			formatted[index][i] = date.Format("2006-01-02")
		}
	}
	return formatted
}

// PrintStrikeCache prints the contents of reverseLookupCacheWithStrike
func (k *KiteConnect) PrintStrikeCache() {
	log := logger.L()

	instrumentMutex.RLock()
	defer instrumentMutex.RUnlock()

	log.Info("Strike Cache Contents", map[string]interface{}{
		"total_entries": len(reverseLookupCacheWithStrike),
	})

	// Sort the keys for better readability
	keys := make([]string, 0, len(reverseLookupCacheWithStrike))
	for k := range reverseLookupCacheWithStrike {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Print each entry
	for _, strike := range keys {
		token := reverseLookupCacheWithStrike[strike]
		log.Info("Strike Entry", map[string]interface{}{
			"strike": strike,
			"token":  token,
		})
	}
}

func (k *KiteConnect) GetIndexNameFromToken(ctx context.Context, instrumentToken string) (string, error) {
	cache := cache.GetInMemoryCacheInstance()

	// Get index name from cache
	instrumentNameKey := fmt.Sprintf("instrument_name_key:%s", instrumentToken)
	indexName, exists := cache.Get(instrumentNameKey)
	if !exists {
		return "", fmt.Errorf("no index found for instrument token: %s", instrumentToken)
	}

	// Type assert the interface{} to string
	indexNameStr, ok := indexName.(string)
	if !ok {
		return "", fmt.Errorf("invalid cache value type for instrument token: %s", instrumentToken)
	}

	return indexNameStr, nil
}
