package zerodha

import (
	"context"
	"fmt"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/filestore"
	"gohustle/logger"
	"gohustle/utils"
	"math"
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
	reverseLookupCacheWithStrike map[string]string
	reverseLookupCache           map[string]TokenInfo
	instrumentMutex              sync.RWMutex
	instrumentsCache             cache.RedisCache

	// Minimum OI threshold for filtering instruments
	MinimumOIForInstrument = 0.00 // Set an appropriate threshold value
)

// DownloadInstrumentData downloads and saves instrument data
func (k *KiteConnect) DownloadInstrumentData(ctx context.Context, instrumentNames []core.Index) error {
	// Use the singleton instance of InstrumentsFileHandler
	handler := GetInstrumentsFileHandler(k.Kite)
	return handler.DownloadInstrumentDataFromZerodha(ctx, instrumentNames)
}

func (k *KiteConnect) GetTentativeATMBasedonLTP(index core.Index, strikes []string) string {
	log := logger.L()

	// Enhanced logging for debugging
	log.Debug("Getting ATM strike based on LTP", map[string]interface{}{
		"index_name":        index.NameInOptions,
		"instrument_token":  index.InstrumentToken,
		"available_strikes": len(strikes),
	})

	redisCache, err := cache.GetRedisCache()
	if err != nil {
		log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
			"index": index.NameInOptions,
		})
		return ""
	}

	ltpDB := redisCache.GetLTPDB3()
	if ltpDB == nil {
		log.Error("Failed to get LTP DB", map[string]interface{}{
			"error": "LTP DB is nil",
			"index": index.NameInOptions,
		})
		return ""
	}

	ltpKey := fmt.Sprintf("%d_ltp", index.InstrumentToken)
	ltp, err := ltpDB.Get(context.Background(), ltpKey).Float64()
	if err != nil {
		log.Error("Failed to get LTP for index", map[string]interface{}{
			"error":   err.Error(),
			"index":   index.NameInOptions,
			"ltp_key": ltpKey,
			"token":   index.InstrumentToken,
		})
		return ""
	}

	// Find nearest strike to LTP
	var nearestStrike string
	minDiff := math.MaxFloat64

	// Log the first few strikes for debugging
	samples := strikes
	if len(strikes) > 5 {
		samples = strikes[:5]
	}
	log.Debug("Finding nearest strike to LTP", map[string]interface{}{
		"index":          index.NameInOptions,
		"ltp":            ltp,
		"strikes_count":  len(strikes),
		"sample_strikes": samples,
	})

	for _, strike := range strikes {
		strikePrice, err := strconv.ParseFloat(strike, 64)
		if err != nil {
			log.Error("Failed to parse strike price", map[string]interface{}{
				"error":  err.Error(),
				"strike": strike,
				"index":  index.NameInOptions,
			})
			continue
		}

		diff := math.Abs(ltp - strikePrice)
		if diff < minDiff {
			minDiff = diff
			nearestStrike = strike
		}
	}

	log.Debug("Selected ATM strike", map[string]interface{}{
		"index":        index.NameInOptions,
		"ltp":          ltp,
		"nearest_diff": minDiff,
		"atm_strike":   nearestStrike,
	})

	return nearestStrike
}

func (k *KiteConnect) SyncAllInstrumentDataToCache(ctx context.Context) error {
	log := logger.L()
	log.Info("Starting comprehensive instrument data sync", nil)

	// 1. Read instrument data from file
	currentDate := utils.GetCurrentKiteDate()
	fileStore := filestore.NewDiskFileStore()

	data, err := fileStore.ReadGzippedProto("instruments", currentDate)
	if err != nil {
		log.Error("Failed to read instrument data", map[string]interface{}{
			"error": err.Error(),
			"date":  currentDate,
		})
		return err
	}

	// 2. Unmarshal the protobuf data
	instrumentList := &InstrumentList{}
	if err := proto.Unmarshal(data, instrumentList); err != nil {
		log.Error("Failed to unmarshal instrument data", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// 3. Process expiry dates
	expiryMap := make(map[string]map[time.Time]bool)

	// 4. First pass: collect all expiries
	for _, inst := range instrumentList.Instruments {
		// Parse expiry date
		expiry, err := utils.ParseKiteDate(inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date", map[string]interface{}{
				"error":         err.Error(),
				"instrument":    inst.Name,
				"tradingsymbol": inst.Tradingsymbol,
				"expiry":        inst.Expiry,
			})
			continue
		}

		// Normalize index name to avoid duplicates like NIFTY 50/NIFTY and NIFTY BANK/BANKNIFTY
		normalizedName := normalizeIndexName(inst.Name)

		// Add expiry to map using normalized name
		if expiryMap[normalizedName] == nil {
			expiryMap[normalizedName] = make(map[time.Time]bool)
		}
		expiryMap[normalizedName][expiry] = true
	}

	// 6. Convert expiry map to sorted slices and limit to MaxExpiriesToProcess
	sortedExpiryMap := make(map[string][]time.Time)

	// Process expiry maps to get the limited set of valid expiries
	// and prepare for instrument filtering
	validExpiries := make(map[string]map[time.Time]bool)

	// Create a map to collect unique strikes for each index and expiry
	// Structure: map[indexName]map[expiry]map[strike]bool
	indexStrikeMap := make(map[string]map[string]map[string]bool)

	for name, dates := range expiryMap {
		expiries := make([]time.Time, 0, len(dates))
		for date := range dates {
			expiries = append(expiries, date)
		}

		// Get today's date for filtering
		today := utils.GetTodayIST()

		// Filter out dates before today
		futureExpiries := make([]time.Time, 0, len(expiries))
		for _, date := range expiries {
			if !date.Before(today) {
				futureExpiries = append(futureExpiries, date)
			}
		}
		expiries = futureExpiries

		// Sort expiries
		sort.Slice(expiries, func(i, j int) bool {
			return expiries[i].Before(expiries[j])
		})

		// Limit to MaxExpiriesToProcess nearest expiries
		if len(expiries) > cache.MaxExpiriesToProcess {
			expiries = expiries[:cache.MaxExpiriesToProcess]
		}

		// Initialize the valid expiries map for this instrument
		validExpiries[name] = make(map[time.Time]bool)
		for _, expiry := range expiries {
			validExpiries[name][expiry] = true
		}

		sortedExpiryMap[name] = expiries

		log.Info("Limited expiries for processing", map[string]interface{}{
			"instrument":         name,
			"total_expiries":     len(dates),
			"processed_expiries": len(expiries),
			"valid_expiries":     validExpiries,
		})
	}

	// 5. Filter instruments to only include those with the nearest expiries
	filteredInstrumentData := make([]cache.InstrumentData, 0)
	processedCount := 0
	skippedCount := 0

	for _, inst := range instrumentList.Instruments {
		// Parse expiry date to check if it's in our valid list
		expiry, err := utils.ParseKiteDate(inst.Expiry)
		if err != nil {
			log.Error("Failed to parse expiry date for filtering", map[string]interface{}{
				"error":         err.Error(),
				"instrument":    inst.Name,
				"tradingsymbol": inst.Tradingsymbol,
				"expiry":        inst.Expiry,
			})
			continue
		}

		// Skip if this expiry is not in our valid list for this instrument
		if validExpiries[inst.Name] == nil || !validExpiries[inst.Name][expiry] {
			skippedCount++
			continue
		}

		// Add filtered instrument data for caching
		filteredInstrumentData = append(filteredInstrumentData, cache.InstrumentData{
			Name:            *core.GetIndices().GetIndexByName(inst.Name),
			TradingSymbol:   inst.Tradingsymbol,
			InstrumentType:  cache.InstrumentType(inst.InstrumentType),
			StrikePrice:     inst.StrikePrice,
			Expiry:          inst.Expiry,
			Exchange:        inst.Exchange,
			Token:           inst.InstrumentToken,
			InstrumentToken: utils.StringToUint32(inst.InstrumentToken),
		})
		processedCount++

		// Collect strike prices for each index and expiry
		// Only collect for CE and PE option types
		if inst.InstrumentType == "CE" || inst.InstrumentType == "PE" {
			// Format expiry date for consistent key format
			expiryStr := utils.FormatKiteDate(utils.ToIST(expiry))

			// Initialize maps if needed
			if indexStrikeMap[inst.Name] == nil {
				indexStrikeMap[inst.Name] = make(map[string]map[string]bool)
			}
			if indexStrikeMap[inst.Name][expiryStr] == nil {
				indexStrikeMap[inst.Name][expiryStr] = make(map[string]bool)
			}

			// Convert strike price to integer by removing decimal places
			strikePriceFloat, err := strconv.ParseFloat(inst.StrikePrice, 64)
			if err != nil {
				log.Error("Failed to parse strike price", map[string]interface{}{
					"error":  err.Error(),
					"strike": inst.StrikePrice,
				})
				continue
			}

			// Convert to integer strike price (remove decimal places)
			integerStrike := fmt.Sprintf("%d", int(strikePriceFloat))

			// Add integer strike price to the map
			indexStrikeMap[inst.Name][expiryStr][integerStrike] = true
		}
	}

	// Log the filtering results
	log.Info("Filtered instruments by expiry", map[string]interface{}{
		"total_instruments": len(instrumentList.Instruments),
		"processed_count":   processedCount,
		"skipped_count":     skippedCount,
	})

	// sortedExpiryMap and validExpiries are already processed above

	// 7. Get CacheMeta singleton instance
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// 8. Sync expiries to cache
	if err := cacheMeta.SyncInstrumentExpiries(ctx, sortedExpiryMap); err != nil {
		log.Error("Failed to sync instrument expiries", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// 10. Store unique strikes for each expiry
	if err := cacheMeta.StoreExpiryStrikes(ctx, indexStrikeMap); err != nil {
		log.Error("Failed to store expiry strikes", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't return error here, continue with the rest of the function
		// This is a new feature and we don't want it to block existing functionality
	} else {
		log.Info("Successfully stored expiry strikes", map[string]interface{}{
			"indices_count": len(indexStrikeMap),
		})
	}

	// To sync only the expiry strikes with symbols
	if err := cacheMeta.SyncInstrumentExpiryStrikesWithSymbols(ctx, filteredInstrumentData); err != nil {
		log.Error("Failed to sync instrument expiry strikes with symbols", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	instrumentsFilteredByOI, err := k.GetFilteredInstrumentsBasedOnOI(ctx)
	if err != nil {
		log.Error("Failed to get filtered instruments based on OI", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// 9. Sync instrument metadata to cache
	if err := cacheMeta.SyncInstrumentMetadata(ctx, instrumentsFilteredByOI); err != nil {
		log.Error("Failed to sync instrument metadata", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	log.Info("Successfully synced all instrument data to cache", nil)
	return nil
}

// Iterate through Indices list
// Get upcoming Expiries
// fetch InstrumentExpiryStrikesWithExchangeAndInstrumentSymbol data from cache, through a helper method in cache_meta
// response witll have comma separated strings like this strike:40500||ce:BANKNIFTY25MAY40500CE||pe:BANKNIFTY25MAY40500PE||exchange:NFO
// from above, populate instrument_string to GetQuotes. ce:{symbol}, pe:{symbol} is there above. create query with structure for get quotes {exchange:symbol}
// GetQuotes will return response with oi for each instrument
// Filter OI with defined values.
// For each instumentsymbol, fetch theInstruments from the file.
// Return those instruments.
func (k *KiteConnect) GetFilteredInstrumentsBasedOnOI(ctx context.Context) ([]cache.InstrumentData, error) {
	log := logger.L()
	log.Info("Getting filtered instruments based on OI", nil)
	instrumentListToReturn := make([]cache.InstrumentData, 0)

	// List of indices to process
	indices := core.GetIndices().GetIndicesToSubscribeForIntraday()
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	instrumentsFileHandler := GetInstrumentsFileHandler(k.Kite)

	for _, index := range indices {
		log.Info("Processing index", map[string]interface{}{
			"index": index.NameInOptions,
		})

		// Get upcoming expiries
		upcomingExpiry, err := cacheMeta.GetNearestExpiry(ctx, index.NameInOptions)
		if err != nil {
			log.Error("Failed to get upcoming expiry", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}

		instumentSymbolListForGetQuotes := make([]string, 0)
		instrumentTokens := make([]string, 0)
		// Get strikes for each expiry
		for _, expiry := range []string{upcomingExpiry} {
			log.Info("Processing expiry", map[string]interface{}{
				"expiry": expiry,
			})
			instrumentSymbolList, err := cacheMeta.GetInstrumentExpiryStrikesWithExchangeAndInstrumentSymbol(ctx, index.NameInOptions, expiry)
			if err != nil {
				log.Error("Failed to get instrument expiry strikes with exchange and instrument symbol", map[string]interface{}{
					"error": err.Error(),
				})
				return nil, err
			}

			for _, instrumentSymbol := range instrumentSymbolList {
				instumentSymbolListForGetQuotes = append(instumentSymbolListForGetQuotes, extractQuotesQuerySymbol(instrumentSymbol)...)
			}

			log.Debug("List to be passed to GetQuotes", map[string]interface{}{
				"list": instumentSymbolListForGetQuotes,
			})

			quotes, err := k.Kite.GetQuote(instumentSymbolListForGetQuotes...)
			if err != nil {
				log.Error("Failed to get quotes", map[string]interface{}{
					"error": err.Error(),
				})
				return nil, err
			}
			log.Debug("Quotes fetched successfully", map[string]interface{}{
				"expiry":                       expiry,
				"quotes":                       quotes,
				"length":                       len(quotes),
				"lengthOfInstrumentSymbolList": len(instrumentSymbolList),
			})

			// Create maps to store instrument symbols with their OI values
			instrumentOIMap := make(map[string]float64)
			tokenOIMap := make(map[string]float64) // Map for storing token -> OI for Redis

			// Process each quote and extract OI information
			for symbol, quote := range quotes {
				// Extract the instrument symbol from the full symbol (e.g., "NFO:NIFTY2551520650CE" -> "NIFTY2551520650CE")
				parts := strings.Split(symbol, ":")
				if len(parts) != 2 {
					log.Error("Invalid symbol format", map[string]interface{}{"symbol": symbol})
					continue
				}
				instrumentSymbol := parts[1]

				// Get the OI value from the quote
				oi := quote.OI

				// Store token and OI in the map for Redis
				tokenStr := strconv.Itoa(quote.InstrumentToken)
				tokenOIMap[tokenStr] = oi

				log.Debug("Quote OI information", map[string]interface{}{
					"symbol": instrumentSymbol,
					"token":  tokenStr,
					"oi":     oi,
				})

				// Only include instruments with OI above the threshold
				if oi >= MinimumOIForInstrument {
					// Store the instrument symbol with its OI value
					instrumentOIMap[instrumentSymbol] = oi
					instrumentTokens = append(instrumentTokens, tokenStr)
				}
			}

			// Store OI data in Redis
			if err := cacheMeta.StoreInstrumentOIData(ctx, tokenOIMap); err != nil {
				log.Error("Failed to store OI data in Redis", map[string]interface{}{
					"error": err.Error(),
				})
				// Continue execution even if storing OI data fails
			}

			// Sort instruments by OI in descending order
			type instrumentOI struct {
				Symbol string
				OI     float64
			}

			sortedInstruments := make([]instrumentOI, 0, len(instrumentOIMap))
			for symbol, oi := range instrumentOIMap {
				sortedInstruments = append(sortedInstruments, instrumentOI{Symbol: symbol, OI: oi})
			}

			// Sort by OI in descending order
			sort.Slice(sortedInstruments, func(i, j int) bool {
				return sortedInstruments[i].OI > sortedInstruments[j].OI
			})

			// Extract the filtered instruments
			filteredInstruments := make([]string, 0)
			for _, inst := range sortedInstruments {
				filteredInstruments = append(filteredInstruments, inst.Symbol)
				// Log the filtered instrument and its OI
				log.Debug("Filtered instrument based on OI", map[string]interface{}{
					"symbol": inst.Symbol,
					"oi":     inst.OI,
				})
			}

			log.Debug("Filtered instruments based on OI", map[string]interface{}{
				"count":       len(filteredInstruments),
				"threshold":   MinimumOIForInstrument,
				"instruments": filteredInstruments,
			})
			instrumentList := make([]cache.InstrumentData, 0)
			// Return the filtered instruments if we have any
			if len(filteredInstruments) > 0 {
				instruments, err := instrumentsFileHandler.FetchInstrumentsFromFilteredList(ctx, filteredInstruments)
				if err != nil {
					log.Error("Failed to fetch instruments from filtered list", map[string]interface{}{
						"error": err.Error(),
					})
					return nil, err
				}
				log.Debug("Fetched instruments from filtered list", map[string]interface{}{
					"count": len(instruments),
				})

				for _, instrument := range instruments {
					log.Debug("Fetched instrument", map[string]interface{}{
						"symbol": instrument.Tradingsymbol,
					})
					instrumentList = append(instrumentList, cache.InstrumentData{
						Name:           *core.GetIndices().GetIndexByName(instrument.Name),
						TradingSymbol:  instrument.Tradingsymbol,
						InstrumentType: cache.InstrumentType(instrument.InstrumentType),
						StrikePrice:    instrument.StrikePrice,
						Expiry:         instrument.Expiry,
						Exchange:       instrument.Exchange,
						Token:          instrument.InstrumentToken,
					})
				}
			}
			instrumentListToReturn = append(instrumentListToReturn, instrumentList...)
			cacheMeta.StoreFilteredInstrumentTokens(ctx, index.NameInOptions, upcomingExpiry, instrumentTokens)
		}
	}

	return instrumentListToReturn, nil
}

func extractQuotesQuerySymbol(input string) []string {
	var result []string

	parts := strings.Split(input, "||")

	var exchange string
	var ceSymbol string
	var peSymbol string

	for _, part := range parts {
		if strings.HasPrefix(part, "exchange:") {
			exchange = strings.Split(part, ":")[1]
		} else if strings.HasPrefix(part, "ce:") {
			ceSymbol = strings.Split(part, ":")[1]
		} else if strings.HasPrefix(part, "pe:") {
			peSymbol = strings.Split(part, ":")[1]
		}
	}

	if exchange != "" {
		if ceSymbol != "" {
			result = append(result, exchange+":"+ceSymbol)
		}
		if peSymbol != "" {
			result = append(result, exchange+":"+peSymbol)
		}
	}

	return result
}

// GetUpcomingExpiryTokensForIndices returns instrument tokens for options of upcoming expiries
// with optional filtering based on open interest
func (k *KiteConnect) GetUpcomingExpiryTokensForIndices(ctx context.Context, indices []core.Index) ([]uint32, error) {
	log := logger.L()
	tokens := make([]uint32, 0)

	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	for _, index := range indices {
		// Get nearest expiry for this index from Redis cache
		nearestExpiry, err := cacheMeta.GetNearestExpiry(ctx, index.NameInOptions)
		if err != nil {
			log.Error("No nearest expiry found in Redis cache for index", map[string]interface{}{
				"index": index.NameInOptions,
				"error": err.Error(),
			})
			continue
		}

		// Get all CE and PE tokens for this index and expiry
		expiryTokens, err := cacheMeta.GetExpiryStrikesWithExchangeAndInstrumentSymbol(ctx, index.NameInOptions, nearestExpiry)
		if err != nil {
			log.Error("Failed to get tokens for index and expiry", map[string]interface{}{
				"index":  index.NameInOptions,
				"expiry": nearestExpiry,
				"error":  err.Error(),
			})
			continue
		}

		// Add tokens to result
		log.Info("Adding tokens to result", map[string]interface{}{
			"index":           index.NameInOptions,
			"expiry":          nearestExpiry,
			"existing_tokens": len(tokens),
			"to_be_added":     len(expiryTokens),
		})
		tokens = append(expiryTokens, tokens...)
	}

	return tokens, nil
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// normalizeIndexName converts various index name formats to a standard format
// to avoid duplicate entries in the expiry map
func normalizeIndexName(indexName string) string {
	// Map of index names in indices format to their option format
	indexMap := map[string]string{
		"NIFTY 50":   "NIFTY",
		"NIFTY BANK": "BANKNIFTY",
		"SENSEX":     "SENSEX",
	}

	// Check if the name is already in the standard format
	if indexName == "NIFTY" || indexName == "BANKNIFTY" || indexName == "SENSEX" {
		return indexName
	}

	// Check if we have a mapping for this index name
	if standardName, exists := indexMap[indexName]; exists {
		return standardName
	}

	// If no mapping exists, return the original name
	return indexName
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

		// Format expiries for logging
		formattedExpiries := make([]string, 0, len(expiries))
		for _, expiry := range expiries {
			formattedExpiries = append(formattedExpiries, utils.FormatKiteDate(expiry))
		}

		logger.L().Info("Converted expiry map to sorted slices", map[string]interface{}{
			"instrument": name,
			"expiries":   formattedExpiries,
		})
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
	currentDate := utils.GetCurrentKiteDate()
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
		expiry, err := utils.ParseKiteDate(inst.Expiry)
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
func (k *KiteConnect) GetIndexTokens() map[string]uint32 {
	indices := core.GetIndices()
	tokens := make(map[string]uint32)

	// Use NameInIndices as key since that's what Zerodha API uses
	for _, index := range indices.GetAllIndices() {
		tokens[index.NameInIndices] = index.InstrumentToken
	}
	return tokens
}
