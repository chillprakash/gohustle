package zerodha

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	"google.golang.org/protobuf/proto"
)

// OptionTokenPair pairs a trading symbol with its instrument token
type OptionTokenPair struct {
	Symbol          string
	InstrumentToken string
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
	Expiry  time.Time
	Symbol  string
	Index   string
	IsIndex bool
}

var (
	tokensCache        map[string]string
	reverseLookupCache map[string]TokenInfo
	once               sync.Once
	instrumentMutex    sync.RWMutex // Package-level mutex
)

func (k *KiteConnect) GetInstrumentInfo(token string) (TokenInfo, bool) {
	instrumentMutex.RLock()
	defer instrumentMutex.RUnlock()

	info, exists := reverseLookupCache[token]
	return info, exists
}

// DownloadInstrumentData downloads and saves instrument data
func (k *KiteConnect) DownloadInstrumentData(ctx context.Context) error {
	log := logger.GetLogger()

	// Define instrument names we're interested in
	instrumentNames := []string{"NIFTY", "BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "BANKEX"}

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
	return k.saveInstrumentsToFile(ctx, filteredInstruments)
}

func (k *KiteConnect) SyncInstrumentExpiriesFromFileToDB(ctx context.Context) error {
	log := logger.GetLogger()

	// Read expiries from file
	expiries, err := k.readInstrumentExpiriesFromFile(ctx)
	if err != nil {
		log.Error("Failed to read expiries from file", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save expiries to DB
	if err := k.saveInstrumentExpiriesToDB(ctx, expiries); err != nil {
		log.Error("Failed to save expiries to DB", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	log.Info("Successfully synced expiries from file to DB", map[string]interface{}{
		"instruments_count": len(expiries),
	})

	return nil
}

// GetInstrumentExpiries reads the gzipped instrument data and returns expiry dates
func (k *KiteConnect) readInstrumentExpiriesFromFile(ctx context.Context) (map[string][]time.Time, error) {
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-2006")

	// Read the gzipped data
	data, err := k.fileStore.ReadGzippedProto("instruments", currentDate)
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

	// Process each instrument
	for _, inst := range instrumentList.Instruments {
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

		// Add expiry to map
		if expiryMap[inst.Name] == nil {
			expiryMap[inst.Name] = make(map[time.Time]bool)
		}
		expiryMap[inst.Name][expiry] = true
	}

	return convertExpiryMapToSortedSlices(expiryMap), nil
}

// CreateLookupMapWithExpiryVSTokenMap extracts instrument tokens and creates a reverse lookup map
func (k *KiteConnect) CreateLookupMapWithExpiryVSTokenMap(instrumentMap *InstrumentExpiryMap) (map[string]string, map[string]TokenInfo) {
	once.Do(func() {
		tokensCache = make(map[string]string)
		reverseLookupCache = make(map[string]TokenInfo)

		// Add options (IsIndex = false)
		for _, expiryMap := range instrumentMap.Data {
			for expiry, options := range expiryMap {
				for _, call := range options.Calls {
					tokensCache[call.InstrumentToken] = call.Symbol
					reverseLookupCache[call.InstrumentToken] = TokenInfo{
						Expiry:  expiry,
						Symbol:  call.Symbol,
						Index:   extractIndex(call.Symbol),
						IsIndex: false,
					}
				}
				for _, put := range options.Puts {
					tokensCache[put.InstrumentToken] = put.Symbol
					reverseLookupCache[put.InstrumentToken] = TokenInfo{
						Expiry:  expiry,
						Symbol:  put.Symbol,
						Index:   extractIndex(put.Symbol),
						IsIndex: false,
					}
				}
			}
		}

		// Add indices (IsIndex = true)
		indexTokens := k.GetIndexTokens()
		for name, token := range indexTokens {
			tokensCache[token] = name
			reverseLookupCache[token] = TokenInfo{
				Expiry:  time.Time{}, // zero time for indices
				Symbol:  name,
				Index:   name,
				IsIndex: true,
			}
		}
	})

	return tokensCache, reverseLookupCache
}

// SaveInstrumentExpiries saves the expiry dates to the database
func (k *KiteConnect) saveInstrumentExpiriesToDB(ctx context.Context, expiries map[string][]time.Time) error {
	log := logger.GetLogger()

	// First, mark all existing records as inactive
	deactivateQuery := `
		UPDATE instrument_expiries 
		SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP 
		WHERE is_active = TRUE
	`
	if _, err := k.db.Exec(ctx, deactivateQuery); err != nil {
		log.Error("Failed to deactivate existing expiries", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Insert new records
	insertQuery := `
		INSERT INTO instrument_expiries (instrument_name, expiry_date)
		VALUES ($1, $2)
		ON CONFLICT (instrument_name, expiry_date)
		DO UPDATE SET 
			is_active = TRUE,
			updated_at = CURRENT_TIMESTAMP
	`

	for instrument, dates := range expiries {
		for _, date := range dates {
			if _, err := k.db.Exec(ctx, insertQuery, instrument, date); err != nil {
				log.Error("Failed to insert expiry", map[string]interface{}{
					"error":      err.Error(),
					"instrument": instrument,
					"expiry":     date,
				})
				return err
			}
		}

		log.Info("Saved expiries for instrument", map[string]interface{}{
			"instrument": instrument,
			"count":      len(dates),
		})
	}

	return nil
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

// formatExpiries formats expiry dates for logging
func formatExpiries(expiries []time.Time) []string {
	formatted := make([]string, len(expiries))
	for i, expiry := range expiries {
		formatted[i] = expiry.Format("02-Jan-2006")
	}
	return formatted
}

func filterInstruments(allInstruments []kiteconnect.Instrument, targetNames []string) []kiteconnect.Instrument {
	filtered := make([]kiteconnect.Instrument, 0)
	for _, inst := range allInstruments {
		for _, name := range targetNames {
			if inst.Name == name {
				filtered = append(filtered, inst)
				break
			}
		}
	}
	return filtered
}

func (k *KiteConnect) saveInstrumentsToFile(ctx context.Context, instruments []kiteconnect.Instrument) error {
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-2006")

	// Convert to proto message
	data, err := proto.Marshal(convertToProtoInstruments(instruments))
	if err != nil {
		log.Error("Failed to marshal instruments", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Save using filestore
	if err := k.fileStore.SaveGzippedProto("instruments", currentDate, data); err != nil {
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
	log := logger.GetLogger()
	currentDate := time.Now().Format("02-01-2006")

	data, err := k.fileStore.ReadGzippedProto("instruments", currentDate)
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

	log.Debug("Created instrument expiry map", map[string]interface{}{
		"instruments_count": len(result.Data),
		"options":           countOptions(result),
		"sample":            formatSampleData(result),
	})

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

func (k *KiteConnect) GetUpcomingExpiryTokens(ctx context.Context, instruments []string) ([]string, error) {
	log := logger.GetLogger()

	// Get the full instrument map
	symbolMap, err := k.GetInstrumentExpirySymbolMap(ctx)
	if err != nil {
		log.Error("Failed to get instrument map", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	// Get token lookup maps (using cached version)
	_, tokenInfo := k.CreateLookupMapWithExpiryVSTokenMap(symbolMap)

	// Find upcoming expiry
	now := time.Now().Truncate(24 * time.Hour)
	upcomingExpiry := make(map[string]time.Time)

	for _, instrument := range instruments {
		if expiryMap, exists := symbolMap.Data[instrument]; exists {
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
				upcomingExpiry[instrument] = minExpiry
			}
		}
	}

	// Collect tokens for upcoming expiry
	tokens := make([]string, 0)
	for token, info := range tokenInfo {
		for _, instrument := range instruments {
			if expiry, exists := upcomingExpiry[instrument]; exists {
				if info.Expiry.Equal(expiry) {
					tokens = append(tokens, token)
				}
			}
		}
	}

	log.Info("Retrieved upcoming expiry tokens", map[string]interface{}{
		"instruments":  instruments,
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
	return map[string]string{
		"NIFTY":           "256265",
		"SENSEX":          "265",
		"BANKEX":          "274441",
		"NIFTYBANK":       "260105",
		"NIFTYFINSERVICE": "257801",
		"NIFTYMIDSELECT":  "288009",
	}
}
