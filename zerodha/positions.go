package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/utils"

	"github.com/redis/go-redis/v9"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// Redis key format constants
const (
	// PositionKeyFormat is the format for position keys in Redis
	RealTradingPositionKeyFormat  = "position:real_trading:%s:%s:%s"  // category, tradingsymbol, product
	PaperTradingPositionKeyFormat = "position:paper_trading:%s:%s:%s" // category, tradingsymbol, product

	// PositionTokenKeyFormat is the format for position token keys in Redis
	PositionTokenKeyFormat = "position:token:%d" // instrumentToken

	PostionsJSONKeyFormat = "positionsdump:%s" // paper or real
)

// PositionManager handles all position-related operations
type PositionManager struct {
	kite           *KiteConnect
	log            *logger.Logger
	positionsRedis *redis.Client
}

// PositionSummary represents the summary of all positions
type PositionSummary struct {
	TotalCallValue    float64 `json:"total_call_value"`
	TotalPutValue     float64 `json:"total_put_value"`
	TotalValue        float64 `json:"total_value"`
	TotalCallPending  float64 `json:"total_call_pending"`
	TotalPutPending   float64 `json:"total_put_pending"`
	TotalPendingValue float64 `json:"total_pending_value"`
}

// MoveStep represents a possible position adjustment step
type MoveStep struct {
	Strike          float64  `json:"strike"`
	Premium         float64  `json:"premium"`
	Steps           []string `json:"steps"`
	InstrumentToken string   `json:"instrument_token"`
}

// MoveSuggestions represents possible position adjustments
type MoveSuggestions struct {
	Away   []MoveStep `json:"away"`
	Closer []MoveStep `json:"closer"`
}

// DetailedPosition represents a position with additional analysis
type DetailedPosition struct {
	TradingSymbol   string          `json:"trading_symbol"` // Full trading symbol
	Strike          float64         `json:"strike"`
	Expiry          string          `json:"expiry"`      // Expiry date in YYYY-MM-DD format (e.g., "2025-05-08")
	OptionType      string          `json:"option_type"` // CE or PE
	Quantity        int64           `json:"quantity"`
	AveragePrice    float64         `json:"average_price"`
	BuyPrice        float64         `json:"buy_price"` // Price at which the position was bought
	SellPrice       float64         `json:"sell_price"`
	LTP             float64         `json:"ltp"`
	Diff            float64         `json:"diff"`
	Value           float64         `json:"value"`
	PaperTrading    bool            `json:"paper_trading"`    // Whether this is a paper trading position
	InstrumentToken string          `json:"instrument_token"` // Instrument token for the position
	Moves           MoveSuggestions `json:"moves"`
}

// PositionAnalysis represents the complete position analysis
type PositionAnalysis struct {
	Summary         PositionSummary    `json:"summary"`
	OpenPositions   []DetailedPosition `json:"open_positions"`
	ClosedPositions []DetailedPosition `json:"closed_positions,omitempty"`
}

type CachedRedisPostitionsAndQuantity struct {
	InstrumentToken string `json:"instrument_token"`
	Quantity        int64  `json:"quantity"`
}

// OrderResponse is defined in orders.go

var (
	positionInstance *PositionManager
	positionOnce     sync.Once
)

// GetPositionManager returns a singleton instance of PositionManager
func GetPositionManager() *PositionManager {
	positionOnce.Do(func() {
		log := logger.L()
		kite := GetKiteConnect()
		redisCache, err := cache.GetRedisCache()
		if err != nil {
			log.Error("Failed to get Redis cache", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		if kite == nil {
			log.Error("Failed to get KiteConnect instance", map[string]interface{}{})
			return
		}

		positionInstance = &PositionManager{
			log:            log,
			kite:           kite,
			positionsRedis: redisCache.GetPositionsDB2(),
		}
		log.Info("Position manager initialized", map[string]interface{}{})
	})
	return positionInstance
}

func (pm *PositionManager) GetOpenPositionTokensVsQuanityFromRedis(ctx context.Context) (map[string]CachedRedisPostitionsAndQuantity, error) {
	if pm == nil || pm.positionsRedis == nil {
		return nil, fmt.Errorf("position manager or redis client not initialized")
	}

	// Initialize result map with instrument token as key
	cachePositionsMap := make(map[string]CachedRedisPostitionsAndQuantity)

	// Get the comma-separated position data from Redis
	allPositions, err := pm.positionsRedis.Get(ctx, RealTradingPositionKeyFormat).Result()

	if err == redis.Nil {
		pm.log.Debug("No positions found in Redis", map[string]interface{}{
			"key": RealTradingPositionKeyFormat,
		})
		return cachePositionsMap, nil
	}

	if err != nil {
		pm.log.Error("Failed to get all positions from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get all positions from Redis: %w", err)
	}

	// Handle empty string case
	if allPositions == "" {
		return cachePositionsMap, nil
	}

	// Split the comma-separated string into individual position entries
	for _, posEntry := range strings.Split(allPositions, ",") {
		// Skip empty entries
		if posEntry == "" {
			continue
		}

		// Parse the token and quantity from the format "token_quantity"
		parts := strings.Split(posEntry, "_")
		if len(parts) != 2 {
			pm.log.Error("Invalid position entry format", map[string]interface{}{
				"entry": posEntry,
			})
			continue
		}

		// Extract token and quantity
		tokenStr := parts[0]
		quantityStr := parts[1]

		// Convert quantity string to int64
		quantity, err := strconv.ParseInt(quantityStr, 10, 64)
		if err != nil {
			pm.log.Error("Failed to parse quantity", map[string]interface{}{
				"quantity": quantityStr,
				"error":    err.Error(),
			})
			continue
		}

		// Add to the result map with token as key
		cachePositionsMap[tokenStr] = CachedRedisPostitionsAndQuantity{
			InstrumentToken: tokenStr,
			Quantity:        quantity,
		}
	}

	return cachePositionsMap, nil
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// PollPositionsAndUpdateInRedis periodically polls positions and updates Redis and database
func (pm *PositionManager) PollPositionsAndUpdateInRedis(ctx context.Context) error {
	positions, err := pm.kite.Kite.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}
	pm.log.Debug("Fetched positions for sync", map[string]interface{}{
		"count":     len(positions.Net),
		"positions": positions.Net,
	})

	// Use a wait group to wait for both operations to complete
	var wg sync.WaitGroup
	wg.Add(2)

	// Channel to collect errors from goroutines
	errChan := make(chan error, 2)

	// Store positions in Redis concurrently
	go func() {
		defer wg.Done()
		// Store net positions alone in Redis
		if err := pm.storePositionsInRedis(ctx, positions.Net); err != nil {
			pm.log.Error("Failed to store positions in Redis", map[string]interface{}{
				"error": err.Error(),
			})
			errChan <- err
		}
	}()

	// Store positions in database concurrently
	go func() {
		defer wg.Done()
		if err := pm.storePositionsInDB(ctx, positions.Net); err != nil {
			pm.log.Error("Failed to store positions in database", map[string]interface{}{
				"error": err.Error(),
			})
			// Don't send this error to errChan as we want to continue even if DB update fails
		}
	}()

	// Wait for both goroutines to finish
	wg.Wait()

	// Check if there was an error in Redis storage (which is critical)
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// storePositionsInDB stores positions in the database
func (pm *PositionManager) storePositionsInDB(ctx context.Context, positions []kiteconnect.Position) error {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	// First, get all existing positions to check for duplicates
	existingPositions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to list existing positions", map[string]interface{}{
			"error": err.Error(),
		})
		// Continue with the sync even if we can't get existing positions
	} else {
		pm.log.Debug("Fetched existing positions for sync", map[string]interface{}{
			"count": len(existingPositions),
		})
	}

	// Create maps to track positions by trading symbol
	// We'll use this to find duplicates and to find the most recent position for each symbol
	existingPositionsBySymbol := make(map[string][]*db.PositionRecord)
	for _, existingPos := range existingPositions {
		// Only include non-paper trading positions in the map
		// This ensures we don't update paper trading positions with real ones
		if !existingPos.PaperTrading {
			key := fmt.Sprintf("%s_%s_%s", existingPos.TradingSymbol, existingPos.Exchange, existingPos.Product)
			existingPositionsBySymbol[key] = append(existingPositionsBySymbol[key], existingPos)
		}
	}

	// Clean up duplicate positions first
	for key, positionList := range existingPositionsBySymbol {
		if len(positionList) > 1 {
			// We have duplicates for this trading symbol
			pm.log.Info("Found duplicate positions", map[string]interface{}{
				"key":             key,
				"duplicate_count": len(positionList),
				"trading_symbol":  positionList[0].TradingSymbol,
			})

			// Sort positions by ID (descending) to keep the most recent one
			sort.Slice(positionList, func(i, j int) bool {
				return positionList[i].ID > positionList[j].ID
			})

			// Keep the position with the highest ID (most recent) and delete others
			for i := 1; i < len(positionList); i++ {
				pm.log.Info("Deleting duplicate position", map[string]interface{}{
					"position_id":    key,
					"trading_symbol": positionList[i].TradingSymbol,
					"database_id":    positionList[i].ID,
					"quantity":       positionList[i].Quantity,
				})

				// Delete the duplicate position using the TimescaleDB's DeletePosition method
				if err := timescaleDB.DeletePosition(ctx, positionList[i].ID); err != nil {
					pm.log.Error("Failed to delete duplicate position", map[string]interface{}{
						"error":          err.Error(),
						"position_id":    key,
						"database_id":    positionList[i].ID,
						"trading_symbol": positionList[i].TradingSymbol,
					})
				}
			}
		}
	}

	// Create a map with only the most recent position for each symbol
	existingPositionMap := make(map[string]*db.PositionRecord)
	for key, positionList := range existingPositionsBySymbol {
		if len(positionList) > 0 {
			// Use the first position (which is the most recent after sorting)
			existingPositionMap[key] = positionList[0]
		}
	}

	// Track which positions we've seen from Zerodha
	seenPositions := make(map[string]bool)

	// Process each position from Zerodha
	for _, pos := range positions {
		// Create a unique position ID
		positionIDStr := fmt.Sprintf("%s_%s_%s", pos.Tradingsymbol, pos.Exchange, pos.Product)

		// Mark this position as seen
		seenPositions[positionIDStr] = true

		// Check if we already have this position
		existingPos, exists := existingPositionMap[positionIDStr]

		// Create a position record
		posRecord := &db.PositionRecord{
			PositionID:    &positionIDStr,
			TradingSymbol: pos.Tradingsymbol,
			Exchange:      pos.Exchange,
			Product:       pos.Product,
			Quantity:      pos.Quantity,
			AveragePrice:  pos.AveragePrice,
			LastPrice:     pos.LastPrice,
			PnL:           pos.PnL,
			RealizedPnL:   pos.Realised,   // Field name is different in Kite API
			UnrealizedPnL: pos.Unrealised, // Field name is different in Kite API
			Multiplier:    pos.Multiplier,
			BuyQuantity:   pos.BuyQuantity,
			SellQuantity:  pos.SellQuantity,
			BuyPrice:      pos.BuyPrice,
			SellPrice:     pos.SellPrice,
			BuyValue:      pos.BuyValue,
			SellValue:     pos.SellValue,
			PositionType:  "net",
			UserID:        "system", // Default user ID
			UpdatedAt:     time.Now(),
			PaperTrading:  false, // Default to false for real positions
			KiteResponse:  pos,   // Store the original Kite position
		}

		// If position exists, update its ID to ensure we update rather than insert
		if exists {
			// Always update the ID to ensure we update rather than insert
			posRecord.ID = existingPos.ID

			// Check if the quantity has changed
			if existingPos.Quantity != pos.Quantity {
				pm.log.Info("Updating position quantity", map[string]interface{}{
					"position_id":    positionIDStr,
					"trading_symbol": pos.Tradingsymbol,
					"old_quantity":   existingPos.Quantity,
					"new_quantity":   pos.Quantity,
					"database_id":    existingPos.ID,
				})

				// If the position is now zero, log that it's been closed
				if pos.Quantity == 0 && existingPos.Quantity != 0 {
					pm.log.Info("Position closed in Zerodha", map[string]interface{}{
						"position_id":    positionIDStr,
						"trading_symbol": pos.Tradingsymbol,
						"previous_qty":   existingPos.Quantity,
					})
				}
			} else {
				pm.log.Debug("Position quantity unchanged", map[string]interface{}{
					"position_id":    positionIDStr,
					"trading_symbol": pos.Tradingsymbol,
					"quantity":       pos.Quantity,
				})
			}
		} else {
			// Create a record for all positions, including those with zero quantity
			// This ensures we track all positions that were opened during the day
			// which is important for accurate P&L calculations
			if pos.Quantity != 0 {
				pm.log.Info("Creating new position", map[string]interface{}{
					"position_id":    positionIDStr,
					"trading_symbol": pos.Tradingsymbol,
					"quantity":       pos.Quantity,
				})
			} else {
				// Store zero-quantity positions as well for historical tracking
				pm.log.Info("Creating zero-quantity position for historical tracking", map[string]interface{}{
					"position_id":    positionIDStr,
					"trading_symbol": pos.Tradingsymbol,
					"buy_value":      pos.BuyValue,
					"sell_value":     pos.SellValue,
				})
			}
		}

		// Store the position in the database
		if err := timescaleDB.UpsertPosition(ctx, posRecord); err != nil {
			pm.log.Error("Failed to upsert position", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    positionIDStr,
				"trading_symbol": pos.Tradingsymbol,
			})
		}
	}

	// Remove positions that exist in the database but are no longer in Zerodha
	for key, existingPos := range existingPositionMap {
		// Skip paper trading positions
		if existingPos.PaperTrading {
			continue
		}

		// If we didn't see this position in the Zerodha response, delete it
		if !seenPositions[key] {
			pm.log.Info("Removing position no longer in Zerodha", map[string]interface{}{
				"position_id":    key,
				"trading_symbol": existingPos.TradingSymbol,
				"database_id":    existingPos.ID,
				"quantity":       existingPos.Quantity,
			})

			// Delete the position from the database
			if err := timescaleDB.DeletePosition(ctx, existingPos.ID); err != nil {
				pm.log.Error("Failed to delete stale position", map[string]interface{}{
					"error":          err.Error(),
					"position_id":    key,
					"database_id":    existingPos.ID,
					"trading_symbol": existingPos.TradingSymbol,
				})
			}
		}
	}

	return nil
}

// storePositionsInRedis stores a list of positions in Redis
func (pm *PositionManager) storePositionsInRedis(ctx context.Context, positions []kiteconnect.Position) error {
	// Build comma-separated list of instrument tokens and quantities
	positionValues := make([]string, 0, len(positions))

	// Process each position
	for _, pos := range positions {
		// Add to the comma-separated list
		positionValues = append(positionValues, fmt.Sprintf("%d_%d", pos.InstrumentToken, pos.Quantity))

		// Store full position details
		key := fmt.Sprintf(PostionsJSONKeyFormat, pos.Tradingsymbol)

		// Convert position to JSON
		posJSON, err := json.Marshal(pos)
		if err != nil {
			pm.log.Error("Failed to marshal position", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}

		// Store full position in Redis
		err = pm.positionsRedis.HSet(ctx, "positions", key, string(posJSON)).Err()
		if err != nil {
			pm.log.Error("Failed to store position in Redis", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}
	}

	// Join all position values with commas and store in Redis
	positionAllKeyFormatValue := strings.Join(positionValues, ",")
	err := pm.positionsRedis.Set(ctx, PaperTradingPositionKeyFormat, positionAllKeyFormatValue, 0).Err()
	if err != nil {
		pm.log.Error("Failed to store all positions in Redis", map[string]interface{}{
			"error": err.Error(),
		})
	}

	return nil
}

// PositionFilterType represents the type of position filtering to apply
type PositionFilterType string

const (
	// PositionFilterAll returns all positions (default)
	PositionFilterAll PositionFilterType = "all"
	// PositionFilterPaper returns only paper trading positions
	PositionFilterPaper PositionFilterType = "paper"
	// PositionFilterReal returns only real trading positions
	PositionFilterReal PositionFilterType = "real"
)

// GetPositionAnalysis returns detailed analysis of positions
// filterType specifies which positions to include (all, paper, or real)
func (pm *PositionManager) GetPositionAnalysis(ctx context.Context, filterType PositionFilterType) (*PositionAnalysis, error) {
	// Initialize analysis structure
	analysis := &PositionAnalysis{
		Summary:         PositionSummary{},
		OpenPositions:   make([]DetailedPosition, 0),
		ClosedPositions: make([]DetailedPosition, 0),
	}

	// Get positions from database (includes both real and paper trading positions)
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache meta instance: %w", err)
	}

	// Fetch all positions from database
	dbPositions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get positions from database: %w", err)
	}

	pm.log.Debug("Fetched positions from database", map[string]interface{}{
		"count": len(dbPositions),
	})

	// Process all positions from database (both real and paper trading)
	for _, dbPos := range dbPositions {
		// Skip non-option positions
		if !isOptionPosition(dbPos.TradingSymbol) {
			continue
		}

		// Apply position filter
		switch filterType {
		case PositionFilterPaper:
			if !dbPos.PaperTrading {
				continue
			}
		case PositionFilterReal:
			if dbPos.PaperTrading {
				continue
			}
			// PositionFilterAll or any other value - include all positions
		}

		// Get option type
		optionType := getOptionType(dbPos.TradingSymbol)

		instrumentToken, _ := cacheMeta.GetTokenBySymbol(ctx, dbPos.TradingSymbol)
		ltpStruct, _ := cacheMeta.GetLTPforInstrumentTokensList(ctx, []string{instrumentToken})
		tokenMeta, _ := cacheMeta.GetMetadataOfToken(ctx, instrumentToken)

		// Create a kiteconnect.Position from the db.PositionRecord for move calculation
		pseudoPos := kiteconnect.Position{
			Tradingsymbol:   dbPos.TradingSymbol,
			Exchange:        dbPos.Exchange,
			InstrumentToken: utils.StringToUint32(instrumentToken),
			Product:         dbPos.Product,
			Quantity:        dbPos.Quantity,
			AveragePrice:    dbPos.AveragePrice,
			LastPrice:       ltpStruct[0].LTP,
			ClosePrice:      0,
			PnL:             dbPos.PnL,
			M2M:             0,
			Multiplier:      dbPos.Multiplier,
			BuyQuantity:     dbPos.BuyQuantity,
			SellQuantity:    dbPos.SellQuantity,
			BuyPrice:        dbPos.BuyPrice,
			SellPrice:       dbPos.SellPrice,
			BuyValue:        dbPos.BuyValue,
			SellValue:       dbPos.SellValue,
		}

		detailedPos := DetailedPosition{
			TradingSymbol:   dbPos.TradingSymbol,
			Strike:          utils.StringToFloat64(tokenMeta.StrikePrice),
			Expiry:          tokenMeta.Expiry,
			OptionType:      optionType,
			Quantity:        int64(dbPos.Quantity),
			AveragePrice:    dbPos.AveragePrice,
			BuyPrice:        dbPos.BuyPrice,
			SellPrice:       dbPos.SellPrice,
			LTP:             ltpStruct[0].LTP,
			Diff:            ltpStruct[0].LTP - dbPos.AveragePrice,
			Value:           math.Abs(float64(dbPos.Quantity) * dbPos.AveragePrice),
			PaperTrading:    dbPos.PaperTrading,
			InstrumentToken: tokenMeta.Token,
			Moves:           pm.calculateMoves(ctx, pseudoPos, utils.StringToFloat64(tokenMeta.StrikePrice), tokenMeta.Expiry),
		}

		// Calculate position value (original capital deployed)
		quantity := float64(dbPos.Quantity)
		positionValue := math.Abs(quantity * dbPos.AveragePrice)

		// Calculate position values based on option premium perspective
		var pendingValue float64
		var pnl float64

		if quantity < 0 {
			// Sell/Short position - premium we collect (positive value)
			// For options we've sold, we've already collected the premium
			// The pending value is what we'd need to pay to close the position
			pendingValue = math.Abs(quantity * detailedPos.LTP)

			// For sold options, profit = premium collected - current value
			premiumCollected := math.Abs(quantity * dbPos.AveragePrice)
			currentCost := math.Abs(quantity * detailedPos.LTP)
			pnl = premiumCollected - currentCost
		} else {
			// Buy/Long position - premium we pay (negative value)
			// For options we've bought, we've paid the premium
			// The pending value is what we'd get if we close the position
			pendingValue = -math.Abs(quantity * detailedPos.LTP)

			// For bought options, profit = current value - premium paid
			premiumPaid := math.Abs(quantity * dbPos.AveragePrice)
			currentValue := math.Abs(quantity * detailedPos.LTP)
			pnl = currentValue - premiumPaid
		}

		// Log position values for debugging
		pm.log.Debug("Position value calculation", map[string]interface{}{
			"symbol":         dbPos.TradingSymbol,
			"quantity":       quantity,
			"avg_price":      dbPos.AveragePrice,
			"ltp":            detailedPos.LTP,
			"position_value": positionValue,
			"pending_value":  pendingValue,
			"pnl":            pnl,
			"position_type":  optionType,
		})

		// Update summary based on option type
		if optionType == "CE" {
			analysis.Summary.TotalCallValue += positionValue
			analysis.Summary.TotalCallPending += pendingValue
		} else {
			analysis.Summary.TotalPutValue += positionValue
			analysis.Summary.TotalPutPending += pendingValue
		}

		// Separate open and closed positions
		if dbPos.Quantity != 0 {
			analysis.OpenPositions = append(analysis.OpenPositions, detailedPos)
		} else {
			analysis.ClosedPositions = append(analysis.ClosedPositions, detailedPos)
		}

	}

	// Calculate totals
	analysis.Summary.TotalValue = analysis.Summary.TotalCallValue + analysis.Summary.TotalPutValue
	analysis.Summary.TotalPendingValue = analysis.Summary.TotalCallPending + analysis.Summary.TotalPutPending

	// Log position counts
	pm.log.Info("Position analysis complete", map[string]interface{}{
		"open_positions":   len(analysis.OpenPositions),
		"closed_positions": len(analysis.ClosedPositions),
	})
	return analysis, nil
}

// Helper functions

func isOptionPosition(tradingSymbol string) bool {
	return strings.HasSuffix(tradingSymbol, "CE") || strings.HasSuffix(tradingSymbol, "PE")
}

func getOptionType(symbol string) string {
	if strings.HasSuffix(symbol, "CE") {
		return "CE"
	}
	return "PE"
}

// getInstrumentTokenForStrike is a helper function to find the instrument token for a given strike price and option type
func (pm *PositionManager) getInstrumentTokenForStrike(ctx context.Context, strike float64, optionType string, baseSymbol string, expiryDate string) string {
	// Get in-memory cache instance
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Create symbol with the strike price
	strike = math.Round(strike) // Round to whole number
	strikeStr := fmt.Sprintf("%.0f", strike)
	newSymbol := baseSymbol + strikeStr + optionType

	// First try direct lookup by symbol
	tokenInterface, exists := inmemoryCache.Get(newSymbol)
	if exists {
		pm.log.Debug("Found token directly from symbol", map[string]interface{}{
			"symbol": newSymbol,
			"token":  fmt.Sprintf("%v", tokenInterface),
		})
		return fmt.Sprintf("%v", tokenInterface)
	}

	// Try to find using the strike lookup key if we have expiry date
	if expiryDate != "" {
		strikeLookupKey := fmt.Sprintf("next_strike_lookup:%.0f:%s:%s", strike, expiryDate, optionType)
		tokenInterface, exists = inmemoryCache.Get(strikeLookupKey)
		if exists {
			pm.log.Debug("Found token using strike lookup", map[string]interface{}{
				"key":   strikeLookupKey,
				"token": fmt.Sprintf("%v", tokenInterface),
			})
			return fmt.Sprintf("%v", tokenInterface)
		}
	}

	// If all else fails, return empty string
	return ""
}

func (pm *PositionManager) calculateMoves(ctx context.Context, pos kiteconnect.Position, strike float64, expiryDate string) MoveSuggestions {
	moves := MoveSuggestions{
		Away:   make([]MoveStep, 0),
		Closer: make([]MoveStep, 0),
	}
	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		pm.log.Error("Failed to get cache meta instance for move", map[string]interface{}{
			"error": err.Error(),
		})
		return moves
	}
	// Calculate steps for position adjustment
	steps := []string{"1/4", "1/2", "1"}

	// Set strike gap based on index name or trading symbol
	strikeGap := 50.0
	if strings.Contains(pos.Tradingsymbol, "BANKNIFTY") || strings.Contains(pos.Tradingsymbol, "SENSEX") {
		strikeGap = 100.0
	} else {
		// Try to get index name from token
		indexName, err := pm.kite.GetIndexNameFromToken(ctx, fmt.Sprintf("%d", pos.InstrumentToken))
		if err == nil && (indexName == "BANKNIFTY" || indexName == "SENSEX") {
			strikeGap = 100.0
		}
	}

	// Get option type from trading symbol
	optionType := getOptionType(pos.Tradingsymbol)

	// Away moves (2 strikes)
	for i := 1; i <= 2; i++ {
		var newStrike float64
		if optionType == "CE" {
			newStrike = strike + (strikeGap * float64(i))
		} else {
			newStrike = strike - (strikeGap * float64(i))
		}

		// Get the instrument token for this strike using our new helper function
		tokenStr, err := cacheMetaInstance.GetInstrumentTokenForStrike(ctx, newStrike, optionType, expiryDate)
		if err != nil {
			pm.log.Error("Failed to get instrument token for strike", map[string]interface{}{
				"strike":      newStrike,
				"option_type": optionType,
				"expiry":      expiryDate,
				"error":       err.Error(),
			})
			return moves
		}
		premium := getPremium(tokenStr)

		moves.Away = append(moves.Away, MoveStep{
			Strike:          newStrike,
			Premium:         premium,
			Steps:           steps,
			InstrumentToken: tokenStr,
		})
	}

	// Closer moves (2 strikes)
	for i := 1; i <= 2; i++ {
		var newStrike float64
		if optionType == "CE" {
			newStrike = strike - (strikeGap * float64(i))
		} else {
			newStrike = strike + (strikeGap * float64(i))
		}

		// Get the instrument token for this strike using our new helper function
		tokenStr, err := cacheMetaInstance.GetInstrumentTokenForStrike(ctx, newStrike, optionType, expiryDate)
		if err != nil {
			pm.log.Error("Failed to get instrument token for strike", map[string]interface{}{
				"strike":      newStrike,
				"option_type": optionType,
				"expiry":      expiryDate,
				"error":       err.Error(),
			})
			return moves
		}
		premium := getPremium(tokenStr)

		moves.Closer = append(moves.Closer, MoveStep{
			Strike:          newStrike,
			Premium:         premium,
			Steps:           steps,
			InstrumentToken: tokenStr,
		})
	}

	return moves
}

func getPremium(tokenStr string) float64 {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return 0.0
	}
	ltpDB := redisCache.GetLTPDB3()
	ltpVal, err := ltpDB.Get(context.Background(), fmt.Sprintf("%s_ltp", tokenStr)).Float64()
	if err != nil {
		return 0.0
	}
	return ltpVal
}
