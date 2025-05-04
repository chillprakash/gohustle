package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	sync "sync"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// PositionManager handles all position-related operations
type PositionManager struct {
	kite *KiteConnect
	log  *logger.Logger
}

// PositionSummary represents the summary of all positions
type PositionSummary struct {
	TotalCallValue float64 `json:"total_call_value"`
	TotalPutValue  float64 `json:"total_put_value"`
	TotalValue     float64 `json:"total_value"`
}

// MoveStep represents a possible position adjustment step
type MoveStep struct {
	Strike  float64  `json:"strike"`
	Premium float64  `json:"premium"`
	Steps   []string `json:"steps"`
}

// MoveSuggestions represents possible position adjustments
type MoveSuggestions struct {
	Away   []MoveStep `json:"away"`
	Closer []MoveStep `json:"closer"`
}

// DetailedPosition represents a position with additional analysis
type DetailedPosition struct {
	TradingSymbol string          `json:"trading_symbol"` // Full trading symbol
	Strike        float64         `json:"strike"`
	Expiry        string          `json:"expiry"`      // Expiry date in YYYY-MM-DD format (e.g., "2025-05-08")
	OptionType    string          `json:"option_type"` // CE or PE
	Quantity      int64           `json:"quantity"`
	AveragePrice  float64         `json:"average_price"`
	SellPrice     float64         `json:"sell_price"`
	LTP           float64         `json:"ltp"`
	Diff          float64         `json:"diff"`
	Value         float64         `json:"value"`
	PaperTrading  bool            `json:"paper_trading"` // Whether this is a paper trading position
	Moves         MoveSuggestions `json:"moves"`
}

// PositionAnalysis represents the complete position analysis
type PositionAnalysis struct {
	Summary   PositionSummary    `json:"summary"`
	Positions []DetailedPosition `json:"positions"`
}

var (
	positionInstance *PositionManager
	positionOnce     sync.Once
)

// GetPositionManager returns a singleton instance of PositionManager
func GetPositionManager() *PositionManager {
	positionOnce.Do(func() {
		log := logger.L()
		kite := GetKiteConnect()
		if kite == nil {
			log.Error("Failed to get KiteConnect instance", map[string]interface{}{})
			return
		}

		positionInstance = &PositionManager{
			log:  log,
			kite: kite,
		}
		log.Info("Position manager initialized", map[string]interface{}{})
	})
	return positionInstance
}

// GetOpenPositions fetches current positions from Zerodha
func (pm *PositionManager) GetOpenPositions(ctx context.Context) (kiteconnect.Positions, error) {
	if pm == nil || pm.kite == nil {
		return kiteconnect.Positions{}, fmt.Errorf("position manager or kite client not initialized")
	}

	positions, err := pm.kite.Kite.GetPositions()
	if err != nil {
		return kiteconnect.Positions{}, fmt.Errorf("failed to fetch positions: %w", err)
	}
	return positions, nil
}

// PollPositionsAndUpdateInRedis periodically polls positions and updates Redis and database
func (pm *PositionManager) PollPositionsAndUpdateInRedis(ctx context.Context) error {
	positions, err := pm.GetOpenPositions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	// Use a wait group to wait for both operations to complete
	var wg sync.WaitGroup
	wg.Add(2)

	// Channel to collect errors from goroutines
	errChan := make(chan error, 2)

	// Store positions in Redis concurrently
	go func() {
		defer wg.Done()
		// Store net positions alone in Redis
		if err := pm.storePositionsInRedis(ctx, "net", positions.Net); err != nil {
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

	// Process each position
	for _, pos := range positions {
		// Create a unique position ID
		positionID := fmt.Sprintf("%s_%s_%s", pos.Tradingsymbol, pos.Exchange, pos.Product)

		// Create a position record
		posRecord := &db.PositionRecord{
			PositionID:    positionID,
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

		// Upsert the position in the database
		if err := timescaleDB.UpsertPosition(ctx, posRecord); err != nil {
			pm.log.Error("Failed to upsert position in database", map[string]interface{}{
				"error":          err.Error(),
				"position_id":    positionID,
				"trading_symbol": pos.Tradingsymbol,
			})
			// Continue with other positions even if one fails
			continue
		}
	}

	return nil
}

// storePositionsInRedis stores a list of positions in Redis
func (pm *PositionManager) storePositionsInRedis(ctx context.Context, category string, positions []kiteconnect.Position) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	positionsRedis := redisCache.GetPositionsDB2()

	for _, pos := range positions {
		// Store full position details
		key := fmt.Sprintf("position:%s:%s:%s", category, pos.Tradingsymbol, pos.Product)

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
		err = positionsRedis.HSet(ctx, "positions", key, string(posJSON)).Err()
		if err != nil {
			pm.log.Error("Failed to store position in Redis", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}

		// Store instrument token to quantity mapping
		tokenKey := fmt.Sprintf("position:token:%d", pos.InstrumentToken)
		err = positionsRedis.Set(ctx, tokenKey, pos.Quantity, 0).Err()
		if err != nil {
			pm.log.Error("Failed to store token quantity in Redis", map[string]interface{}{
				"token":    pos.InstrumentToken,
				"quantity": pos.Quantity,
				"error":    err.Error(),
			})
		} else {
			pm.log.Debug("Stored token quantity", map[string]interface{}{
				"token":    pos.InstrumentToken,
				"quantity": pos.Quantity,
			})
		}
	}
	return nil
}

// GetPositionAnalysis returns detailed analysis of all positions
func (pm *PositionManager) GetPositionAnalysis(ctx context.Context) (*PositionAnalysis, error) {
	// Initialize analysis structure
	analysis := &PositionAnalysis{
		Summary:   PositionSummary{},
		Positions: make([]DetailedPosition, 0),
	}

	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis cache: %w", err)
	}
	ltpDB := redisCache.GetLTPDB3()
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Get positions from database (includes both real and paper trading positions)
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	// Fetch all positions from database
	dbPositions, err := timescaleDB.ListPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from database", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get positions from database: %w", err)
	}

	pm.log.Info("Fetched positions from database", map[string]interface{}{
		"count": len(dbPositions),
	})

	// Process all positions from database (both real and paper trading)
	for _, dbPos := range dbPositions {

		// Skip non-option positions
		if !isOptionPosition(dbPos.TradingSymbol) {
			continue
		}

		// Get option type
		optionType := getOptionType(dbPos.TradingSymbol)

		// Get current LTP
		var ltp float64 = dbPos.LastPrice
		var expiryDate string
		var strikePrice float64
		var tokenStr string

		// Try to get instrument token from cache - we store this as trading symbol -> token mapping
		tokenValue, exists := inmemoryCache.Get(dbPos.TradingSymbol)
		if exists {
			// Convert token to string format for cache keys
			tokenStr = fmt.Sprintf("%v", tokenValue)

			// Try to get LTP from Redis using instrument token
			ltpKey := fmt.Sprintf("%s_ltp", tokenStr)
			ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
			if err == nil && ltpVal > 0 {
				ltp = ltpVal
			}

			// Get expiry date from cache
			expiryKey := fmt.Sprintf("expiry:%s", tokenStr)
			expiryVal, expiryExists := inmemoryCache.Get(expiryKey)
			if expiryExists {
				switch v := expiryVal.(type) {
				case string:
					// Keep the ISO format (YYYY-MM-DD) as is
					if len(v) >= 10 && strings.Contains(v, "-") { // Format like "2025-05-25"
						expiryDate = v
					} else {
						// Check if it's in MMYYYY format (like "052025")
						if len(v) == 6 && !strings.Contains(v, "-") {
							// Convert from MMYYYY to YYYY-MM-DD
							month := v[:2]
							year := v[2:]
							// Default to day 1 if we don't have day information
							expiryDate = year + "-" + month + "-01"
						} else {
							// For any other format, just use as-is and log
							expiryDate = v
							pm.log.Debug("Unexpected expiry date format", map[string]interface{}{
								"expiry": v,
							})
						}
					}
				case time.Time:
					// Format time.Time as YYYY-MM-DD
					expiryDate = v.Format("2006-01-02")
				default:
					// Just log and continue
					pm.log.Debug("Unexpected expiry date format in cache", map[string]interface{}{
						"type":  fmt.Sprintf("%T", expiryVal),
						"value": expiryVal,
					})
				}
			}

			// Get strike price from cache
			strikeKey := fmt.Sprintf("strike:%s", tokenStr)
			strikeVal, strikeExists := inmemoryCache.Get(strikeKey)
			if strikeExists {
				switch v := strikeVal.(type) {
				case float64:
					strikePrice = v
				case string:
					if val, err := strconv.ParseFloat(v, 64); err == nil {
						strikePrice = val
					}
				default:
					// Just log and continue
					pm.log.Debug("Unexpected strike price format in cache", map[string]interface{}{
						"type":  fmt.Sprintf("%T", strikeVal),
						"value": strikeVal,
					})
				}
			}

		}

		// Create a kiteconnect.Position from the db.PositionRecord for move calculation
		pseudoPos := kiteconnect.Position{
			Tradingsymbol:   dbPos.TradingSymbol,
			Exchange:        dbPos.Exchange,
			InstrumentToken: 0, // We'll set this from the token value if available
			Product:         dbPos.Product,
			Quantity:        dbPos.Quantity,
			AveragePrice:    dbPos.AveragePrice,
			LastPrice:       ltp,
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

		// Set the instrument token from the token value we already retrieved
		if exists {
			switch v := tokenValue.(type) {
			case int:
				pseudoPos.InstrumentToken = uint32(v)
			case int64:
				pseudoPos.InstrumentToken = uint32(v)
			case uint32:
				pseudoPos.InstrumentToken = v
			case string:
				if tokenInt, err := strconv.Atoi(v); err == nil {
					pseudoPos.InstrumentToken = uint32(tokenInt)
				}
			}
		}

		detailedPos := DetailedPosition{
			TradingSymbol: dbPos.TradingSymbol,
			Strike:        strikePrice,
			Expiry:        expiryDate,
			OptionType:    optionType,
			Quantity:      int64(dbPos.Quantity),
			AveragePrice:  dbPos.AveragePrice,
			SellPrice:     dbPos.SellPrice,
			LTP:           ltp,
			Diff:          ltp - dbPos.AveragePrice,
			Value:         math.Abs(float64(dbPos.Quantity) * dbPos.AveragePrice),
			PaperTrading:  dbPos.PaperTrading,
			Moves:         pm.calculateMoves(ctx, pseudoPos),
		}

		// Update summary
		if optionType == "CE" {
			analysis.Summary.TotalCallValue += detailedPos.Value
		} else {
			analysis.Summary.TotalPutValue += detailedPos.Value
		}

		analysis.Positions = append(analysis.Positions, detailedPos)
	}

	analysis.Summary.TotalValue = analysis.Summary.TotalCallValue + analysis.Summary.TotalPutValue
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

func (pm *PositionManager) calculateMoves(ctx context.Context, pos kiteconnect.Position) MoveSuggestions {
	moves := MoveSuggestions{
		Away:   make([]MoveStep, 0),
		Closer: make([]MoveStep, 0),
	}

	// Calculate steps for position adjustment
	steps := []string{"1/4", "1/2", "1"}

	// Get in-memory cache instance
	inmemoryCache := cache.GetInMemoryCacheInstance()

	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		pm.log.Error("Failed to get Redis cache", map[string]interface{}{
			"error": err.Error(),
		})
		return moves
	}
	ltpDB := redisCache.GetLTPDB3()

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

	// Extract strike price and option type from trading symbol
	var strike float64
	optionType := ""

	// Check if it's a CE or PE option
	if strings.HasSuffix(pos.Tradingsymbol, "CE") {
		optionType = "CE"
	} else if strings.HasSuffix(pos.Tradingsymbol, "PE") {
		optionType = "PE"
	} else {
		pm.log.Error("Invalid option type in trading symbol", map[string]interface{}{
			"symbol": pos.Tradingsymbol,
		})
		return moves
	}

	// First try to get strike from cache
	strikeKey := fmt.Sprintf("strike:%d", pos.InstrumentToken)
	strikePrice, ok := inmemoryCache.Get(strikeKey)
	if ok {
		switch v := strikePrice.(type) {
		case float64:
			strike = v
		case string:
			if val, err := strconv.ParseFloat(v, 64); err == nil {
				strike = val
			}
		default:
			pm.log.Error("Invalid strike price type", map[string]interface{}{
				"type": fmt.Sprintf("%T", strikePrice),
			})
		}
	}

	// If we couldn't get strike from cache, extract it from trading symbol
	if strike == 0 {
		// Extract numeric part from trading symbol (the strike price)
		var numStr string
		for i := 0; i < len(pos.Tradingsymbol); i++ {
			if pos.Tradingsymbol[i] >= '0' && pos.Tradingsymbol[i] <= '9' {
				// Found start of number
				numStr = ""
				for j := i; j < len(pos.Tradingsymbol); j++ {
					if (pos.Tradingsymbol[j] >= '0' && pos.Tradingsymbol[j] <= '9') || pos.Tradingsymbol[j] == '.' {
						numStr += string(pos.Tradingsymbol[j])
					} else {
						break
					}
				}
				break
			}
		}

		if numStr != "" {
			var err error
			strike, err = strconv.ParseFloat(numStr, 64)
			if err != nil {
				pm.log.Error("Failed to parse strike from trading symbol", map[string]interface{}{
					"symbol": pos.Tradingsymbol,
					"numStr": numStr,
					"error":  err.Error(),
				})
				// Fall back to using the last price as strike
				strike = pos.LastPrice
			}
		} else {
			// Fall back to using the last price as strike
			strike = pos.LastPrice
		}
	}

	// Extract base symbol (without strike and option type)
	baseSymbol := ""
	for i := 0; i < len(pos.Tradingsymbol); i++ {
		if pos.Tradingsymbol[i] >= '0' && pos.Tradingsymbol[i] <= '9' {
			baseSymbol = pos.Tradingsymbol[:i]
			break
		}
	}

	// If we couldn't extract base symbol, use a default approach
	if baseSymbol == "" {
		// Remove last 2 chars (CE or PE) and any digits
		baseSymbol = strings.TrimRight(pos.Tradingsymbol[:len(pos.Tradingsymbol)-2], "0123456789.")
	}

	// Function to create a new trading symbol with a different strike
	createNewSymbol := func(newStrike float64) string {
		// Convert strike price to whole number (remove decimals)
		newStrike = math.Round(newStrike)
		// Format strike price correctly (no decimal for whole numbers)
		strikeStr := ""
		if newStrike == float64(int(newStrike)) {
			// It's a whole number
			strikeStr = fmt.Sprintf("%.0f", newStrike)
		} else {
			// It has decimals
			strikeStr = fmt.Sprintf("%.1f", newStrike)
		}
		return baseSymbol + strikeStr + optionType
	}

	// Function to get premium for a new strike
	getPremium := func(newSymbol string, defaultMultiplier float64) float64 {
		// Try to get instrument token for this symbol
		tokenInterface, exists := inmemoryCache.Get(newSymbol)
		if !exists {
			return pos.LastPrice * defaultMultiplier
		}

		// Convert token to string
		var tokenStr string
		switch v := tokenInterface.(type) {
		case string:
			tokenStr = v
		case int:
			tokenStr = fmt.Sprintf("%d", v)
		case uint32:
			tokenStr = fmt.Sprintf("%d", v)
		default:
			return pos.LastPrice * defaultMultiplier
		}

		// Try to get LTP from Redis
		ltpKey := fmt.Sprintf("%s_ltp", tokenStr)
		ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
		if err == nil && ltpVal > 0 {
			return ltpVal
		}

		// Fall back to default calculation
		return pos.LastPrice * defaultMultiplier
	}

	// Away moves (2 strikes)
	for i := 1; i <= 2; i++ {
		var newStrike float64
		if optionType == "CE" {
			newStrike = strike + (strikeGap * float64(i))
		} else {
			newStrike = strike - (strikeGap * float64(i))
		}

		// Create new symbol and get premium
		newSymbol := createNewSymbol(newStrike)
		premium := getPremium(newSymbol, 0.8) // Default multiplier 0.8 for away moves

		moves.Away = append(moves.Away, MoveStep{
			Strike:  newStrike,
			Premium: premium,
			Steps:   steps,
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

		// Create new symbol and get premium
		newSymbol := createNewSymbol(newStrike)
		premium := getPremium(newSymbol, 1.2) // Default multiplier 1.2 for closer moves

		moves.Closer = append(moves.Closer, MoveStep{
			Strike:  newStrike,
			Premium: premium,
			Steps:   steps,
		})
	}

	return moves
}
