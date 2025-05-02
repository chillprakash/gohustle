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
	Strike       float64         `json:"strike"`
	OptionType   string          `json:"option_type"` // CE or PE
	Quantity     int64           `json:"quantity"`
	AveragePrice float64         `json:"average_price"`
	SellPrice    float64         `json:"sell_price"`
	LTP          float64         `json:"ltp"`
	Diff         float64         `json:"diff"`
	VWAP         float64         `json:"vwap"`
	Value        float64         `json:"value"`
	Moves        MoveSuggestions `json:"moves"`
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
		// Continue with Zerodha positions if DB fetch fails
	} else {
		pm.log.Info("Fetched positions from database", map[string]interface{}{
			"count": len(dbPositions),
		})
	}

	// Also get positions from Zerodha API for real-time data
	positions, err := pm.GetOpenPositions(ctx)
	if err != nil {
		pm.log.Error("Failed to get positions from Zerodha", map[string]interface{}{
			"error": err.Error(),
		})
		// If we have DB positions, we can continue without Zerodha positions
		if len(dbPositions) == 0 {
			return nil, fmt.Errorf("failed to get positions from any source: %w", err)
		}
	}

	// Process positions from Zerodha API
	for _, pos := range positions.Net {
		// Skip non-option positions
		if !isOptionPosition(pos.Tradingsymbol) {
			continue
		}

		// Get current LTP
		ltpKey := fmt.Sprintf("%d_ltp", pos.InstrumentToken)
		ltp, err := ltpDB.Get(ctx, ltpKey).Float64()
		if err != nil {
			pm.log.Error("Failed to get LTP", map[string]interface{}{
				"token": pos.InstrumentToken,
				"error": err.Error(),
			})
			ltp = pos.LastPrice
		}

		optionType := getOptionType(pos.Tradingsymbol)
		strike_key := fmt.Sprintf("strike:%d", pos.InstrumentToken)
		strikePrice, ok := inmemoryCache.Get(strike_key)
		if !ok {
			pm.log.Error("Failed to get strike price", map[string]interface{}{
				"token":  pos.InstrumentToken,
				"symbol": pos.Tradingsymbol,
				"key":    strike_key,
			})
			continue
		}

		// Convert strike price to float64 based on type
		var strikePriceFloat float64
		switch v := strikePrice.(type) {
		case float64:
			strikePriceFloat = v
		case string:
			var err error
			strikePriceFloat, err = strconv.ParseFloat(v, 64)
			if err != nil {
				pm.log.Error("Failed to parse strike price", map[string]interface{}{
					"token":  pos.InstrumentToken,
					"strike": v,
					"error":  err.Error(),
				})
				continue
			}
		default:
			pm.log.Error("Invalid strike price type", map[string]interface{}{
				"token": pos.InstrumentToken,
				"type":  fmt.Sprintf("%T", strikePrice),
			})
			continue
		}

		detailedPos := DetailedPosition{
			Strike:       strikePriceFloat,
			OptionType:   optionType,
			Quantity:     int64(pos.Quantity),
			AveragePrice: pos.AveragePrice,
			SellPrice:    pos.SellPrice,
			LTP:          ltp,
			Diff:         ltp - pos.AveragePrice,
			VWAP:         0,
			Value:        math.Abs(float64(pos.Quantity) * pos.AveragePrice),
		}

		// Calculate moves
		detailedPos.Moves = pm.calculateMoves(ctx, pos)

		// Update summary
		if optionType == "CE" {
			analysis.Summary.TotalCallValue += detailedPos.Value
		} else {
			analysis.Summary.TotalPutValue += detailedPos.Value
		}

		analysis.Positions = append(analysis.Positions, detailedPos)
	}

	// Process positions from database (including paper trading positions)
	for _, dbPos := range dbPositions {
		// Skip positions already processed from Zerodha API (to avoid duplicates)
		if !dbPos.PaperTrading {
			// Skip real trading positions as they're already included from Zerodha API
			continue
		}
		
		// Skip non-option positions
		if !isOptionPosition(dbPos.TradingSymbol) {
			continue
		}
		
		// Get option type
		optionType := getOptionType(dbPos.TradingSymbol)
		
		// Get current LTP
		var ltp float64 = dbPos.LastPrice
		
		// Try to get instrument token from cache
		instrumentToken, exists := inmemoryCache.Get(dbPos.TradingSymbol)
		if exists {
			// Try to get LTP from Redis using instrument token
			ltpKey := fmt.Sprintf("%v_ltp", instrumentToken)
			ltpVal, err := ltpDB.Get(ctx, ltpKey).Float64()
			if err == nil && ltpVal > 0 {
				ltp = ltpVal
			}
		}
		
		// Extract strike price from trading symbol
		var strikePriceFloat float64
		parts := strings.Split(dbPos.TradingSymbol, "")
		for i := 0; i < len(parts); i++ {
			if len(parts[i]) > 0 && parts[i][0] >= '0' && parts[i][0] <= '9' {
				// Found a number, try to parse it as strike price
				strikePriceStr := ""
				for j := i; j < len(parts); j++ {
					if len(parts[j]) > 0 && ((parts[j][0] >= '0' && parts[j][0] <= '9') || parts[j][0] == '.') {
						strikePriceStr += parts[j]
					} else {
						break
					}
				}
				strikePriceFloat, _ = strconv.ParseFloat(strikePriceStr, 64)
				break
			}
		}
		
		// If we couldn't extract strike price, use a default value
		if strikePriceFloat == 0 {
			strikePriceFloat = dbPos.AveragePrice
		}
		
		detailedPos := DetailedPosition{
			Strike:       strikePriceFloat,
			OptionType:   optionType,
			Quantity:     int64(dbPos.Quantity),
			AveragePrice: dbPos.AveragePrice,
			SellPrice:    dbPos.SellPrice,
			LTP:          ltp,
			Diff:         ltp - dbPos.AveragePrice,
			VWAP:         0,
			Value:        math.Abs(float64(dbPos.Quantity) * dbPos.AveragePrice),
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

func isOptionPosition(symbol string) bool {
	return len(symbol) > 2 && (strings.HasSuffix(symbol, "CE") || strings.HasSuffix(symbol, "PE"))
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

	// Set strike gap based on index name from token
	strikeGap := 50.0
	indexName, err := pm.kite.GetIndexNameFromToken(ctx, fmt.Sprintf("%d", pos.InstrumentToken))
	if err == nil {
		if indexName == "BANKNIFTY" || indexName == "SENSEX" {
			strikeGap = 100.0
		}
	}

	// Extract strike price from cache using the same key as population
	inmemoryCache := cache.GetInMemoryCacheInstance()
	strikeKey := fmt.Sprintf("strike:%d", pos.InstrumentToken)
	strikePrice, ok := inmemoryCache.Get(strikeKey)
	if !ok {
		pm.log.Error("Failed to get strike price for moves calculation", map[string]interface{}{
			"token": pos.InstrumentToken,
			"key":   strikeKey,
		})
		return moves
	}

	var strike float64
	switch v := strikePrice.(type) {
	case float64:
		strike = v
	case string:
		var err error
		strike, err = strconv.ParseFloat(v, 64)
		if err != nil {
			pm.log.Error("Failed to parse strike price string for moves calculation", map[string]interface{}{
				"token": pos.InstrumentToken,
				"value": v,
				"error": err.Error(),
			})
			return moves
		}
	default:
		pm.log.Error("Invalid strike price type for moves calculation", map[string]interface{}{
			"token": pos.InstrumentToken,
			"type":  fmt.Sprintf("%T", strikePrice),
		})
		return moves
	}

	// Away moves (2 strikes)
	for i := 1; i <= 2; i++ {
		if strings.HasSuffix(pos.Tradingsymbol, "CE") {
			newStrike := strike + (strikeGap * float64(i))
			moves.Away = append(moves.Away, MoveStep{
				Strike:  newStrike,
				Premium: pos.LastPrice * 0.8, // Simplified premium calculation
				Steps:   steps,
			})
		} else {
			newStrike := strike - (strikeGap * float64(i))
			moves.Away = append(moves.Away, MoveStep{
				Strike:  newStrike,
				Premium: pos.LastPrice * 0.8,
				Steps:   steps,
			})
		}
	}

	// Closer moves (2 strikes)
	for i := 1; i <= 2; i++ {
		if strings.HasSuffix(pos.Tradingsymbol, "CE") {
			newStrike := strike - (strikeGap * float64(i))
			moves.Closer = append(moves.Closer, MoveStep{
				Strike:  newStrike,
				Premium: pos.LastPrice * 1.2,
				Steps:   steps,
			})
		} else {
			newStrike := strike + (strikeGap * float64(i))
			moves.Closer = append(moves.Closer, MoveStep{
				Strike:  newStrike,
				Premium: pos.LastPrice * 1.2,
				Steps:   steps,
			})
		}
	}

	return moves
}
