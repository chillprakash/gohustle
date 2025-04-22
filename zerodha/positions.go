package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	sync "sync"

	"gohustle/cache"
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

// PollPositionsAndUpdateInRedis periodically polls positions and updates Redis
func (pm *PositionManager) PollPositionsAndUpdateInRedis(ctx context.Context) error {
	positions, err := pm.GetOpenPositions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	// Store net positions alone
	if err := pm.storePositionsInRedis(ctx, "net", positions.Net); err != nil {
		return err
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
			pm.log.Info("Stored token quantity", map[string]interface{}{
				"token":    pos.InstrumentToken,
				"quantity": pos.Quantity,
			})
		}
	}
	return nil
}

// GetPositionAnalysis returns detailed analysis of all positions
func (pm *PositionManager) GetPositionAnalysis(ctx context.Context) (*PositionAnalysis, error) {
	positions, err := pm.GetOpenPositions(ctx)
	inmemoryCache := cache.GetInMemoryCacheInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	// Get Redis cache for LTP data
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis cache: %w", err)
	}
	ltpDB := redisCache.GetLTPDB3()

	analysis := &PositionAnalysis{
		Summary:   PositionSummary{},
		Positions: make([]DetailedPosition, 0),
	}

	// Process each position
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

	// Add nearby strikes (this is a simplified version - you might want to add more logic)
	strikeGap := 50.0 // Assuming 50-point strikes

	// Extract strike price from trading symbol
	inmemoryCache := cache.GetInMemoryCacheInstance()
	strike_key := fmt.Sprintf("strike:%d", pos.InstrumentToken)
	strikePrice, ok := inmemoryCache.Get(strike_key)
	if !ok {
		pm.log.Error("Failed to get strike price for moves calculation", map[string]interface{}{
			"token":  pos.InstrumentToken,
			"symbol": pos.Tradingsymbol,
			"key":    strike_key,
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
			pm.log.Error("Failed to parse strike price for moves calculation", map[string]interface{}{
				"token":  pos.InstrumentToken,
				"strike": v,
				"error":  err.Error(),
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
