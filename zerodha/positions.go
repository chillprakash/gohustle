package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/logger"
	"gohustle/utils"

	"github.com/redis/go-redis/v9"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type positions struct {
	ID              int64     `json:"id" db:"id"`
	InstrumentToken uint32    `json:"instrument_token" db:"instrument_token"`
	TradingSymbol   string    `json:"trading_symbol" db:"trading_symbol"`
	Exchange        string    `json:"exchange" db:"exchange"`
	Product         string    `json:"product" db:"product"`
	BuyPrice        float64   `json:"buy_price" db:"buy_price"`
	BuyValue        float64   `json:"buy_value" db:"buy_value"`
	BuyQuantity     int       `json:"buy_quantity" db:"buy_quantity"`
	SellPrice       float64   `json:"sell_price" db:"sell_price"`
	SellValue       float64   `json:"sell_value" db:"sell_value"`
	SellQuantity    int       `json:"sell_quantity" db:"sell_quantity"`
	Multiplier      float64   `json:"multiplier" db:"multiplier"`
	AveragePrice    float64   `json:"average_price" db:"average_price"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time `json:"updated_at" db:"updated_at"`
}

const (
	PositionsKeyFormat     = "position:all_positions" // List of all positions
	PositionTokenKeyFormat = "position:token:%d"      // instrumentToken
	PostionsJSONKeyFormat  = "positionsdump:real"     // Positions dump
)

type PositionManager struct {
	kite                *KiteConnect
	log                 *logger.Logger
	positionsRedis      *redis.Client
	cacheMetaInstance   *cache.CacheMeta
	appParameterManager *appparameters.AppParameterManager
}

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
		appParameterManager := appparameters.GetAppParameterManager()
		if appParameterManager == nil {
			log.Error("Failed to get AppParameterManager instance", map[string]interface{}{})
			return
		}
		cacheMetaInstance, err := cache.GetCacheMetaInstance()
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
			log:                 log,
			kite:                kite,
			positionsRedis:      redisCache.GetPositionsDB2(),
			cacheMetaInstance:   cacheMetaInstance,
			appParameterManager: appParameterManager,
		}
		log.Info("Position manager initialized", map[string]interface{}{})
	})
	return positionInstance
}

func (pm *PositionManager) CreatePositions(ctx context.Context, orders []Order, indexMeta *cache.InstrumentData, side Side) error {
	if pm == nil || pm.positionsRedis == nil {
		return fmt.Errorf("position manager or redis client not initialized")
	}
	productType := pm.appParameterManager.GetOrderAppParameters().ProductType

	ltpData, err := pm.cacheMetaInstance.GetLTPforInstrumentToken(ctx, utils.Uint32ToString(indexMeta.InstrumentToken))
	if err != nil {
		return err
	}

	positionsList := []positions{}
	for _, order := range orders {
		position := positions{
			InstrumentToken: indexMeta.InstrumentToken,
			TradingSymbol:   indexMeta.TradingSymbol,
			Exchange:        indexMeta.Exchange,
			Product:         string(productType),
			Multiplier:      1,
			CreatedAt:       time.Now(),
			AveragePrice:    ltpData.LTP,
		}

		if side == SideBuy {
			position.BuyQuantity = order.Quantity
			position.BuyValue = float64(order.Quantity) * ltpData.LTP

		} else {
			position.SellQuantity = order.Quantity
			position.SellValue = float64(order.Quantity) * ltpData.LTP
		}

		pm.log.Info("Creating positions", map[string]interface{}{
			"position": position,
		})
		positionsList = append(positionsList, position)
	}

	_, err = storePositionsToRedis(ctx, positionsList)
	return err
}

func (pm *PositionManager) ListPositionsFromDB(ctx context.Context) ([]positions, error) {
	if pm == nil {
		return nil, fmt.Errorf("position manager not initialized")
	}

	var positionsList []positions

	// Debug: Log that we're looking for positions
	pm.log.Debug("Looking for positions in Redis", map[string]interface{}{})

	// DIRECT APPROACH: Get all position keys directly from the hash
	// This bypasses the need to use the consolidated key list which might be inconsistent
	positionKeys, err := pm.positionsRedis.HKeys(ctx, "positions").Result()
	if err != nil {
		pm.log.Error("Failed to get position keys from hash", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get position keys: %w", err)
	}

	// Log what we found
	pm.log.Debug("Found positions in Redis hash", map[string]interface{}{
		"count": len(positionKeys),
		"sample_keys": func() []string {
			if len(positionKeys) > 5 {
				return positionKeys[:5] // Just show first 5 keys
			}
			return positionKeys
		}(),
	})

	// If no positions found, return empty list
	if len(positionKeys) == 0 {
		pm.log.Debug("No positions found in Redis", map[string]interface{}{})
		return positionsList, nil
	}

	// Get all positions in a pipeline
	pipe := pm.positionsRedis.Pipeline()
	cmds := make([]*redis.StringCmd, len(positionKeys))

	for i, key := range positionKeys {
		// Get the position JSON from the hash
		cmds[i] = pipe.HGet(ctx, "positions", key)
	}

	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	// Process results
	for i, cmd := range cmds {
		posJSON, err := cmd.Result()
		if err != nil {
			if err != redis.Nil {
				pm.log.Error("Failed to get position", map[string]interface{}{
					"key":   positionKeys[i],
					"error": err.Error(),
				})
			}
			continue
		}

		// Skip empty results
		if posJSON == "" {
			continue
		}

		// Parse the JSON into a map
		var posData map[string]interface{}
		if err := json.Unmarshal([]byte(posJSON), &posData); err != nil {
			pm.log.Error("Failed to parse position JSON", map[string]interface{}{
				"key":   positionKeys[i],
				"error": err.Error(),
			})
			continue
		}

		// Create a position object from the parsed data
		var pos positions

		// Generate a unique ID for this position
		pos.ID = time.Now().UnixNano()

		// Extract the token from the position data
		if token, ok := posData["instrument_token"].(float64); ok {
			pos.InstrumentToken = uint32(token)
		}

		// Extract string fields
		if symbol, ok := posData["tradingsymbol"].(string); ok {
			pos.TradingSymbol = symbol
		}
		if exchange, ok := posData["exchange"].(string); ok {
			pos.Exchange = exchange
		}
		if product, ok := posData["product"].(string); ok {
			pos.Product = product
		}

		// Extract numeric fields
		if qty, ok := posData["quantity"].(float64); ok {
			if qty > 0 {
				pos.BuyQuantity = int(qty)
				pos.SellQuantity = 0
			} else {
				pos.SellQuantity = int(math.Abs(qty))
				pos.BuyQuantity = 0
			}
		}

		if avgPrice, ok := posData["average_price"].(float64); ok {
			pos.AveragePrice = avgPrice
		}

		// Calculate buy/sell values based on quantity and prices
		if pos.BuyQuantity > 0 {
			pos.BuyPrice = pos.AveragePrice
			pos.BuyValue = pos.BuyPrice * float64(pos.BuyQuantity)
		} else if pos.SellQuantity > 0 {
			pos.SellPrice = pos.AveragePrice
			pos.SellValue = pos.SellPrice * float64(pos.SellQuantity)
		}

		// Set timestamps
		pos.CreatedAt = time.Now().Add(-24 * time.Hour) // Assume created yesterday
		pos.UpdatedAt = time.Now()

		positionsList = append(positionsList, pos)
	}

	return positionsList, nil
}

// storePositionsToRedis stores or updates positions in Redis
// Uses Redis hash to store position data
func storePositionsToRedis(ctx context.Context, positions []positions) ([]int64, error) {
	if len(positions) == 0 {
		return []int64{}, nil
	}

	log := logger.L()
	log.Info("Storing positions to Redis", map[string]interface{}{
		"positions_count": len(positions),
	})

	// Get Redis client
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis cache: %w", err)
	}

	// Use standard key prefix for positions
	keyPrefix := "position:"
	allPositionsKey := PositionsKeyFormat

	var positionIDs []int64
	pipe := redisCache.GetCacheDB1().Pipeline()

	// Store each position
	for _, pos := range positions {
		now := time.Now()

		// Generate a unique ID if not present
		if pos.ID == 0 {
			// Use timestamp + instrument token as a simple ID generation mechanism
			pos.ID = time.Now().UnixNano() + int64(pos.InstrumentToken)
		}

		// Create position key
		posKey := fmt.Sprintf("%s%s:%s:%s", keyPrefix, pos.TradingSymbol, pos.Exchange, pos.Product)

		// Convert position to map for Redis hash
		posMap := map[string]interface{}{
			"id":               pos.ID,
			"instrument_token": pos.InstrumentToken,
			"trading_symbol":   pos.TradingSymbol,
			"exchange":         pos.Exchange,
			"product":          pos.Product,
			"buy_price":        pos.BuyPrice,
			"buy_value":        pos.BuyValue,
			"buy_quantity":     pos.BuyQuantity,
			"sell_price":       pos.SellPrice,
			"sell_value":       pos.SellValue,
			"sell_quantity":    pos.SellQuantity,
			"multiplier":       pos.Multiplier,
			"average_price":    pos.AveragePrice,
			"created_at":       pos.CreatedAt.Format(time.RFC3339),
			"updated_at":       now.Format(time.RFC3339),
		}

		// Store position in Redis hash
		pipe.HSet(ctx, posKey, posMap)

		// Set expiration (keep positions for 30 days)
		pipe.Expire(ctx, posKey, 30*24*time.Hour)

		// Add to all positions set
		pipe.SAdd(ctx, allPositionsKey, posKey)

		// Add to position token mapping
		tokenKey := fmt.Sprintf(PositionTokenKeyFormat, pos.InstrumentToken)
		pipe.Set(ctx, tokenKey, posKey, 30*24*time.Hour)

		positionIDs = append(positionIDs, pos.ID)
	}

	// Execute pipeline
	_, err = pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to store positions in Redis: %w", err)
	}

	return positionIDs, nil
}

func (pm *PositionManager) GetOpenPositionTokensVsQuanityFromRedis(ctx context.Context) (map[string]CachedRedisPostitionsAndQuantity, error) {
	if pm == nil || pm.positionsRedis == nil {
		return nil, fmt.Errorf("position manager or redis client not initialized")
	}

	// Initialize result map with instrument token as key
	cachePositionsMap := make(map[string]CachedRedisPostitionsAndQuantity)

	// Get the comma-separated position data from Redis
	allPositions, err := pm.positionsRedis.Get(ctx, PositionsKeyFormat).Result()

	if err == redis.Nil {
		pm.log.Debug("No positions found in Redis", map[string]interface{}{
			"key": PositionsKeyFormat,
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
	wg.Add(1)

	// Channel to collect errors from goroutines
	errChan := make(chan error, 1)

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

// storePositionsInRedis stores a list of positions in Redis, removing any stale positions
func (pm *PositionManager) storePositionsInRedis(ctx context.Context, positions []kiteconnect.Position) error {
	// Filter out positions with zero quantity (closed positions)
	activePositions := make([]kiteconnect.Position, 0, len(positions))

	// Create a map of all tokens from the API response for stale detection
	allTokensFromAPI := make(map[uint32]bool)

	// Process all positions from API
	for _, pos := range positions {
		// Mark this token as seen in the API response
		allTokensFromAPI[uint32(pos.InstrumentToken)] = true

		// Only keep positions with non-zero quantity
		if pos.Quantity != 0 {
			activePositions = append(activePositions, pos)
		} else {
			// This is a closed position, log it
			pm.log.Debug("Skipping closed position", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"token":  pos.InstrumentToken,
			})
		}
	}

	// Get existing position data from PositionsKeyFormat
	existingPositionsStr, err := pm.positionsRedis.Get(ctx, PositionsKeyFormat).Result()
	if err != nil && err != redis.Nil {
		pm.log.Error("Failed to get existing positions", map[string]interface{}{
			"error": err.Error(),
		})
		// Continue with storing new positions even if we can't get existing keys
	}

	// Parse existing positions into a map for tracking
	existingTokens := make(map[uint32]bool)
	if existingPositionsStr != "" {
		// Parse the comma-separated list of instrument_token_quantity
		existingPositionValues := strings.Split(existingPositionsStr, ",")
		for _, posValue := range existingPositionValues {
			parts := strings.Split(posValue, "_")
			if len(parts) == 2 {
				tokenStr := parts[0]
				token, err := strconv.ParseUint(tokenStr, 10, 32)
				if err == nil {
					existingTokens[uint32(token)] = true
				}
			}
		}
	}

	// Process each active position
	positionValues := make([]string, 0, len(activePositions))
	for _, pos := range activePositions {
		// Create a simplified position object with only relevant fields
		relevantPos := map[string]interface{}{
			"instrument_token": pos.InstrumentToken,
			"tradingsymbol":    pos.Tradingsymbol,
			"quantity":         pos.Quantity,
			"average_price":    pos.AveragePrice,
			"last_price":       pos.LastPrice,
			"pnl":              pos.PnL,
			"product":          pos.Product,
			"exchange":         pos.Exchange,
		}

		// Convert position to JSON
		posJSON, err := json.Marshal(relevantPos)
		if err != nil {
			pm.log.Error("Failed to marshal position", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}

		// Create key for this position
		key := fmt.Sprintf("position:%s:%d", pos.Tradingsymbol, pos.InstrumentToken)

		// Store position in Redis
		err = pm.positionsRedis.HSet(ctx, "positions", key, string(posJSON)).Err()
		if err != nil {
			pm.log.Error("Failed to store position in Redis", map[string]interface{}{
				"symbol": pos.Tradingsymbol,
				"error":  err.Error(),
			})
			continue
		}

		// Mark this token as processed (not stale)
		existingTokens[uint32(pos.InstrumentToken)] = false

		// Add to the position values list for the consolidated key
		positionValues = append(positionValues, fmt.Sprintf("%d_%d", pos.InstrumentToken, pos.Quantity))
	}

	// Find and remove stale positions that are no longer in the API response or have zero quantity
	// First, get all position keys from Redis
	positionKeys, err := pm.positionsRedis.HKeys(ctx, "positions").Result()
	if err != nil {
		pm.log.Error("Failed to get position keys from Redis", map[string]interface{}{
			"error": err.Error(),
		})
	} else {
		// Check each position in Redis to see if it's still active
		for _, key := range positionKeys {
			// Extract the token from the key (format: position:{tradingsymbol}:{token})
			keyParts := strings.Split(key, ":")
			if len(keyParts) >= 3 {
				tokenStr := keyParts[2]
				token, err := strconv.ParseUint(tokenStr, 10, 32)
				if err != nil {
					pm.log.Error("Failed to parse token from key", map[string]interface{}{
						"key":   key,
						"token": tokenStr,
						"error": err.Error(),
					})
					continue
				}

				// Check if this token is in the API response and has non-zero quantity
				if !allTokensFromAPI[uint32(token)] {
					// This position is stale (not in API response at all)
					pm.log.Debug("Removing stale position (not in API)", map[string]interface{}{
						"key":   key,
						"token": token,
					})

					// Remove from Redis
					err := pm.positionsRedis.HDel(ctx, "positions", key).Err()
					if err != nil {
						pm.log.Error("Failed to remove stale position", map[string]interface{}{
							"key":   key,
							"error": err.Error(),
						})
					}
				}
			}
		}
	}

	// Store the consolidated list of positions
	positionAllKeyFormatValue := strings.Join(positionValues, ",")

	// Log the consolidated position list for debugging
	pm.log.Debug("Storing consolidated position list", map[string]interface{}{
		"count": len(positionValues),
		"value": positionAllKeyFormatValue,
	})

	err = pm.positionsRedis.Set(ctx, PositionsKeyFormat, positionAllKeyFormatValue, 0).Err()
	if err != nil {
		pm.log.Error("Failed to store all positions in Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Store a timestamp of when positions were last updated
	err = pm.positionsRedis.Set(ctx, "positions:last_updated", time.Now().Unix(), 0).Err()
	if err != nil {
		pm.log.Error("Failed to update positions timestamp", map[string]interface{}{
			"error": err.Error(),
		})
	}

	return nil
}

// GetPositionAnalysis returns detailed analysis of positions
func (pm *PositionManager) GetPositionAnalysis(ctx context.Context) (*PositionAnalysis, error) {
	// Initialize analysis structure
	analysis := &PositionAnalysis{
		Summary:         PositionSummary{},
		OpenPositions:   make([]DetailedPosition, 0),
		ClosedPositions: make([]DetailedPosition, 0),
	}

	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		return nil, fmt.Errorf("failed to get cache meta instance: %w", err)
	}

	// Fetch positions from Redis
	allPositions, err := pm.ListPositionsFromDB(ctx)
	if err != nil {
		pm.log.Error("Failed to fetch positions from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("failed to get positions from Redis: %w", err)
	}

	pm.log.Debug("Fetched positions from Redis", map[string]interface{}{
		"count": len(allPositions),
	})

	// Process all positions from Redis
	for _, dbPos := range allPositions {
		// Skip non-option positions
		if !isOptionPosition(dbPos.TradingSymbol) {
			continue
		}

		ltpStruct, _ := cacheMeta.GetLTPforInstrumentToken(ctx, utils.Uint32ToString(dbPos.InstrumentToken))
		tokenMeta, _ := cacheMeta.GetMetadataOfToken(ctx, utils.Uint32ToString(dbPos.InstrumentToken))

		// Create a kiteconnect.Position from the db.PositionRecord for move calculation
		pseudoPos := kiteconnect.Position{
			Tradingsymbol:   dbPos.TradingSymbol,
			Exchange:        dbPos.Exchange,
			InstrumentToken: dbPos.InstrumentToken,
			Product:         dbPos.Product,
			Quantity:        dbPos.BuyQuantity - dbPos.SellQuantity,
			AveragePrice:    dbPos.AveragePrice,
			LastPrice:       ltpStruct.LTP,
			ClosePrice:      0,
			PnL:             0,
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
			OptionType:      string(tokenMeta.InstrumentType),
			Quantity:        int64(dbPos.BuyQuantity - dbPos.SellQuantity),
			AveragePrice:    dbPos.AveragePrice,
			BuyPrice:        dbPos.BuyPrice,
			SellPrice:       dbPos.SellPrice,
			LTP:             ltpStruct.LTP,
			Diff:            ltpStruct.LTP - dbPos.AveragePrice,
			Value:           math.Abs(float64(dbPos.BuyQuantity-dbPos.SellQuantity) * dbPos.AveragePrice),
			InstrumentToken: tokenMeta.Token,
			Moves:           pm.calculateMoves(ctx, pseudoPos, tokenMeta),
		}

		// Calculate position value (original capital deployed)
		quantity := float64(dbPos.BuyQuantity - dbPos.SellQuantity)
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
			"position_type":  tokenMeta.InstrumentType,
		})

		// Update summary based on option type
		if tokenMeta.InstrumentType == "CE" {
			analysis.Summary.TotalCallValue += positionValue
			analysis.Summary.TotalCallPending += pendingValue
		} else {
			analysis.Summary.TotalPutValue += positionValue
			analysis.Summary.TotalPutPending += pendingValue
		}

		// Separate open and closed positions
		if quantity != 0 {
			analysis.OpenPositions = append(analysis.OpenPositions, detailedPos)
		} else {
			analysis.ClosedPositions = append(analysis.ClosedPositions, detailedPos)
		}
	}

	// Calculate totals
	analysis.Summary.TotalValue = analysis.Summary.TotalCallValue + analysis.Summary.TotalPutValue
	analysis.Summary.TotalPendingValue = analysis.Summary.TotalCallPending + analysis.Summary.TotalPutPending

	// Log position counts
	pm.log.Debug("Position analysis complete", map[string]interface{}{
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

// calculateMoves calculates potential position adjustment moves
func (pm *PositionManager) calculateMoves(ctx context.Context, pos kiteconnect.Position, tokenMeta cache.InstrumentData) MoveSuggestions {
	moves := MoveSuggestions{
		Away:   make([]MoveStep, 0, 3), // Pre-allocate based on expected max moves
		Closer: make([]MoveStep, 0, 3),
	}

	if pm.cacheMetaInstance == nil {
		pm.log.Error("Failed to get cache meta instance for move calculation",
			map[string]interface{}{"error": "cache meta instance not initialized"})
		return moves
	}

	steps := []string{"1/4", "1/2", "1"}
	strikeGap := float64(tokenMeta.Name.StrikeGap)
	strike := utils.StringToFloat64(tokenMeta.StrikePrice)
	optionType := tokenMeta.InstrumentType

	// Process moves in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Process away moves (2 strikes)
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(step int) {
			defer wg.Done()
			newStrike := strike + (float64(step) * strikeGap)
			pm.processMove(ctx, &moves, &mu, newStrike, optionType, tokenMeta.Expiry, steps, true)
		}(i)
	}

	// Process closer moves (2 strikes)
	for i := 1; i <= 2; i++ {
		wg.Add(1)
		go func(step int) {
			defer wg.Done()
			newStrike := strike - (float64(step) * strikeGap)
			pm.processMove(ctx, &moves, &mu, newStrike, optionType, tokenMeta.Expiry, steps, false)
		}(i)
	}

	wg.Wait()
	return moves
}

// processMove handles the processing of a single move
func (pm *PositionManager) processMove(ctx context.Context, moves *MoveSuggestions, mu *sync.Mutex,
	strike float64, optionType cache.InstrumentType, expiry string, steps []string, isAway bool) {

	tokenStr, err := pm.cacheMetaInstance.GetInstrumentTokenForStrike(ctx, strike, optionType, expiry)
	if err != nil {
		pm.log.Debug("No instrument token found for strike",
			map[string]interface{}{
				"strike":      strike,
				"option_type": optionType,
				"expiry":      expiry,
				"error":       err.Error(),
			})
		return
	}

	move := MoveStep{
		Strike:          strike,
		Steps:           steps,
		InstrumentToken: tokenStr,
		Premium:         getPremium(tokenStr),
	}

	mu.Lock()
	defer mu.Unlock()
	if isAway {
		moves.Away = append(moves.Away, move)
	} else {
		moves.Closer = append(moves.Closer, move)
	}
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
