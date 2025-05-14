package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// OrderManager handles order status polling and updates
type OrderManager struct {
	log  *logger.Logger
	kite *KiteConnect
}

var (
	orderManagerInstance *OrderManager
	orderManagerOnce     sync.Once
)

// GetOrderManager returns a singleton instance of OrderManager
func GetOrderManager() *OrderManager {
	orderManagerOnce.Do(func() {
		log := logger.L()
		kite := GetKiteConnect()

		if kite == nil {
			log.Error("Failed to initialize order manager: KiteConnect is nil", map[string]interface{}{})
			return
		}

		orderManagerInstance = &OrderManager{
			log:  log,
			kite: kite,
		}
		log.Info("Order manager initialized", map[string]interface{}{})
	})
	return orderManagerInstance
}

// GetOpenOrders fetches all open orders from Zerodha
func (om *OrderManager) GetOpenOrders(ctx context.Context) ([]kiteconnect.Order, error) {
	if om == nil || om.kite == nil {
		return nil, fmt.Errorf("order manager or kite client not initialized")
	}

	orders, err := om.kite.Kite.GetOrders()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch orders: %w", err)
	}
	return orders, nil
}

// PollOrdersAndUpdateInRedis periodically polls orders and updates Redis
func (om *OrderManager) PollOrdersAndUpdateInRedis(ctx context.Context) error {
	orders, err := om.GetOpenOrders(ctx)
	if err != nil {
		return fmt.Errorf("failed to get orders: %w", err)
	}

	// If no orders to process, return early
	if len(orders) == 0 {
		return nil
	}

	// Use a wait group to wait for both operations to complete
	var wg sync.WaitGroup
	wg.Add(2)

	// Channel to collect errors from goroutines
	errChan := make(chan error, 2)

	// Create a separate context for Redis operations with a longer timeout
	redisCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Store orders in Redis concurrently
	go func() {
		defer wg.Done()
		if err := om.storeOrdersInRedis(redisCtx, orders); err != nil {
			om.log.Error("Failed to store orders in Redis", map[string]interface{}{
				"error": err.Error(),
				"count": len(orders),
			})
			errChan <- err
		}
	}()

	// Update the database with the latest order statuses concurrently
	go func() {
		defer wg.Done()
		if err := om.updateOrderStatusesInDB(orders); err != nil {
			om.log.Error("Failed to update order statuses in DB", map[string]interface{}{
				"error": err.Error(),
				"count": len(orders),
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

// storeOrdersInRedis stores orders in Redis for quick access
func (om *OrderManager) storeOrdersInRedis(ctx context.Context, orders []kiteconnect.Order) error {
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return fmt.Errorf("failed to get Redis cache: %w", err)
	}

	// Use LTP DB for orders as well (since we don't have a dedicated orders DB)
	ordersDB := redisCache.GetLTPDB3()
	if ordersDB == nil {
		return fmt.Errorf("orders Redis DB is nil")
	}

	// Create a context with a more generous timeout for Redis operations
	// Increased from 500ms to 3s to handle potential Redis latency
	redisCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	// Process orders in batches to avoid overwhelming Redis
	const batchSize = 10
	for i := 0; i < len(orders); i += batchSize {
		end := i + batchSize
		if end > len(orders) {
			end = len(orders)
		}
		batch := orders[i:end]

		// Process this batch of orders
		for _, order := range batch {
			// Store order status with retry logic
			statusKey := fmt.Sprintf("order_%s_status", order.OrderID)

			// Try up to 3 times with exponential backoff
			var redisErr error
			for attempt := 0; attempt < 3; attempt++ {
				redisErr = ordersDB.Set(redisCtx, statusKey, order.Status, 24*time.Hour).Err()
				if redisErr == nil {
					break // Success
				}

				// If context deadline exceeded, don't retry
				if strings.Contains(redisErr.Error(), "context deadline exceeded") {
					break
				}

				// Exponential backoff before retry
				backoff := time.Duration(50*(1<<attempt)) * time.Millisecond
				time.Sleep(backoff)
			}

			if redisErr != nil {
				om.log.Error("Failed to store order status in Redis after retries", map[string]interface{}{
					"order_id": order.OrderID,
					"status":   order.Status,
					"error":    redisErr.Error(),
				})
				continue
			}

			// Store the entire order as JSON for detailed information
			orderKey := fmt.Sprintf("order_%s", order.OrderID)
			orderJSON, err := json.Marshal(order)
			if err != nil {
				om.log.Error("Failed to marshal order to JSON", map[string]interface{}{
					"order_id": order.OrderID,
					"error":    err.Error(),
				})
				continue
			}

			// Try up to 3 times with exponential backoff for storing JSON
			for attempt := 0; attempt < 3; attempt++ {
				redisErr = ordersDB.Set(redisCtx, orderKey, orderJSON, 24*time.Hour).Err()
				if redisErr == nil {
					break // Success
				}

				// If context deadline exceeded, don't retry
				if strings.Contains(redisErr.Error(), "context deadline exceeded") {
					break
				}

				// Exponential backoff before retry
				backoff := time.Duration(50*(1<<attempt)) * time.Millisecond
				time.Sleep(backoff)
			}

			if redisErr != nil {
				om.log.Error("Failed to store order JSON in Redis after retries", map[string]interface{}{
					"order_id": order.OrderID,
					"error":    redisErr.Error(),
				})
			}
		}

		// Add a small delay between batches to avoid overwhelming Redis
		if i+batchSize < len(orders) {
			time.Sleep(50 * time.Millisecond)
		}
	}

	return nil
}

// updateOrderStatusesInDB updates the order statuses in the database
func (om *OrderManager) updateOrderStatusesInDB(orders []kiteconnect.Order) error {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	for _, order := range orders {
		// Skip paper trading orders (they won't have a matching Zerodha order ID)
		if len(order.OrderID) > 6 && order.OrderID[:6] == "paper-" {
			continue
		}

		// Update the order status in the database
		// For now, just log that we would update the status
		// You'll need to implement the actual DB update method
		om.log.Debug("Would update order status in DB", map[string]interface{}{
			"order_id":        order.OrderID,
			"status":          order.Status,
			"filled_quantity": order.FilledQuantity,
		})

		// TODO: Implement the actual DB update when the method is available
		// if err := timescaleDB.UpdateOrderStatus(ctx, order.OrderID, order.Status, order.FilledQuantity); err != nil {
		// 	om.log.Error("Failed to update order status in DB", map[string]interface{}{
		// 		"order_id": order.OrderID,
		// 		"status":   order.Status,
		// 		"error":    err.Error(),
		// 	})
		// }
	}

	return nil
}

// GetOrderStatus gets the status of an order from Redis or Zerodha
func (om *OrderManager) GetOrderStatus(ctx context.Context, orderID string) (string, error) {
	// First try to get from Redis
	redisCache, err := cache.GetRedisCache()
	if err != nil {
		return "", fmt.Errorf("failed to get Redis cache: %w", err)
	}

	ordersDB := redisCache.GetLTPDB3()
	if ordersDB == nil {
		return "", fmt.Errorf("orders Redis DB is nil")
	}

	// Create a context with timeout for Redis operations
	redisCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Try to get from Redis first
	statusKey := fmt.Sprintf("order_%s_status", orderID)
	status, err := ordersDB.Get(redisCtx, statusKey).Result()
	if err == nil && status != "" {
		return status, nil
	}

	// If not in Redis, try to get from Zerodha
	// Skip for paper trading orders
	if len(orderID) > 6 && orderID[:6] == "paper-" {
		return "PAPER", nil
	}

	// Get order details from Zerodha
	order, err := om.kite.Kite.GetOrderHistory(orderID)
	if err != nil {
		return "", fmt.Errorf("failed to get order history: %w", err)
	}

	if len(order) == 0 {
		return "", fmt.Errorf("no order found with ID: %s", orderID)
	}

	// Return the status of the most recent order update
	return order[len(order)-1].Status, nil
}

// OrderType represents the type of order (MARKET, LIMIT, etc.)
type OrderType string

// OrderSide represents whether the order is BUY or SELL
type OrderSide string

// ProductType represents the product type (NRML, MIS, CNC)
type ProductType string

const (
	// Order Types
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"
	OrderTypeSL     OrderType = "SL"
	OrderTypeSLM    OrderType = "SL-M"

	// Order Sides
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"

	// Product Types
	ProductTypeNRML ProductType = "NRML"
	ProductTypeMIS  ProductType = "MIS"
	ProductTypeCNC  ProductType = "CNC"
)

// PlaceOrderRequest represents the parameters for placing an order
type PlaceOrderRequest struct {
	TradingSymbol string      `json:"trading_symbol"`
	Exchange      string      `json:"exchange"`
	OrderType     OrderType   `json:"order_type"`
	Side          OrderSide   `json:"side"`
	Quantity      int         `json:"quantity"`
	Price         float64     `json:"price,omitempty"`
	TriggerPrice  float64     `json:"trigger_price,omitempty"`
	Product       ProductType `json:"product"`
	Validity      string      `json:"validity,omitempty"`
	DisclosedQty  int         `json:"disclosed_qty,omitempty"`
	Tag           string      `json:"tag,omitempty"`
}

// OrderResponse represents a simplified response for order placement
// (expand as needed for your UI)
type OrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// ToOrderRecord converts order request/response to db.OrderRecord for persistence
func ToOrderRecord(orderReq interface{}, resp *OrderResponse, userID string, kiteResp interface{}) *db.OrderRecord {
	var (
		orderType, gttType, tradingSymbol, exchange, side, product, validity, tag string
		quantity, disclosedQty                                                    int
		price, triggerPrice                                                       float64
		paperTrading                                                              bool
	)
	placedAt := time.Now()

	// Check if this is a paper trading order based on the status
	paperTrading = resp.Status == "PAPER"

	switch req := orderReq.(type) {
	case PlaceOrderRequest:
		orderType = string(req.OrderType)
		tradingSymbol = req.TradingSymbol
		exchange = req.Exchange
		side = string(req.Side)
		quantity = req.Quantity
		price = req.Price
		triggerPrice = req.TriggerPrice
		product = string(req.Product)
		validity = req.Validity
		disclosedQty = req.DisclosedQty
		tag = req.Tag
	case GTTOrderRequest:
		orderType = "GTT"
		tradingSymbol = req.TradingSymbol
		exchange = req.Exchange
		gttType = req.TriggerType
		quantity = 0 // Not directly available in GTTOrderRequest
		price = req.LastPrice
		triggerPrice = 0 // Could be set from req.TriggerValues if needed
		product = ""
		validity = ""
		disclosedQty = 0
		tag = ""
	}

	return &db.OrderRecord{
		OrderID:       resp.OrderID,
		OrderType:     orderType,
		GTTType:       gttType,
		Status:        resp.Status,
		Message:       resp.Message,
		TradingSymbol: tradingSymbol,
		Exchange:      exchange,
		Side:          side,
		Quantity:      quantity,
		Price:         price,
		TriggerPrice:  triggerPrice,
		Product:       product,
		Validity:      validity,
		DisclosedQty:  disclosedQty,
		Tag:           tag,
		UserID:        userID,
		PlacedAt:      placedAt,
		KiteResponse:  kiteResp,
		PaperTrading:  paperTrading,
	}
}

// SaveOrderAsync persists an order asynchronously using a goroutine
// For paper trading orders, it also manages virtual positions
func SaveOrderAsync(orderReq interface{}, resp *OrderResponse, userID string, kiteResp interface{}) {
	orderRecord := ToOrderRecord(orderReq, resp, userID, kiteResp)
	go func() {
		ctx := context.Background()
		dbConn := db.GetTimescaleDB()

		// Insert the order record
		err := dbConn.InsertOrder(ctx, orderRecord)
		if err != nil {
			logger.L().Error("Failed to persist order", map[string]interface{}{
				"order_id": resp.OrderID,
				"error":    err.Error(),
			})
		} else {
			logger.L().Info("Successfully persisted order", map[string]interface{}{
				"order_id":      resp.OrderID,
				"paper_trading": orderRecord.PaperTrading,
			})
		}

		// Handle paper trading position management
		if orderRecord.PaperTrading {
			managePaperTradingPosition(ctx, orderRecord)
		}
	}()
}

// managePaperTradingPosition creates or updates paper trading positions based on order details
func managePaperTradingPosition(ctx context.Context, order *db.OrderRecord) {
	log := logger.L()
	log.Info("Managing paper trading position", map[string]interface{}{
		"order_id":       order.OrderID,
		"trading_symbol": order.TradingSymbol,
		"side":           order.Side,
		"quantity":       order.Quantity,
		"paper_trading":  order.PaperTrading,
	})

	// Extract strategy from order tag if available
	strategy := ""
	if order.Tag != "" {
		// Check if the tag is a strategy name directly
		if strings.HasPrefix(order.Tag, "strategy:") {
			strategy = strings.TrimPrefix(order.Tag, "strategy:")
			log.Info("Found strategy in order tag", map[string]interface{}{
				"strategy": strategy,
				"order_id": order.OrderID,
			})
		} else {
			// Use the tag as the strategy name
			strategy = order.Tag
			log.Info("Using order tag as strategy", map[string]interface{}{
				"strategy": strategy,
				"order_id": order.OrderID,
			})
		}
	}

	// Verify this is actually a paper trading order
	if !order.PaperTrading {
		log.Error("Attempted to manage position for non-paper trading order", map[string]interface{}{
			"order_id": order.OrderID,
		})
		return
	}

	dbConn := db.GetTimescaleDB()
	if dbConn == nil {
		log.Error("Failed to get database connection for paper position management", nil)
		return
	}

	log.Info("Looking for existing paper positions", map[string]interface{}{
		"trading_symbol": order.TradingSymbol,
		"exchange":       order.Exchange,
		"product":        order.Product,
		"paper_trading":  true,
	})

	// Check if a position already exists for this symbol
	existingPositions, err := dbConn.ListPositions(ctx)
	if err != nil {
		log.Error("Failed to list positions for paper trading", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Look for an existing paper position for this symbol
	var existingPosition *db.PositionRecord
	for _, pos := range existingPositions {
		if pos.PaperTrading && pos.TradingSymbol == order.TradingSymbol &&
			pos.Exchange == order.Exchange && pos.Product == order.Product {
			existingPosition = pos
			log.Info("Found existing paper position", map[string]interface{}{
				"id":       pos.ID,
				"quantity": pos.Quantity,
			})
			break
		}
	}
	cacheMetaInstance, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta instance", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Calculate position quantity based on order side
	quantity := order.Quantity
	if order.Side == "SELL" {
		quantity = -quantity // Negative quantity for short positions
	}

	// Try to get the current market price
	lastPrice := order.Price
	if lastPrice == 0 && order.TradingSymbol != "" {
		// Get the market data manager
		marketDataMgr := GetMarketDataManager()

		// First try to get the instrument token using our helper method
		instrumentToken, exists := cacheMetaInstance.GetInstrumentTokenForSymbol(ctx, order.TradingSymbol)
		if exists {
			log.Info("Found instrument token for trading symbol", map[string]interface{}{
				"trading_symbol":   order.TradingSymbol,
				"instrument_token": instrumentToken,
			})

			// Try to get LTP using the instrument token
			ltp, found := marketDataMgr.GetLTP(ctx, instrumentToken)
			if found {
				lastPrice = ltp
				log.Info("Found LTP for paper trading position", map[string]interface{}{
					"ltp":              ltp,
					"instrument_token": instrumentToken,
					"trading_symbol":   order.TradingSymbol,
				})
			}
		}
	}

	if existingPosition != nil {
		// Update existing position
		newQuantity := existingPosition.Quantity + quantity

		if newQuantity == 0 {
			// Position is closed, remove it
			if err := dbConn.DeletePosition(ctx, existingPosition.ID); err != nil {
				log.Error("Failed to delete closed paper position", map[string]interface{}{
					"id":    existingPosition.ID,
					"error": err.Error(),
				})
			} else {
				log.Info("Closed paper trading position", map[string]interface{}{
					"id":             existingPosition.ID,
					"trading_symbol": order.TradingSymbol,
				})
			}
		} else {
			// Update position quantity and other details
			existingPosition.Quantity = newQuantity
			existingPosition.LastPrice = lastPrice

			// Update buy/sell price based on order side
			if order.Side == "BUY" {
				// For buy orders, update the buy price
				if existingPosition.BuyPrice == 0 || existingPosition.BuyQuantity == 0 {
					existingPosition.BuyPrice = order.Price
				} else {
					// Calculate weighted average buy price
					existingPosition.BuyPrice = ((existingPosition.BuyPrice * float64(existingPosition.BuyQuantity)) +
						(order.Price * float64(quantity))) / float64(existingPosition.BuyQuantity+quantity)
				}
				existingPosition.BuyQuantity += quantity
				existingPosition.BuyValue = existingPosition.BuyPrice * float64(existingPosition.BuyQuantity)
			} else if order.Side == "SELL" {
				// For sell orders, update the sell price
				if existingPosition.SellPrice == 0 || existingPosition.SellQuantity == 0 {
					existingPosition.SellPrice = order.Price
				} else {
					// Calculate weighted average sell price
					existingPosition.SellPrice = ((existingPosition.SellPrice * float64(existingPosition.SellQuantity)) +
						(order.Price * float64(-quantity))) / float64(existingPosition.SellQuantity-quantity)
				}
				existingPosition.SellQuantity -= quantity // Subtract because quantity is negative for sells
				existingPosition.SellValue = existingPosition.SellPrice * float64(existingPosition.SellQuantity)
			}

			// Update average price based on new order
			if (existingPosition.Quantity > 0 && quantity > 0) || (existingPosition.Quantity < 0 && quantity < 0) {
				// Adding to position, calculate new average price
				existingPosition.AveragePrice = ((existingPosition.AveragePrice * float64(existingPosition.Quantity-quantity)) +
					(order.Price * float64(quantity))) / float64(existingPosition.Quantity)
			}

			// Update P&L
			pnl := (lastPrice - existingPosition.AveragePrice) * float64(existingPosition.Quantity)
			if existingPosition.Quantity < 0 {
				// For short positions, P&L is reversed
				pnl = -pnl
			}
			existingPosition.PnL = pnl
			existingPosition.UpdatedAt = time.Now()

			// Update position in database
			if err := dbConn.UpsertPosition(ctx, existingPosition); err != nil {
				log.Error("Failed to update paper position", map[string]interface{}{
					"id":    existingPosition.ID,
					"error": err.Error(),
				})
			} else {
				log.Info("Updated paper trading position", map[string]interface{}{
					"id":             existingPosition.ID,
					"trading_symbol": order.TradingSymbol,
					"quantity":       existingPosition.Quantity,
					"average_price":  existingPosition.AveragePrice,
				})
			}
		}
	} else if quantity != 0 {
		// Create new paper position
		// Create a unique position ID for this paper trading position
		positionIDStr := fmt.Sprintf("%s_%s_%s", order.TradingSymbol, order.Exchange, order.Product)

		// Create a new position record
		newPosition := &db.PositionRecord{
			PositionID:    &positionIDStr,
			TradingSymbol: order.TradingSymbol,
			Exchange:      order.Exchange,
			Product:       order.Product,
			Quantity:      quantity,
			AveragePrice:  order.Price,
			LastPrice:     lastPrice,
			PnL:           0, // Will be calculated below
			Multiplier:    1, // Default multiplier
			BuyQuantity:   0,
			SellQuantity:  0,
			BuyPrice:      0,
			SellPrice:     0,
			BuyValue:      0,
			SellValue:     0,
			PositionType:  "net",
			UserID:        order.UserID,
			UpdatedAt:     time.Now(),
			PaperTrading:  true,
		}

		// Set buy/sell price based on order side
		if order.Side == "BUY" {
			newPosition.BuyPrice = order.Price
			newPosition.BuyQuantity = quantity
			newPosition.BuyValue = order.Price * float64(quantity)
		} else if order.Side == "SELL" {
			newPosition.SellPrice = order.Price
			newPosition.SellQuantity = -quantity // Negative quantity becomes positive for sell quantity
			newPosition.SellValue = order.Price * float64(-quantity)
		}

		// Get strategy ID if a strategy name is provided
		var strategyID *int
		if strategy != "" {
			// Here you would look up the strategy ID from the strategy name
			// For now, we'll leave it as nil
			newPosition.StrategyID = strategyID
		}
		cacheMetaInstance, err := cache.GetCacheMetaInstance()
		if err != nil {
			log.Error("Failed to get cache meta instance", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}
		// Try to get the instrument token
		instrumentToken, exists := cacheMetaInstance.GetInstrumentTokenForSymbol(ctx, order.TradingSymbol)

		// If token not found through the helper, try to extract from KiteResponse
		if !exists && order.KiteResponse != nil {
			if kiteOrder, ok := order.KiteResponse.(map[string]interface{}); ok {
				if token, hasToken := kiteOrder["instrument_token"]; hasToken {
					instrumentToken = token
					exists = true

					// Store the mapping in cache for future use
					inMemoryCache := cache.GetInMemoryCacheInstance()
					if inMemoryCache != nil {
						inMemoryCache.Set(order.TradingSymbol, instrumentToken, 7*24*time.Hour)
						log.Info("Stored trading symbol to token mapping for new position", map[string]interface{}{
							"trading_symbol":   order.TradingSymbol,
							"instrument_token": instrumentToken,
						})
					}
				}
			}
		}

		// If we have an instrument token, update the last price if needed
		if exists && lastPrice == 0 {
			// Use the MarketDataManager to get the LTP
			marketDataMgr := GetMarketDataManager()
			ltp, found := marketDataMgr.GetLTP(ctx, instrumentToken)
			if found {
				lastPrice = ltp
				newPosition.LastPrice = ltp
				log.Info("Updated position last price from LTP", map[string]interface{}{
					"ltp":              ltp,
					"instrument_token": instrumentToken,
				})
			}
		}

		// Calculate P&L
		pnl := (lastPrice - newPosition.AveragePrice) * float64(newPosition.Quantity)
		if newPosition.Quantity < 0 {
			// For short positions, P&L is reversed
			pnl = -pnl
		}
		newPosition.PnL = pnl

		// Insert position into database
		if err := dbConn.UpsertPosition(ctx, newPosition); err != nil {
			log.Error("Failed to insert new paper position", map[string]interface{}{
				"trading_symbol": newPosition.TradingSymbol,
				"error":          err.Error(),
			})
		} else {
			log.Info("Created new paper trading position", map[string]interface{}{
				"id":             newPosition.ID,
				"trading_symbol": order.TradingSymbol,
				"quantity":       newPosition.Quantity,
				"average_price":  newPosition.AveragePrice,
			})
		}
	}
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Index-specific freeze limits for iceberg orders
type FreezeLimit struct {
	Units    int  // Maximum units per order
	LotSize  int  // Standard lot size
	MaxLots  int  // Maximum number of lots per order
	Supports bool // Whether iceberg orders are supported
}

// Freeze limits by index/segment
var indexFreezeLimits = map[string]FreezeLimit{
	"NIFTY":     {Units: 1800, LotSize: 50, MaxLots: 36, Supports: true},  // Nifty F&O (NSE)
	"BANKNIFTY": {Units: 900, LotSize: 15, MaxLots: 60, Supports: true},   // Bank Nifty F&O (NSE)
	"SENSEX":    {Units: 1000, LotSize: 10, MaxLots: 100, Supports: true}, // SENSEX (BSE)
	"SENSEX50":  {Units: 1800, LotSize: 50, MaxLots: 36, Supports: true},  // SENSEX50 (BSE)
	"BANKEX":    {Units: 900, LotSize: 15, MaxLots: 60, Supports: true},   // BANKEX (BSE)
}

// Exchange-specific quantity limits for equity
var exchangeQuantityLimits = map[string]int{
	"NSE":     100000, // NSE equity limit (Zerodha)
	"BSE":     100000, // BSE equity limit (Zerodha)
	"default": 50000,  // Default limit for other exchanges
}

// PlaceOrder places a regular order using the Kite Connect API
// Automatically handles iceberg orders if quantity exceeds exchange limits
func PlaceOrder(req PlaceOrderRequest) (*OrderResponse, error) {
	// Get the KiteConnect instance
	kc := GetKiteConnect()
	log := logger.L()

	// Extract index name from trading symbol (if applicable)
	indexName, isIndex := extractIndexFromSymbol(req.TradingSymbol)

	// Check if this is a potential iceberg order based on quantity
	var freezeLimit int
	var shouldUseIceberg bool

	// Different handling for index derivatives vs equity
	if isIndex {
		// For index derivatives, use index-specific freeze limits
		freezeLimitInfo, limitExists := getIndexFreezeLimit(indexName)
		if limitExists && freezeLimitInfo.Supports {
			freezeLimit = freezeLimitInfo.Units
			shouldUseIceberg = req.Quantity > freezeLimit && req.DisclosedQty == 0

			log.Info("Using index-specific freeze limit", map[string]interface{}{
				"index":    indexName,
				"limit":    freezeLimit,
				"lot_size": freezeLimitInfo.LotSize,
				"max_lots": freezeLimitInfo.MaxLots,
			})
		} else {
			// Fall back to exchange limits if index not recognized
			freezeLimit = getExchangeQuantityLimit(req.Exchange)
			shouldUseIceberg = req.Quantity > freezeLimit && req.DisclosedQty == 0
		}
	} else {
		// For equity, use exchange-specific limits
		freezeLimit = getExchangeQuantityLimit(req.Exchange)
		shouldUseIceberg = req.Quantity > freezeLimit && req.DisclosedQty == 0
	}

	// Apply iceberg order logic if needed
	if shouldUseIceberg {
		// This is a large order that should be handled as an iceberg order
		log.Info("Converting to iceberg order due to large quantity", map[string]interface{}{
			"trading_symbol": req.TradingSymbol,
			"quantity":       req.Quantity,
			"freeze_limit":   freezeLimit,
			"is_index":       isIndex,
			"index_name":     indexName,
		})

		// Set disclosed quantity to the exchange limit or less
		// Typically, disclosed quantity is set to a fraction of the total
		// to minimize market impact
		disclosedQty := min(freezeLimit, req.Quantity/5) // Disclose 20% or the limit, whichever is smaller
		if disclosedQty < 1 {
			disclosedQty = 1 // Ensure at least 1 share is disclosed
		}
		req.DisclosedQty = disclosedQty

		log.Info("Iceberg order parameters set", map[string]interface{}{
			"total_quantity":     req.Quantity,
			"disclosed_quantity": req.DisclosedQty,
		})
	}

	// Create OrderParams for the Kite Connect client
	orderParams := kiteconnect.OrderParams{
		Exchange:        req.Exchange,
		Tradingsymbol:   req.TradingSymbol,
		TransactionType: string(req.Side),
		OrderType:       string(req.OrderType),
		Quantity:        req.Quantity,
		Product:         string(req.Product),
		Validity:        req.Validity,
	}

	// Set optional parameters
	if req.Price != 0 {
		orderParams.Price = req.Price
	}

	if req.TriggerPrice != 0 {
		orderParams.TriggerPrice = req.TriggerPrice
	}

	if req.DisclosedQty != 0 {
		orderParams.DisclosedQuantity = req.DisclosedQty
	}

	if req.Tag != "" {
		orderParams.Tag = req.Tag
	}

	// Place the order using the Kite Connect client's PlaceOrder method
	// "regular" is the variety for normal orders
	kiteResp, err := kc.Kite.PlaceOrder("regular", orderParams)
	if err != nil {
		return nil, fmt.Errorf("kite order placement failed: %v", err)
	}

	// Determine if this was placed as an iceberg order
	isIceberg := req.DisclosedQty > 0 && req.DisclosedQty < req.Quantity
	message := "Order placed successfully"
	if isIceberg {
		message = "Iceberg order placed successfully"
	}

	return &OrderResponse{
		OrderID: kiteResp.OrderID,
		Status:  "success", // The Kite API returns a successful response if the order is placed
		Message: message,
	}, nil
}

// getExchangeQuantityLimit returns the maximum order quantity for a given exchange (for equity)
func getExchangeQuantityLimit(exchange string) int {
	limit, exists := exchangeQuantityLimits[exchange]
	if !exists {
		return exchangeQuantityLimits["default"]
	}
	return limit
}

// getIndexFreezeLimit returns the freeze limit information for a specific index
func getIndexFreezeLimit(indexName string) (FreezeLimit, bool) {
	limit, exists := indexFreezeLimits[indexName]
	return limit, exists
}

// extractIndexFromSymbol attempts to identify the index from a trading symbol
// Returns the index name and a boolean indicating if it's an index derivative
func extractIndexFromSymbol(symbol string) (string, bool) {
	// Common index prefixes in trading symbols
	indexPrefixes := []string{"NIFTY", "BANKNIFTY", "FINNIFTY", "SENSEX", "BANKEX", "SENSEX50"}

	// Check if the symbol starts with any known index prefix
	for _, prefix := range indexPrefixes {
		if strings.HasPrefix(symbol, prefix) {
			return prefix, true
		}
	}

	// Additional check for options contracts which might have different formats
	// For example: "NIFTY23MAY18500CE" for a NIFTY option
	for _, prefix := range indexPrefixes {
		if strings.Contains(symbol, prefix) {
			return prefix, true
		}
	}

	// Not an index derivative
	return "", false
}

// Note: Using the min function from instruments.go

// GTTOrderRequest for placing a GTT order
// PlaceGTTOrder places a GTT order using the Kite Connect API
type GTTOrderRequest struct {
	TriggerType   string                   `json:"trigger_type"`
	TradingSymbol string                   `json:"tradingsymbol"`
	Exchange      string                   `json:"exchange"`
	TriggerValues []float64                `json:"trigger_values"`
	LastPrice     float64                  `json:"last_price"`
	Orders        []map[string]interface{} `json:"orders"`
}

// SimpleTrigger implements the kiteconnect.Trigger interface for single trigger GTT orders
type SimpleTrigger struct {
	triggerValue float64
	limitPrice   float64
	quantity     float64
}

// TriggerValues returns the trigger values for the GTT order
func (t SimpleTrigger) TriggerValues() []float64 {
	return []float64{t.triggerValue}
}

// LimitPrices returns the limit prices for the GTT order
func (t SimpleTrigger) LimitPrices() []float64 {
	return []float64{t.limitPrice}
}

// Quantities returns the quantities for the GTT order
func (t SimpleTrigger) Quantities() []float64 {
	return []float64{t.quantity}
}

// Type returns the GTT type
func (t SimpleTrigger) Type() kiteconnect.GTTType {
	return kiteconnect.GTTTypeSingle
}

func PlaceGTTOrder(req GTTOrderRequest) (*OrderResponse, error) {
	// Get the KiteConnect instance
	kc := GetKiteConnect()

	// Extract order details from the first order (assuming single trigger for now)
	if len(req.Orders) == 0 {
		return nil, fmt.Errorf("no orders provided in GTT request")
	}

	order := req.Orders[0]
	transactionType, _ := order["transaction_type"].(string)
	quantity, _ := order["quantity"].(float64)
	price, _ := order["price"].(float64)
	product, _ := order["product"].(string)

	// Create a trigger implementation
	trigger := SimpleTrigger{
		triggerValue: req.TriggerValues[0],
		limitPrice:   price,
		quantity:     quantity,
	}

	// Create GTT params
	gttParams := kiteconnect.GTTParams{
		Tradingsymbol:   req.TradingSymbol,
		Exchange:        req.Exchange,
		LastPrice:       req.LastPrice,
		TransactionType: transactionType,
		Product:         product,
		Trigger:         trigger,
	}

	// Place the GTT order
	gttResp, err := kc.Kite.PlaceGTT(gttParams)
	if err != nil {
		return nil, fmt.Errorf("kite GTT placement failed: %v", err)
	}

	// Return a successful response with the trigger ID
	return &OrderResponse{
		OrderID: fmt.Sprintf("gtt-%d", gttResp.TriggerID),
		Status:  "success",
		Message: "GTT order placed successfully",
	}, nil
}
