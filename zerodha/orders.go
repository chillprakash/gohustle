package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
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

	// Store orders in Redis
	if err := om.storeOrdersInRedis(ctx, orders); err != nil {
		return err
	}

	// Also update the database with the latest order statuses
	if err := om.updateOrderStatusesInDB(ctx, orders); err != nil {
		om.log.Error("Failed to update order statuses in DB", map[string]interface{}{
			"error": err.Error(),
		})
		// Don't return error here, as we still want to continue with Redis updates
	}

	return nil
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

	// Create a context with timeout for Redis operations
	redisCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Store each order in Redis with key format: order_{order_id}_status
	for _, order := range orders {
		// Store order status
		statusKey := fmt.Sprintf("order_%s_status", order.OrderID)
		if err := ordersDB.Set(redisCtx, statusKey, order.Status, 24*time.Hour).Err(); err != nil {
			om.log.Error("Failed to store order status in Redis", map[string]interface{}{
				"order_id": order.OrderID,
				"status":   order.Status,
				"error":    err.Error(),
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

		if err := ordersDB.Set(redisCtx, orderKey, orderJSON, 24*time.Hour).Err(); err != nil {
			om.log.Error("Failed to store order in Redis", map[string]interface{}{
				"order_id": order.OrderID,
				"error":    err.Error(),
			})
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
		om.log.Info("Would update order status in DB", map[string]interface{}{
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
func SaveOrderAsync(orderReq interface{}, resp *OrderResponse, userID string, kiteResp interface{}) {
	orderRecord := ToOrderRecord(orderReq, resp, userID, kiteResp)
	go func() {
		dbConn := db.GetTimescaleDB()
		// Use context.Background() for now, consider passing a context if needed
		err := dbConn.InsertOrder(context.Background(), orderRecord)
		if err != nil {
			// Consider more robust logging (e.g., using a logger package)
			fmt.Printf("[OrderPersist] Failed to persist order %s: %v\n", resp.OrderID, err)
		} else {
			fmt.Printf("[OrderPersist] Successfully persisted order %s\n", resp.OrderID) // Optional: Log success
		}
	}()
}

// PlaceOrder places a regular order using the Kite Connect API
func PlaceOrder(req PlaceOrderRequest) (*OrderResponse, error) {
	// Get the KiteConnect instance
	kc := GetKiteConnect()

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

	return &OrderResponse{
		OrderID: kiteResp.OrderID,
		Status:  "success", // The Kite API returns a successful response if the order is placed
		Message: "Order placed successfully",
	}, nil
}

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
