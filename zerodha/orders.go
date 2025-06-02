package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/db"
	"gohustle/logger"
	"gohustle/utils"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

// Order represents a trading order in the system
type Order struct {
	ID                 int64           `json:"id" db:"id"`
	ExternalOrderID    string          `json:"external_order_id" db:"external_order_id"`
	OrderType          string          `json:"order_type" db:"order_type"`
	InstrumentToken    uint32          `json:"instrument_token" db:"instrument_token"`
	TradingSymbol      string          `json:"trading_symbol" db:"trading_symbol"`
	Quantity           int             `json:"quantity" db:"quantity"`
	PaperTrading       bool            `json:"paper_trading" db:"paper_trading"`
	CreatedAt          time.Time       `json:"created_at" db:"created_at"`
	PayloadToBroker    json.RawMessage `json:"payload_to_broker" db:"payload_to_broker"`
	ResponseFromBroker json.RawMessage `json:"response_from_broker" db:"response_from_broker"`
}

// TableName specifies the database table name for GORM
func (Order) TableName() string {
	return "orders"
}

type PlaceOrderRequest struct {
	InstrumentToken uint32    `json:"instrument_token"`
	OrderType       OrderType `json:"order_type"`
	Quantity        int       `json:"quantity"`
	Percentage      int       `json:"percentage"`
	PaperTrading    bool      `json:"paper_trading"`
}

type IceBergParams struct {
	IcebergQty  int
	IcebergLegs int
}

type ModifyPositionOrderStruct struct {
	ExistingStrike           int
	NewStrike                int
	Percentage               int
	ToProcessAtCurrentStrike []ModifyPositionParams
	ToProcessAtNewStrike     []ModifyPositionParams
}

type ModifyPositionParams struct {
	Side     Side
	Quantity int
}

type OrderType string

const (
	OrderTypeCreateBuy    OrderType = "create_position_buy"
	OrderTypeCreateSell   OrderType = "create_position_sell"
	OrderTypeModifyAway   OrderType = "modify_position_away"
	OrderTypeModifyCloser OrderType = "modify_position_closer"
	OrderTypeExit         OrderType = "exit_position"
)

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type OrderManager struct {
	log       *logger.Logger
	kite      *KiteConnect
	cacheMeta *cache.CacheMeta
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
		cacheMeta, err := cache.GetCacheMetaInstance()
		if err != nil {
			log.Error("Failed to initialize order manager: CacheMeta is nil", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		if kite == nil {
			log.Error("Failed to initialize order manager: KiteConnect is nil", map[string]interface{}{})
			return
		}

		orderManagerInstance = &OrderManager{
			log:       log,
			kite:      kite,
			cacheMeta: cacheMeta,
		}
		log.Info("Order manager initialized", map[string]interface{}{})
	})
	return orderManagerInstance
}

// OrderResponse represents a simplified response for order placement
// (expand as needed for your UI)
type OrderResponse struct {
	OrderID  string          `json:"order_id"`
	Status   string          `json:"status"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Message  string          `json:"message,omitempty"`
}

func placeSingleOrder(indexMeta *cache.InstrumentData, side Side,
	orderType appparameters.AppParameterTypes, productType appparameters.AppParameterTypes, quantity int) (*OrderResponse, error) {

	orderParams := kiteconnect.OrderParams{
		Exchange:        indexMeta.Exchange,
		Tradingsymbol:   indexMeta.TradingSymbol,
		Product:         string(productType),
		OrderType:       string(orderType),
		TransactionType: string(side),
		Quantity:        quantity,
	}

	log.Info("Placing order", map[string]interface{}{
		"order_params": orderParams,
	})

	orderResponse, err := GetOrderManager().kite.Kite.PlaceOrder("regular", orderParams)
	if err != nil {
		return nil, fmt.Errorf("failed to place order: %w", err)
	}

	payload, err := json.Marshal(orderParams)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal order params: %w", err)
	}

	response, err := json.Marshal(orderResponse)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal order response: %w", err)
	}

	log.Info("Order placed successfully", map[string]interface{}{
		"order_response": orderResponse,
	})

	return &OrderResponse{
		OrderID:  orderResponse.OrderID,
		Status:   "success",
		Payload:  payload,
		Response: response,
	}, nil
}

func placeOrderAtZerodha(indexMeta *cache.InstrumentData, side Side, quantity int) (*[]OrderResponse, error) {
	var orderResponses []OrderResponse
	orderManager := GetOrderManager()
	if orderManager == nil {
		return nil, fmt.Errorf("order manager not initialized")
	}

	appParameterManager := appparameters.GetAppParameterManager()
	if appParameterManager == nil {
		return nil, fmt.Errorf("app parameter manager not initialized")
	}

	orderType := appParameterManager.GetOrderAppParameters().OrderType
	productType := appParameterManager.GetOrderAppParameters().ProductType
	freezeLimit := indexMeta.Name.GetUnitsPerLot()

	// For quantities within limit, place a single order
	if quantity <= freezeLimit {
		orderResponse, err := placeSingleOrder(indexMeta, side, orderType, productType, quantity)
		if err != nil {
			return nil, err
		}
		orderResponses = append(orderResponses, *orderResponse)
		return &orderResponses, nil
	}
	remainingQty := quantity

	// Calculate number of full lots and remaining quantity
	fullLots := remainingQty / freezeLimit
	remainingQty = remainingQty % freezeLimit

	log.Info("Placing orders", map[string]interface{}{
		"full_lots":     fullLots,
		"remaining_qty": remainingQty,
		"freeze_limit":  freezeLimit,
	})

	// Place full lot orders
	for i := 0; i < fullLots; i++ {
		resp, err := placeSingleOrder(indexMeta, side, orderType, productType, freezeLimit)

		if err != nil {
			orderManager.log.Error("Failed to place partial order", map[string]interface{}{
				"error":    err.Error(),
				"quantity": freezeLimit,
			})
			continue
		}
		orderResponses = append(orderResponses, *resp)
	}

	// Place remaining quantity if any
	if remainingQty > 0 {
		resp, err := placeSingleOrder(indexMeta, side, orderType, productType, remainingQty)

		if err == nil {
			orderResponses = append(orderResponses, *resp)
		} else {
			orderManager.log.Error("Failed to place remaining order", map[string]interface{}{
				"error":    err.Error(),
				"quantity": remainingQty,
			})
		}
	}

	return &orderResponses, nil
}

// Update the createNormalOrder function
func createNormalOrder(req PlaceOrderRequest, indexMeta *cache.InstrumentData) *Order {
	orderID := uuid.New().String()
	return &Order{
		ExternalOrderID:    orderID,
		OrderType:          string(req.OrderType),
		InstrumentToken:    req.InstrumentToken,
		TradingSymbol:      indexMeta.TradingSymbol,
		Quantity:           req.Quantity,
		PaperTrading:       true,
		CreatedAt:          time.Now(),
		PayloadToBroker:    json.RawMessage(fmt.Sprintf(`{"paper":true,"quantity":%d}`, req.Quantity)),
		ResponseFromBroker: json.RawMessage(fmt.Sprintf(`{"order_id":"%s","status":"success","paper":true}`, orderID)),
	}
}

// storeOrdersToDB stores the given orders in the database and returns their external order IDs
func storeOrdersToDB(ctx context.Context, orders []Order) ([]string, error) {
	if len(orders) == 0 {
		return []string{}, nil
	}

	log := logger.L()
	log.Info("Storing orders to DB", map[string]interface{}{
		"order_count": len(orders),
	})

	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("failed to get database instance")
	}

	orderIDs := make([]string, 0, len(orders))
	var copyErr error

	// Use WithTx for automatic transaction management
	err := timescaleDB.WithTx(ctx, func(tx pgx.Tx) error {
		// Use COPY command for bulk insert
		_, copyErr = tx.CopyFrom(
			ctx,
			pgx.Identifier{"orders"},
			[]string{"external_order_id", "order_type", "instrument_token", "trading_symbol",
				"quantity", "paper_trading", "created_at", "payload_to_broker", "response_from_broker"},
			pgx.CopyFromSlice(len(orders), func(i int) ([]interface{}, error) {
				order := orders[i]
				payload, err := json.Marshal(order.PayloadToBroker)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal order payload: %w", err)
				}

				response, err := json.Marshal(order.ResponseFromBroker)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal order response: %w", err)
				}

				// Collect the order ID
				orderIDs = append(orderIDs, order.ExternalOrderID)

				return []interface{}{
					order.ExternalOrderID,
					order.OrderType,
					order.InstrumentToken,
					order.TradingSymbol,
					order.Quantity,
					order.PaperTrading,
					order.CreatedAt,
					payload,
					response,
				}, nil
			}),
		)
		return copyErr
	})

	if err != nil {
		log.Error("Failed to store orders in transaction", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, fmt.Errorf("transaction failed: %w", err)
	}

	log.Info("Successfully stored orders", map[string]interface{}{
		"order_count": len(orderIDs),
		"order_ids":   orderIDs,
	})

	return orderIDs, nil
}

func processCreateOrder(req PlaceOrderRequest, indexMeta *cache.InstrumentData) (*[]OrderResponse, error) {
	var orderResponses []OrderResponse
	var orders []Order
	side := SideBuy
	if req.OrderType == OrderTypeCreateSell {
		side = SideSell
	}
	log := logger.L()
	log.Info("Processing create order", map[string]interface{}{
		"place_order_request": req,
		"side":                side,
	})

	// Handle real trading
	if !req.PaperTrading {
		// Place order with Zerodha
		orderResponse, err := placeOrderAtZerodha(indexMeta, side, req.Quantity)
		if err != nil {
			log.Error("Failed to place order at Zerodha", map[string]interface{}{
				"error":  err.Error(),
				"side":   side,
				"symbol": indexMeta.TradingSymbol,
			})
			return nil, fmt.Errorf("failed to place %s order: %w", side, err)
		}
		orderResponses = *orderResponse

		// Prepare orders for DB storage
		orders = make([]Order, 0, len(orderResponses))
		for _, resp := range orderResponses {
			orders = append(orders, Order{
				ExternalOrderID:    resp.OrderID,
				OrderType:          string(req.OrderType),
				InstrumentToken:    indexMeta.InstrumentToken,
				TradingSymbol:      indexMeta.TradingSymbol,
				Quantity:           req.Quantity,
				PaperTrading:       false,
				CreatedAt:          time.Now(),
				PayloadToBroker:    resp.Payload,
				ResponseFromBroker: resp.Response,
			})
		}
	} else {
		// Handle paper trading
		log.Info("Creating paper order", map[string]interface{}{
			"place_order_request": req,
		})
		order := createNormalOrder(req, indexMeta)
		log.Info("Created paper order", map[string]interface{}{
			"order": order,
		})
		if order == nil {
			log.Error("Failed to create paper order", map[string]interface{}{
				"error": "failed to create paper order",
			})
			return nil, fmt.Errorf("failed to create paper order")
		}
		orders = []Order{*order}

		// Create response for paper order
		orderResponses = []OrderResponse{{
			OrderID:  order.ExternalOrderID,
			Status:   "success",
			Payload:  order.PayloadToBroker,
			Response: order.ResponseFromBroker,
		}}

		log.Info("Creating paper positions", map[string]interface{}{
			"orders": orders,
			"index":  indexMeta,
			"side":   side,
		})

		positionManager := GetPositionManager()
		positionManager.CreatePaperPositions(context.Background(), orders, indexMeta, side)
	}

	// Store orders in DB
	orderIDs, err := storeOrdersToDB(context.Background(), orders)
	if err != nil {
		log.Error("Failed to store orders", map[string]interface{}{
			"error":  err.Error(),
			"paper":  req.PaperTrading,
			"symbol": indexMeta.TradingSymbol,
		})
		return nil, fmt.Errorf("failed to store orders: %w", err)
	}
	log.Info("Orders stored successfully", map[string]interface{}{
		"order_ids": orderIDs,
	})

	// Prepare clean response
	cleanResponses := make([]OrderResponse, 0, len(orderResponses))
	for _, resp := range orderResponses {
		cleanResponses = append(cleanResponses, OrderResponse{
			OrderID: resp.OrderID,
			Status:  resp.Status,
			Message: resp.Message,
		})
	}

	return &cleanResponses, nil
}

// PlaceOrder places a regular order using the Kite Connect API
// Automatically handles iceberg orders if quantity exceeds exchange limits
func PlaceOrder(req PlaceOrderRequest) (*[]OrderResponse, error) {
	log := logger.L()
	cacheMeta, err := cache.GetCacheMetaInstance()
	if err != nil {
		log.Error("Failed to get cache meta", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	indexMeta, err := cacheMeta.GetMetadataOfToken(context.Background(), utils.Uint32ToString(req.InstrumentToken))
	if err != nil {
		log.Error("Failed to get index meta", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	if req.OrderType == OrderTypeCreateBuy || req.OrderType == OrderTypeCreateSell {
		createOrderResponse, err := processCreateOrder(req, &indexMeta)
		log.Info("Create order response", map[string]interface{}{
			"response": createOrderResponse,
		})
		if err != nil {
			log.Error("Failed to process create order", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		return createOrderResponse, nil
	}

	if req.OrderType == OrderTypeModifyAway || req.OrderType == OrderTypeModifyCloser {
		return processModifyOrder(req, &indexMeta)
	}
	return nil, nil
}

func processModifyOrder(placeOrderRequest PlaceOrderRequest, indexMeta *cache.InstrumentData) (*[]OrderResponse, error) {
	positionsManager := GetPositionManager()
	positions, err := positionsManager.GetOpenPositionTokensVsQuanityFromRedis(context.Background())
	if err != nil {
		log.Error("Failed to get open positions from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}
	//Modify can happen only with existing positions as base.
	for _, position := range positions {
		if position.InstrumentToken == utils.Uint32ToString(indexMeta.InstrumentToken) {
			// existingQuantity := position.Quantity
			if placeOrderRequest.OrderType == OrderTypeModifyAway {
				if err != nil {
					log.Error("Failed to calculate strike to move", map[string]interface{}{
						"error": err.Error(),
					})
					return nil, err
				}

			}
			if placeOrderRequest.OrderType == OrderTypeModifyAway {

			}
		}
	}
	return nil, nil
}
