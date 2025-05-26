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
	"gohustle/utils"

	"github.com/jackc/pgx/v5"
)

// Order represents a trading order in the system
type Order struct {
	ID              int64           `json:"id" db:"id"`
	ExternalOrderID string          `json:"external_order_id" db:"external_order_id"`
	OrderType       string          `json:"order_type" db:"order_type"`
	InstrumentToken uint32          `json:"instrument_token" db:"instrument_token"`
	TradingSymbol   string          `json:"trading_symbol" db:"trading_symbol"`
	Quantity        int             `json:"quantity" db:"quantity"`
	PaperTrading    bool            `json:"paper_trading" db:"paper_trading"`
	CreatedAt       time.Time       `json:"created_at" db:"created_at"`
	PayloadToBroker json.RawMessage `json:"payload_to_broker" db:"payload_to_broker"`
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

// Index-specific freeze limits for iceberg orders
type FreezeLimit struct {
	Units   int // Maximum units per order
	LotSize int // Standard lot size
}

// Freeze limits by index/segment
var indexFreezeLimits = map[string]FreezeLimit{
	"NIFTY":     {Units: 1800, LotSize: 75}, // Nifty F&O (NSE)
	"BANKNIFTY": {Units: 900, LotSize: 35},  // Bank Nifty F&O (NSE)
	"SENSEX":    {Units: 1000, LotSize: 20}, // SENSEX (BSE)
}

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
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func createNormalOrder(req PlaceOrderRequest, indexMeta cache.InstrumentData) (*Order, error) {
	order := &Order{
		OrderType:       string(req.OrderType),
		InstrumentToken: indexMeta.InstrumentToken,
		TradingSymbol:   indexMeta.TradingSymbol,
		Quantity:        req.Quantity,
		PaperTrading:    req.PaperTrading,
		CreatedAt:       time.Now(),
	}
	return order, nil
}
func storeOrdersToDB(ctx context.Context, orders []Order) error {
	if len(orders) == 0 {
		return nil
	}

	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("failed to get database instance")
	}

	// Use WithTx for automatic transaction management
	err := timescaleDB.WithTx(ctx, func(tx pgx.Tx) error {
		// Use COPY command for bulk insert (most efficient for large batches)
		_, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"orders"},
			[]string{"external_order_id", "order_type", "instrument_token", "trading_symbol",
				"quantity", "paper_trading", "created_at", "payload_to_broker"},
			pgx.CopyFromSlice(len(orders), func(i int) ([]interface{}, error) {
				order := orders[i]
				payload, err := json.Marshal(order)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal order payload: %w", err)
				}

				return []interface{}{
					order.ExternalOrderID,
					order.OrderType,
					order.InstrumentToken,
					order.TradingSymbol,
					order.Quantity,
					order.PaperTrading,
					order.CreatedAt,
					payload,
				}, nil
			}),
		)

		return err
	})

	if err != nil {
		return fmt.Errorf("failed to store orders: %w", err)
	}

	return nil
}

// PlaceOrder places a regular order using the Kite Connect API
// Automatically handles iceberg orders if quantity exceeds exchange limits
func PlaceOrder(req PlaceOrderRequest) (*OrderResponse, error) {
	log := logger.L()
	cacheMeta, err := cache.GetCacheMetaInstance()
	var orders []Order
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
		order, err := createNormalOrder(req, indexMeta)
		if err != nil {
			log.Error("Failed to create order", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}

		if req.PaperTrading {
			orders = append(orders, *order)
		}
		if err != nil {
			log.Error("Failed to create order", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		return nil, nil
	}

	if req.OrderType == OrderTypeModifyAway || req.OrderType == OrderTypeModifyCloser {
		processModifyOrder(req, indexMeta)
	}
	return nil, nil
}

func populateModifyOrderProcess(placeOrderRequest PlaceOrderRequest, indexMeta *cache.IndexMeta, existingQuantity int) ModifyPositionOrderStruct {
	//Assume I have 21000 position
	//Percentage is 0.25/0.5
	//Exisiting side is Buy

	if indexMeta.InstrumentType == cache.InstrumentTypeCE {
		newStrike := indexMeta.Strike + 10

	}
	if indexMeta.InstrumentType == cache.InstrumentTypePE {
		newStrike := indexMeta.Strike - 10

	}

	modifyPosition := ModifyPositionOrderStruct{
		ExistingStrike:           indexMeta.Strike,
		NewStrike:                indexMeta.Strike,
		Percentage:               placeOrderRequest.Percentage,
		ToProcessAtCurrentStrike: []ModifyPositionParams{},
		ToProcessAtNewStrike:     []ModifyPositionParams{},
	}
}

func processModifyOrder(placeOrderRequest PlaceOrderRequest, indexMeta *models.IndexMeta) (*OrderResponse, error) {
	positionsManager := GetPositionManager()
	positions, err := positionsManager.GetOpenPositionTokensVsQuanityFromRedis(context.Background())
	if err != nil {
		log.Error("Failed to get open positions from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, err
	}

	for _, position := range positions {
		if position.InstrumentToken == utils.Uint32ToString(indexMeta.InstrumentToken) {
			existingQuantity := position.Quantity
			if placeOrderRequest.OrderType == OrderTypeModifyAway {
				strikeToMove, err := calculateStrikeToMove(placeOrderRequest, indexMeta, existingQuantity)
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

}

func calculateStrikeToMove(placeOrderRequest PlaceOrderRequest, indexMeta *models.IndexMeta) (int, error) {
	if placeOrderRequest.OrderType == OrderTypeModifyAway {
		if placeOrderRequest.Side == SideBuy {
			return indexMeta.Strike + placeOrderRequest.Quantity, nil
		}
		if placeOrderRequest.Side == SideSell {
			return indexMeta.Strike - placeOrderRequest.Quantity, nil
		}
	}
	if placeOrderRequest.OrderType == OrderTypeModifyCloser {
		if placeOrderRequest.Side == SideBuy {
			return indexMeta.Strike + placeOrderRequest.Quantity, nil
		}
		if placeOrderRequest.Side == SideSell {
			return indexMeta.Strike - placeOrderRequest.Quantity, nil
		}
	}
	return 0, nil
}
