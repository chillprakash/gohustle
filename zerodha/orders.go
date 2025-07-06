package zerodha

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"gohustle/appparameters"
	"gohustle/cache"
	"gohustle/core"
	"gohustle/logger"
	"gohustle/utils"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type KiteOrder struct {
	Exchange        string
	Tradingsymbol   string
	Product         appparameters.ProductType
	OrderType       appparameters.OrderType
	TransactionType Side
	Quantity        int
}

type PlaceOrderRequest struct {
	InstrumentToken       uint32    `json:"instrument_token"`
	OrderType             OrderType `json:"order_type"`
	Quantity              int       `json:"quantity"`
	Percentage            int       `json:"percentage"`
	TargetInstrumentToken uint32    `json:"target_instrument_token"`
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
	orderType appparameters.OrderType, productType appparameters.ProductType, quantity int) (*OrderResponse, error) {

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

func placeOrdersAtZerodha(indexMeta *cache.InstrumentData, side Side, quantity int) (*[]OrderResponse, error) {
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
	log.Info("Placing orders", map[string]interface{}{
		"quantity":     quantity,
		"freeze_limit": freezeLimit,
	})

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

func populateKiteOrderStruct(quantity int, side Side, instrumentData *cache.InstrumentData) KiteOrder {
	productType := appparameters.GetAppParameterManager().GetOrderAppParameters().ProductType
	orderType := appparameters.GetAppParameterManager().GetOrderAppParameters().OrderType
	return KiteOrder{
		Exchange:        instrumentData.Exchange,
		Tradingsymbol:   instrumentData.TradingSymbol,
		Product:         productType,
		OrderType:       orderType,
		TransactionType: side,
		Quantity:        quantity,
	}
}

func populateModifyQuantityForModifyRequest(req PlaceOrderRequest, existingQuantity int, index *core.Index) int {
	log := logger.L()
	log.Info("Populating modify quantity for modify request", map[string]interface{}{
		"existing_quantity": existingQuantity,
		"req":               req,
		"index":             index,
	})
	if req.Percentage != 0 {
		// Calculate the percentage of existing quantity
		quantity := existingQuantity * req.Percentage / 100

		// Round to nearest multiple of MaxLotsPerOrder
		rounded := (quantity + index.MaxLotsPerOrder/2) / index.MaxLotsPerOrder * index.MaxLotsPerOrder

		// Ensure we don't round down to zero for small quantities
		if rounded == 0 && quantity > 0 {
			rounded = index.MaxLotsPerOrder
		}

		logger.L().Debug("Rounded quantity to nearest multiple of MaxLotsPerOrder", map[string]interface{}{
			"original_quantity":  quantity,
			"rounded_quantity":   rounded,
			"max_lots_per_order": index.MaxLotsPerOrder,
			"percentage":         req.Percentage,
		})

		return rounded
	}

	// Return direct quantity if specified, otherwise 0
	if req.Quantity != 0 {
		return req.Quantity
	}
	return 0
}

// PlaceOrder places a regular order using the Kite Connect API
// Automatically handles iceberg orders if quantity exceeds exchange limits
func PlaceOrder(req PlaceOrderRequest) ([]OrderResponse, error) {
	log := logger.L()
	orderResponseToReturn := []OrderResponse{}
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
	kiteOrdersTobePlaced := []KiteOrder{}
	side := SideBuy
	if req.OrderType == OrderTypeCreateBuy || req.OrderType == OrderTypeCreateSell {
		if req.OrderType == OrderTypeCreateSell {
			side = SideSell
		}
		kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(req.Quantity, side, &indexMeta))
	}

	if req.OrderType == OrderTypeExit {
		quantity, err := getQuantityOfPositionsforInstrumentToken(utils.Uint32ToString(indexMeta.InstrumentToken))
		log.Info("Quantity of positions for exit order", map[string]interface{}{
			"quantity": quantity,
		})
		if err != nil {
			log.Error("Failed to get quantity of positions", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		if quantity > 0 {
			side = SideSell
		}
		kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(quantity, side, &indexMeta))
	}

	if req.OrderType == OrderTypeModifyAway || req.OrderType == OrderTypeModifyCloser {
		existingQuantity, err := getQuantityOfPositionsforInstrumentToken(utils.Uint32ToString(indexMeta.InstrumentToken))
		if err != nil {
			log.Error("Failed to get quantity of positions", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		modifyQuantity := populateModifyQuantityForModifyRequest(req, existingQuantity, &indexMeta.Name)
		targetIndexMeta, err := cacheMeta.GetMetadataOfToken(context.Background(), utils.Uint32ToString(req.TargetInstrumentToken))
		log.Info("Target index meta", map[string]interface{}{
			"target_index_meta": targetIndexMeta,
			"modify_quantity":   modifyQuantity,
			"existing_quantity": existingQuantity,
		})
		if err != nil {
			log.Error("Failed to get target index meta", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		if existingQuantity < 0 {
			var currentPositionSide Side = SideBuy
			var targetPositionSide Side = SideSell
			kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(modifyQuantity, currentPositionSide, &indexMeta))
			kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(modifyQuantity, targetPositionSide, &targetIndexMeta))
		} else if existingQuantity > 0 {
			var currentPositionSide Side = SideSell
			var targetPositionSide Side = SideBuy
			kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(modifyQuantity, currentPositionSide, &indexMeta))
			kiteOrdersTobePlaced = append(kiteOrdersTobePlaced, populateKiteOrderStruct(modifyQuantity, targetPositionSide, &targetIndexMeta))
		}
	}
	log.Info("Kite orders to be placed", map[string]interface{}{
		"kite_orders": kiteOrdersTobePlaced,
	})

	// Place all orders at Zerodha
	for _, order := range kiteOrdersTobePlaced {
		ordersResponse, err := placeOrdersAtZerodha(&indexMeta, order.TransactionType, order.Quantity)
		if err != nil {
			log.Error("Failed to place orders at Zerodha", map[string]interface{}{
				"error": err.Error(),
			})
			return nil, err
		}
		orderResponseToReturn = append(orderResponseToReturn, *ordersResponse...)
	}

	// Return the responses
	return orderResponseToReturn, nil
}

func getQuantityOfPositionsforInstrumentToken(instrumentToken string) (int, error) {
	positionsManager := GetPositionManager()
	positions, err := positionsManager.GetOpenPositionTokensVsQuanityFromRedis(context.Background())
	if err != nil {
		log.Error("Failed to get open positions from Redis", map[string]interface{}{
			"error": err.Error(),
		})
		return 0, err
	}

	// Directly look up the position by instrument token
	if position, exists := positions[instrumentToken]; exists {
		// Convert int64 to int to match function signature
		return int(position.Quantity), nil
	}

	return 0, nil
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
