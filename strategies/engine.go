package strategies

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/db"
	"gohustle/logger"
	"gohustle/strategies/base"
	"gohustle/strategies/iron_condor"
	"gohustle/zerodha"
)

// StrategyEngine manages the execution of all trading strategies
type StrategyEngine struct {
	registry     *StrategyRegistry
	orderManager *zerodha.OrderManager
	positionMgr  *zerodha.PositionManager
	pnlManager   *zerodha.PnLManager
	log          *logger.Logger
	running      bool
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.RWMutex
}

var (
	engineInstance *StrategyEngine
	engineOnce     sync.Once
)

// GetStrategyEngine returns a singleton instance of StrategyEngine
func GetStrategyEngine() *StrategyEngine {
	engineOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		engineInstance = &StrategyEngine{
			registry:     GetStrategyRegistry(),
			orderManager: zerodha.GetOrderManager(),
			positionMgr:  zerodha.GetPositionManager(),
			pnlManager:   zerodha.GetPnLManager(),
			log:          logger.L(),
			running:      false,
			ctx:          ctx,
			cancel:       cancel,
		}

		// Register built-in strategies
		engineInstance.registerBuiltInStrategies()
	})
	return engineInstance
}

// registerBuiltInStrategies registers all built-in strategies
func (e *StrategyEngine) registerBuiltInStrategies() {
	// Register Iron Condor strategy
	e.registry.RegisterStrategy("iron_condor", func(params map[string]interface{}) (base.Strategy, error) {
		return iron_condor.NewIronCondorStrategy(), nil
	})

	// Add more built-in strategies here
}

// Initialize sets up the strategy engine
func (e *StrategyEngine) Initialize(ctx context.Context) error {
	e.log.Info("Initializing strategy engine")

	// Load strategies from database
	strategies, err := e.registry.ListStrategiesFromDB(ctx)
	if err != nil {
		e.log.Error("Failed to load strategies from database", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	e.log.Info("Loaded strategies from database", map[string]interface{}{
		"count": len(strategies),
	})

	// Start active strategies
	for _, strategyRecord := range strategies {
		if !strategyRecord.Active {
			continue
		}

		// Combine all parameters
		params := strategyRecord.Parameters.(map[string]interface{})
		if strategyRecord.EntryRules != nil {
			params["entry_rules"] = strategyRecord.EntryRules
		}
		if strategyRecord.ExitRules != nil {
			params["exit_rules"] = strategyRecord.ExitRules
		}
		if strategyRecord.RiskParameters != nil {
			params["risk_parameters"] = strategyRecord.RiskParameters
		}

		err := e.registry.StartStrategy(ctx, strategyRecord.Name, params)
		if err != nil {
			e.log.Error("Failed to start strategy", map[string]interface{}{
				"strategy": strategyRecord.Name,
				"error":    err.Error(),
			})
			continue
		}

		e.log.Info("Started strategy from database", map[string]interface{}{
			"strategy": strategyRecord.Name,
			"id":       strategyRecord.ID,
		})
	}

	return nil
}

// Start begins the strategy engine execution
func (e *StrategyEngine) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	e.running = true
	e.wg.Add(1)

	go e.run()

	e.log.Info("Strategy engine started")
	return nil
}

// Stop halts the strategy engine
func (e *StrategyEngine) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	e.cancel()
	e.wg.Wait()
	e.running = false

	e.log.Info("Strategy engine stopped")
	return nil
}

// run is the main execution loop for the strategy engine
func (e *StrategyEngine) run() {
	defer e.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.evaluateStrategies()
		}
	}
}

// evaluateStrategies processes all active strategies
func (e *StrategyEngine) evaluateStrategies() {
	// Get list of running strategies
	strategies := e.registry.GetRunningStrategies()

	for _, strategyName := range strategies {
		strategy, err := e.registry.GetStrategy(strategyName)
		if err != nil {
			e.log.Error("Failed to get strategy", map[string]interface{}{
				"strategy": strategyName,
				"error":    err.Error(),
			})
			continue
		}

		// Process entry signals
		e.processEntrySignals(strategy)

		// Process exit signals
		e.processExitSignals(strategy)

		// Process position management
		e.managePositions(strategy)
	}
}

// processEntrySignals evaluates entry conditions for a strategy
func (e *StrategyEngine) processEntrySignals(strategy base.Strategy) {
	// Get market data for relevant symbols
	// In a real implementation, you would get data for the symbols the strategy is interested in
	// For now, we'll use a placeholder approach

	// Example market data for NIFTY
	marketData := base.MarketDataMap{
		"symbol":         "NIFTY",
		"exchange":       "NSE",
		"last_price":     18500.0,
		"open_interest":  1000000,
		"volume":         5000000,
		"bid_price":      18499.0,
		"ask_price":      18501.0,
		"timestamp":      time.Now(),
		"iv":             20.0,
		"delta":          0.0,
		"theta":          0.0,
		"gamma":          0.0,
		"vega":           0.0,
		"strike_price":   0.0,
		"expiry_date":    time.Now().Add(30 * 24 * time.Hour),
		"option_type":    "",
		"underlying_ltp": 18500.0,
	}

	// Check for entry signals
	signal, err := strategy.ShouldEnter(e.ctx, marketData)
	if err != nil {
		e.log.Error("Error checking entry signals", map[string]interface{}{
			"strategy": strategy.Name(),
			"error":    err.Error(),
		})
		return
	}

	// Process signal if present
	if signal != nil {
		e.executeSignal(strategy, signal)
	}
}

// processExitSignals evaluates exit conditions for a strategy
func (e *StrategyEngine) processExitSignals(strategy base.Strategy) {
	// Get positions for this strategy
	positions, err := e.getPositionsForStrategy(strategy.Name())
	if err != nil {
		e.log.Error("Error getting positions for strategy", map[string]interface{}{
			"strategy": strategy.Name(),
			"error":    err.Error(),
		})
		return
	}

	for _, position := range positions {
		// Get market data for this position
		marketData := e.getMarketDataForSymbol(position.TradingSymbol, position.Exchange)

		// Check for exit signals
		signal, err := strategy.ShouldExit(e.ctx, position, marketData)
		if err != nil {
			e.log.Error("Error checking exit signals", map[string]interface{}{
				"strategy": strategy.Name(),
				"position": position.TradingSymbol,
				"error":    err.Error(),
			})
			continue
		}

		// Process signal if present
		if signal != nil {
			e.executeSignal(strategy, signal)
		}
	}
}

// managePositions handles position management for a strategy
func (e *StrategyEngine) managePositions(strategy base.Strategy) {
	// Get positions for this strategy
	positions, err := e.getPositionsForStrategy(strategy.Name())
	if err != nil {
		e.log.Error("Error getting positions for strategy", map[string]interface{}{
			"strategy": strategy.Name(),
			"error":    err.Error(),
		})
		return
	}

	for _, position := range positions {
		// Get market data for this position
		marketData := e.getMarketDataForSymbol(position.TradingSymbol, position.Exchange)

		// Check for position management actions
		action, err := strategy.ManagePosition(e.ctx, position, marketData)
		if err != nil {
			e.log.Error("Error managing position", map[string]interface{}{
				"strategy": strategy.Name(),
				"position": position.TradingSymbol,
				"error":    err.Error(),
			})
			continue
		}

		// Process action if present
		if action != nil && action.Action != "HOLD" {
			e.executePositionAction(strategy, position, action)
		}
	}
}

// executeSignal places an order based on a strategy signal
func (e *StrategyEngine) executeSignal(strategy base.Strategy, signal *base.SignalResult) {
	// Create order request
	orderRequest := zerodha.PlaceOrderRequest{
		Exchange:      signal.Exchange,
		TradingSymbol: signal.Symbol,
		OrderType:     zerodha.OrderType(signal.OrderType),
		Quantity:      signal.Quantity,
		Product:       zerodha.ProductTypeMIS, // Default to intraday
		Validity:      "DAY",
		Tag:           strategy.Name(), // Use strategy name as tag
	}

	// Set side based on action
	if signal.Action == "BUY" {
		orderRequest.Side = zerodha.OrderSideBuy
	} else {
		orderRequest.Side = zerodha.OrderSideSell
	}

	// Set price for limit orders
	if signal.OrderType == "LIMIT" && signal.Price > 0 {
		orderRequest.Price = signal.Price
	}

	// Set stop loss for SL orders
	if signal.StopLoss > 0 {
		orderRequest.TriggerPrice = signal.StopLoss
	}

	// Log the signal
	e.log.Info("Executing strategy signal", map[string]interface{}{
		"strategy": strategy.Name(),
		"symbol":   signal.Symbol,
		"action":   signal.Action,
		"quantity": signal.Quantity,
		"reason":   signal.Reason,
	})

	// Place the order
	response, err := zerodha.PlaceOrder(orderRequest)

	// Extract order ID if successful
	var orderID string
	if response != nil {
		orderID = response.OrderID
	}
	if err != nil {
		e.log.Error("Failed to place order for strategy signal", map[string]interface{}{
			"strategy": strategy.Name(),
			"symbol":   signal.Symbol,
			"action":   signal.Action,
			"error":    err.Error(),
		})
		return
	}

	e.log.Info("Order placed for strategy signal", map[string]interface{}{
		"strategy": strategy.Name(),
		"symbol":   signal.Symbol,
		"action":   signal.Action,
		"orderID":  orderID,
	})
}

// executePositionAction adjusts a position based on a strategy action
func (e *StrategyEngine) executePositionAction(strategy base.Strategy, position *db.PositionRecord, action *base.PositionAction) {
	if action.Action == "CLOSE" {
		// Create order to close position
		orderRequest := zerodha.PlaceOrderRequest{
			Exchange:      position.Exchange,
			TradingSymbol: position.TradingSymbol,
			OrderType:     zerodha.OrderTypeMarket,
			Quantity:      position.Quantity,
			Product:       zerodha.ProductTypeMIS, // Default to intraday
			Validity:      "DAY",
			Tag:           strategy.Name(), // Use strategy name as tag
			Side:          zerodha.OrderSide(getOppositeTransactionType(position.PositionType)),
		}

		// Log the action
		e.log.Info("Closing position for strategy", map[string]interface{}{
			"strategy": strategy.Name(),
			"symbol":   position.TradingSymbol,
			"action":   action.Action,
			"reason":   action.Reason,
		})

		// Place the order
		response, err := zerodha.PlaceOrder(orderRequest)

		// Extract order ID if successful
		var orderID string
		if response != nil {
			orderID = response.OrderID
		}
		if err != nil {
			e.log.Error("Failed to close position for strategy", map[string]interface{}{
				"strategy": strategy.Name(),
				"symbol":   position.TradingSymbol,
				"error":    err.Error(),
			})
			return
		}

		e.log.Info("Order placed to close position", map[string]interface{}{
			"strategy": strategy.Name(),
			"symbol":   position.TradingSymbol,
			"orderID":  orderID,
		})
	} else if action.Action == "ADJUST" && action.AdjustQuantity != 0 {
		// Create order to adjust position size
		side := zerodha.OrderSideBuy
		if action.AdjustQuantity < 0 {
			side = zerodha.OrderSideSell
		}

		orderRequest := zerodha.PlaceOrderRequest{
			Exchange:      position.Exchange,
			TradingSymbol: position.TradingSymbol,
			Side:          side,
			OrderType:     zerodha.OrderTypeMarket,
			Quantity:      abs(action.AdjustQuantity),
			Product:       zerodha.ProductTypeMIS, // Default to intraday
			Validity:      "DAY",
			Tag:           strategy.Name(), // Use strategy name as tag
		}

		// Log the action
		e.log.Info("Adjusting position for strategy", map[string]interface{}{
			"strategy":  strategy.Name(),
			"symbol":    position.TradingSymbol,
			"action":    action.Action,
			"adjust_by": action.AdjustQuantity,
			"reason":    action.Reason,
		})

		// Place the order
		response, err := zerodha.PlaceOrder(orderRequest)

		// Extract order ID if successful
		var orderID string
		if response != nil {
			orderID = response.OrderID
		}
		if err != nil {
			e.log.Error("Failed to adjust position for strategy", map[string]interface{}{
				"strategy": strategy.Name(),
				"symbol":   position.TradingSymbol,
				"error":    err.Error(),
			})
			return
		}

		e.log.Info("Order placed to adjust position", map[string]interface{}{
			"strategy": strategy.Name(),
			"symbol":   position.TradingSymbol,
			"orderID":  orderID,
		})
	}
}

// getPositionsForStrategy retrieves all positions for a specific strategy
func (e *StrategyEngine) getPositionsForStrategy(strategyName string) ([]*db.PositionRecord, error) {
	// Get all positions from database directly
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	// Get positions for this strategy
	positions, err := timescaleDB.GetPositionsByStrategy(e.ctx, strategyName)
	if err != nil {
		return nil, err
	}

	// Filter positions for this strategy
	var strategyPositions []*db.PositionRecord
	for _, pos := range positions {
		// Skip positions with nil StrategyID
		if pos.StrategyID == nil {
			continue
		}

		// Get strategy name from strategy ID
		strategy, err := e.getStrategyNameByID(*pos.StrategyID)
		if err != nil {
			continue
		}
		if strategy == strategyName {
			strategyPositions = append(strategyPositions, pos)
		}
	}

	return strategyPositions, nil
}

// getStrategyNameByID retrieves the strategy name from its ID
func (e *StrategyEngine) getStrategyNameByID(strategyID int) (string, error) {
	if strategyID == 0 {
		return "", fmt.Errorf("invalid strategy ID: 0")
	}

	// Get the strategy from the database
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return "", fmt.Errorf("timescale DB is nil")
	}

	// Get the strategy by ID
	strategy, err := timescaleDB.GetStrategyByID(context.Background(), strategyID)
	if err != nil {
		return "", fmt.Errorf("failed to get strategy by ID: %w", err)
	}

	return strategy.Name, nil
}

// getMarketDataForSymbol retrieves market data for a specific symbol
func (e *StrategyEngine) getMarketDataForSymbol(symbol, exchange string) base.MarketDataMap {
	// In a real implementation, you would get actual market data
	// For now, we'll return placeholder data

	// Example market data
	return base.MarketDataMap{
		"symbol":         symbol,
		"exchange":       exchange,
		"last_price":     18500.0, // Placeholder
		"open_interest":  1000000,
		"volume":         5000000,
		"bid_price":      18499.0,
		"ask_price":      18501.0,
		"timestamp":      time.Now(),
		"iv":             20.0,
		"delta":          0.0,
		"theta":          0.0,
		"gamma":          0.0,
		"vega":           0.0,
		"strike_price":   0.0,
		"expiry_date":    time.Now().Add(30 * 24 * time.Hour),
		"option_type":    "",
		"underlying_ltp": 18500.0,
	}
}

// abs returns the absolute value of an integer
func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// getOppositeTransactionType returns the opposite transaction type
func getOppositeTransactionType(positionType string) string {
	if positionType == "long" || positionType == "LONG" {
		return "SELL"
	}
	return "BUY"
}
