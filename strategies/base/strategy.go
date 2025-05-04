package base

import (
	"context"
	"time"

	"gohustle/db"
	"gohustle/logger"
)

// MarketDataMap represents market data as a flexible map
type MarketDataMap map[string]interface{}

// SignalResult represents a trading signal from a strategy
type SignalResult struct {
	Action        string  // "BUY", "SELL", "HOLD"
	Symbol        string  // Trading symbol
	Exchange      string  // Exchange
	Quantity      int     // Quantity to trade
	Price         float64 // Target price
	OrderType     string  // "MARKET", "LIMIT", etc.
	StopLoss      float64 // Stop loss price
	TakeProfit    float64 // Take profit price
	Confidence    float64 // Signal confidence (0-1)
	Reason        string  // Reason for the signal
	Tags          map[string]string // Additional tags
	ExtraData     map[string]interface{} // Additional data
}

// PositionAction represents an action to take on an existing position
type PositionAction struct {
	Action        string  // "CLOSE", "ADJUST", "HOLD"
	AdjustQuantity int     // Quantity to adjust by (positive or negative)
	NewStopLoss    float64 // Updated stop loss
	NewTakeProfit  float64 // Updated take profit
	Reason         string  // Reason for the action
}

// StrategyMetrics contains performance data for a strategy
type StrategyMetrics struct {
	TotalTrades      int
	WinningTrades    int
	LosingTrades     int
	WinRate          float64
	AverageWin       float64
	AverageLoss      float64
	ProfitFactor     float64
	SharpeRatio      float64
	MaxDrawdown      float64
	CAGR             float64
	TotalPnL         float64
	RealizedPnL      float64
	UnrealizedPnL    float64
	AnnualizedReturn float64
	VolatilityDaily  float64
}

// TradeRecord represents a completed trade for performance analysis
type TradeRecord struct {
	StrategyID    int
	EntryTime     time.Time
	ExitTime      time.Time
	Symbol        string
	Exchange      string
	Direction     string // "LONG" or "SHORT"
	EntryPrice    float64
	ExitPrice     float64
	Quantity      int
	PnL           float64
	PnLPercent    float64
	Fees          float64
	NetPnL        float64
	Duration      time.Duration
	ExitReason    string
	Tags          map[string]string
}

// BacktestResult contains the results of a strategy backtest
type BacktestResult struct {
	Metrics       StrategyMetrics
	Trades        []TradeRecord
	EquityCurve   map[time.Time]float64
	Drawdowns     map[time.Time]float64
	MonthlyReturns map[string]float64
	Parameters    map[string]interface{}
}

// AlertCondition defines when an alert should be triggered
type AlertCondition struct {
	Type      string // "PRICE", "TECHNICAL", "TIME", etc.
	Symbol    string
	Parameter string
	Operator  string // ">", "<", "=", etc.
	Value     float64
	ExtraData map[string]interface{} // Additional data
}

// AlertAction defines what to do when an alert is triggered
type AlertAction struct {
	Type       string // "NOTIFY", "ORDER", "ADJUST", etc.
	Message    string
	Recipients []string
	OrderParams map[string]interface{}
}

// AlertResult represents a triggered alert
type AlertResult struct {
	Condition AlertCondition
	Action    AlertAction
	Triggered time.Time
	Executed  bool
}

// Strategy defines the interface for all trading strategies
type Strategy interface {
	// Core methods
	Name() string
	Description() string
	Type() string
	
	// Lifecycle methods
	Initialize(ctx context.Context, params map[string]interface{}) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	
	// Decision methods
	ShouldEnter(ctx context.Context, data MarketDataMap) (*SignalResult, error)
	ShouldExit(ctx context.Context, position *db.PositionRecord, data MarketDataMap) (*SignalResult, error)
	
	// Position sizing and management
	CalculatePositionSize(ctx context.Context, capital float64, risk float64) (int, error)
	ManagePosition(ctx context.Context, position *db.PositionRecord, data MarketDataMap) (*PositionAction, error)
	
	// Performance tracking
	CalculateMetrics(ctx context.Context) (*StrategyMetrics, error)
	GetPerformanceHistory(ctx context.Context, startTime, endTime time.Time) ([]*TradeRecord, error)
	
	// Backtesting methods
	Backtest(ctx context.Context, data []MarketDataMap, capital float64) (*BacktestResult, error)
	
	// Monitoring methods
	SetAlert(condition AlertCondition, action AlertAction) error
	CheckAlerts(ctx context.Context, data MarketDataMap) ([]AlertResult, error)
}

// BaseStrategy provides common functionality for all strategies
type BaseStrategy struct {
	name           string
	description    string
	strategyType   string
	parameters     map[string]interface{}
	entryRules     map[string]interface{}
	exitRules      map[string]interface{}
	riskParameters map[string]interface{}
	active         bool
	creatorID      string
	log            *logger.Logger
	alerts         map[string]struct {
		condition AlertCondition
		action    AlertAction
	}
}

// NewBaseStrategy creates a new base strategy
func NewBaseStrategy(name, description, strategyType string) *BaseStrategy {
	return &BaseStrategy{
		name:           name,
		description:    description,
		strategyType:   strategyType,
		parameters:     make(map[string]interface{}),
		entryRules:     make(map[string]interface{}),
		exitRules:      make(map[string]interface{}),
		riskParameters: make(map[string]interface{}),
		active:         true,
		log:            logger.L(),
		alerts:         make(map[string]struct {
			condition AlertCondition
			action    AlertAction
		}),
	}
}

// Name returns the strategy name
func (s *BaseStrategy) Name() string {
	return s.name
}

// Description returns the strategy description
func (s *BaseStrategy) Description() string {
	return s.description
}

// Type returns the strategy type
func (s *BaseStrategy) Type() string {
	return s.strategyType
}

// Initialize sets up the strategy with parameters
func (s *BaseStrategy) Initialize(ctx context.Context, params map[string]interface{}) error {
	s.parameters = params
	
	// Extract entry rules if provided
	if entryRules, ok := params["entry_rules"].(map[string]interface{}); ok {
		s.entryRules = entryRules
	}
	
	// Extract exit rules if provided
	if exitRules, ok := params["exit_rules"].(map[string]interface{}); ok {
		s.exitRules = exitRules
	}
	
	// Extract risk parameters if provided
	if riskParams, ok := params["risk_parameters"].(map[string]interface{}); ok {
		s.riskParameters = riskParams
	}
	
	s.log.Info("Strategy initialized", map[string]interface{}{
		"strategy": s.name,
		"type":     s.strategyType,
	})
	
	return nil
}

// Start begins strategy execution
func (s *BaseStrategy) Start(ctx context.Context) error {
	s.active = true
	s.log.Info("Strategy started", map[string]interface{}{
		"strategy": s.name,
	})
	return nil
}

// Stop halts strategy execution
func (s *BaseStrategy) Stop(ctx context.Context) error {
	s.active = false
	s.log.Info("Strategy stopped", map[string]interface{}{
		"strategy": s.name,
	})
	return nil
}

// SetAlert adds an alert to the strategy
func (s *BaseStrategy) SetAlert(condition AlertCondition, action AlertAction) error {
	alertKey := condition.Symbol + "_" + condition.Parameter
	s.alerts[alertKey] = struct {
		condition AlertCondition
		action    AlertAction
	}{condition, action}
	
	s.log.Info("Alert set for strategy", map[string]interface{}{
		"strategy":  s.name,
		"symbol":    condition.Symbol,
		"parameter": condition.Parameter,
		"operator":  condition.Operator,
		"value":     condition.Value,
	})
	
	return nil
}

// CheckAlerts checks if any alerts should be triggered
func (s *BaseStrategy) CheckAlerts(ctx context.Context, data MarketDataMap) ([]AlertResult, error) {
	var results []AlertResult
	
	for _, alert := range s.alerts {
		symbol, ok := data["symbol"].(string)
		if !ok || symbol != alert.condition.Symbol {
			continue
		}
		
		triggered := false
		
		// Check if the condition is met
		switch alert.condition.Parameter {
		case "price":
			if lastPrice, ok := data["last_price"].(float64); ok {
				switch alert.condition.Operator {
				case ">":
					triggered = lastPrice > alert.condition.Value
				case "<":
					triggered = lastPrice < alert.condition.Value
				case "=":
					triggered = lastPrice == alert.condition.Value
				}
			}
		// Add more parameter checks as needed
		}
		
		if triggered {
			results = append(results, AlertResult{
				Condition: alert.condition,
				Action:    alert.action,
				Triggered: time.Now(),
				Executed:  false,
			})
			
			lastPrice, _ := data["last_price"].(float64)
			s.log.Info("Alert triggered", map[string]interface{}{
				"strategy":  s.name,
				"symbol":    alert.condition.Symbol,
				"parameter": alert.condition.Parameter,
				"value":     lastPrice,
			})
		}
	}
	
	return results, nil
}

// These methods should be implemented by concrete strategies
func (s *BaseStrategy) ShouldEnter(ctx context.Context, data MarketDataMap) (*SignalResult, error) {
	return nil, nil
}

func (s *BaseStrategy) ShouldExit(ctx context.Context, position *db.PositionRecord, data MarketDataMap) (*SignalResult, error) {
	return nil, nil
}

func (s *BaseStrategy) CalculatePositionSize(ctx context.Context, capital float64, risk float64) (int, error) {
	return 0, nil
}

func (s *BaseStrategy) ManagePosition(ctx context.Context, position *db.PositionRecord, data MarketDataMap) (*PositionAction, error) {
	return nil, nil
}

func (s *BaseStrategy) CalculateMetrics(ctx context.Context) (*StrategyMetrics, error) {
	return nil, nil
}

func (s *BaseStrategy) GetPerformanceHistory(ctx context.Context, startTime, endTime time.Time) ([]*TradeRecord, error) {
	return nil, nil
}

func (s *BaseStrategy) Backtest(ctx context.Context, data []MarketDataMap, capital float64) (*BacktestResult, error) {
	return nil, nil
}
