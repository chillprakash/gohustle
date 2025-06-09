package iron_condor

import (
	"context"
	"fmt"
	"math"
	"time"

	"gohustle/backend/db"
	"gohustle/backend/strategies/base"
)

// IronCondorStrategy implements an iron condor options strategy
type IronCondorStrategy struct {
	*base.BaseStrategy
	
	// Strategy-specific parameters
	width              int     // Width between strikes
	shortDelta         float64 // Target delta for short options
	daysToExpiry       int     // Target days to expiry
	profitTarget       float64 // Profit target as percentage of max credit
	stopLoss           float64 // Stop loss as percentage of max credit
	maxPositionSize    int     // Maximum position size
	ivRankMinimum      float64 // Minimum IV rank to enter
	daysToEarningsMin  int     // Minimum days to earnings
	dteExit            int     // Days to expiry to exit
	underlyingSymbols  []string // Underlying symbols to trade
}

// NewIronCondorStrategy creates a new iron condor strategy
func NewIronCondorStrategy() base.Strategy {
	baseStrategy := base.NewBaseStrategy(
		"Iron Condor",
		"A market-neutral options strategy that profits from low volatility",
		"options",
	)
	
	return &IronCondorStrategy{
		BaseStrategy: baseStrategy,
		width:        10,
		shortDelta:   0.3,
		daysToExpiry: 30,
		profitTarget: 50,  // 50% of max credit
		stopLoss:     200, // 200% of max credit (2x loss)
		maxPositionSize: 5,
		ivRankMinimum: 40,
		daysToEarningsMin: 10,
		dteExit: 7,
		underlyingSymbols: []string{"NIFTY", "BANKNIFTY"},
	}
}

// Initialize sets up the strategy with parameters
func (s *IronCondorStrategy) Initialize(ctx context.Context, params map[string]interface{}) error {
	// Call base implementation first
	if err := s.BaseStrategy.Initialize(ctx, params); err != nil {
		return err
	}
	
	// Extract strategy-specific parameters
	if width, ok := params["width"].(float64); ok {
		s.width = int(width)
	}
	
	if delta, ok := params["delta"].(float64); ok {
		s.shortDelta = delta
	}
	
	if dte, ok := params["days_to_expiry"].(float64); ok {
		s.daysToExpiry = int(dte)
	}
	
	if profit, ok := params["profit_target_percent"].(float64); ok {
		s.profitTarget = profit
	}
	
	if loss, ok := params["max_loss_percent"].(float64); ok {
		s.stopLoss = loss
	}
	
	if size, ok := params["max_position_size"].(float64); ok {
		s.maxPositionSize = int(size)
	}
	
	if ivRank, ok := params["iv_rank_min"].(float64); ok {
		s.ivRankMinimum = ivRank
	}
	
	if daysToEarnings, ok := params["days_to_earnings_min"].(float64); ok {
		s.daysToEarningsMin = int(daysToEarnings)
	}
	
	if dteExit, ok := params["dte_exit"].(float64); ok {
		s.dteExit = int(dteExit)
	}
	
	if symbols, ok := params["underlying_symbols"].([]interface{}); ok {
		s.underlyingSymbols = make([]string, 0, len(symbols))
		for _, sym := range symbols {
			if symStr, ok := sym.(string); ok {
				s.underlyingSymbols = append(s.underlyingSymbols, symStr)
			}
		}
	}
	
	return nil
}

// ShouldEnter determines if an iron condor should be entered
func (s *IronCondorStrategy) ShouldEnter(ctx context.Context, data base.MarketDataMap) (*base.SignalResult, error) {
	// Check if this is a symbol we're interested in
	symbol, ok := data["symbol"].(string)
	if !ok {
		return nil, nil
	}
	
	isTargetSymbol := false
	for _, sym := range s.underlyingSymbols {
		if symbol == sym {
			isTargetSymbol = true
			break
		}
	}
	
	if !isTargetSymbol {
		return nil, nil
	}
	
	// Check IV rank
	iv, ok := data["iv"].(float64)
	if !ok || iv < s.ivRankMinimum {
		return nil, nil
	}
	
	// Find appropriate expiry
	// In a real implementation, you would query available expirations
	// For now, we'll assume the data contains the appropriate expiry
	
	// Calculate strike prices based on delta
	// In a real implementation, you would query option chains
	// For now, we'll use a simplified approach
	
	// Assume ATM strike is close to current price
	lastPrice, ok := data["last_price"].(float64)
	if !ok {
		return nil, nil
	}
	
	atmStrike := math.Round(lastPrice/100) * 100
	
	// Calculate short strikes based on delta target
	// This is simplified - in reality you'd look up the actual option chain
	putShortStrike := atmStrike - (atmStrike * s.shortDelta * 0.1)
	callShortStrike := atmStrike + (atmStrike * s.shortDelta * 0.1)
	
	// Calculate long strikes based on width
	putLongStrike := putShortStrike - (float64(s.width) * 100)
	callLongStrike := callShortStrike + (float64(s.width) * 100)
	
	// Format trading symbols
	// This is a simplified example - actual symbol construction depends on exchange format
	expiryDate, ok := data["expiry_date"].(time.Time)
	if !ok {
		// Use a default expiry date if not provided
		expiryDate = time.Now().AddDate(0, 0, s.daysToExpiry)
	}
	
	expiryStr := expiryDate.Format("02JAN06")
	putShortSymbol := fmt.Sprintf("%s%s%dPE", symbol, expiryStr, int(putShortStrike))
	putLongSymbol := fmt.Sprintf("%s%s%dPE", symbol, expiryStr, int(putLongStrike))
	callShortSymbol := fmt.Sprintf("%s%s%dCE", symbol, expiryStr, int(callShortStrike))
	callLongSymbol := fmt.Sprintf("%s%s%dCE", symbol, expiryStr, int(callLongStrike))
	
	// Calculate position size
	posSize := s.maxPositionSize
	
	// Create signal for each leg of the iron condor
	signals := []*base.SignalResult{
		{
			Action:     "SELL",
			Symbol:     putShortSymbol,
			Exchange:   data["exchange"].(string),
			Quantity:   posSize,
			OrderType:  "LIMIT",
			Confidence: 0.8,
			Reason:     "Iron Condor PUT Short Leg",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"leg":      "put_short",
			},
		},
		{
			Action:     "BUY",
			Symbol:     putLongSymbol,
			Exchange:   data["exchange"].(string),
			Quantity:   posSize,
			OrderType:  "LIMIT",
			Confidence: 0.8,
			Reason:     "Iron Condor PUT Long Leg",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"leg":      "put_long",
			},
		},
		{
			Action:     "SELL",
			Symbol:     callShortSymbol,
			Exchange:   data["exchange"].(string),
			Quantity:   posSize,
			OrderType:  "LIMIT",
			Confidence: 0.8,
			Reason:     "Iron Condor CALL Short Leg",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"leg":      "call_short",
			},
		},
		{
			Action:     "BUY",
			Symbol:     callLongSymbol,
			Exchange:   data["exchange"].(string),
			Quantity:   posSize,
			OrderType:  "LIMIT",
			Confidence: 0.8,
			Reason:     "Iron Condor CALL Long Leg",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"leg":      "call_long",
			},
		},
	}
	
	// In a real implementation, you would return all signals
	// For this example, we'll just return the first one
	return signals[0], nil
}

// ShouldExit determines if an iron condor position should be exited
func (s *IronCondorStrategy) ShouldExit(ctx context.Context, position *db.PositionRecord, data base.MarketDataMap) (*base.SignalResult, error) {
	// For now, skip strategy check since we're transitioning from Strategy to StrategyID
	// and need to handle existing positions that might have nil StrategyID
	
	// Calculate days to expiry
	// In a real implementation, you would extract this from the position
	// For now, we'll use the data expiry
	expiryDate, ok := data["expiry_date"].(time.Time)
	if !ok {
		// Use a default if not provided
		return nil, nil
	}
	
	daysToExpiry := int(math.Ceil(expiryDate.Sub(time.Now()).Hours() / 24))
	
	// Check if we're close to expiry
	if daysToExpiry <= s.dteExit {
		return &base.SignalResult{
			Action:     "BUY", // Opposite of the original position action
			Symbol:     position.TradingSymbol,
			Exchange:   position.Exchange,
			Quantity:   position.Quantity,
			OrderType:  "MARKET",
			Confidence: 0.9,
			Reason:     "Iron Condor exit due to approaching expiry",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"exit_reason": "dte",
			},
		}, nil
	}
	
	// Check profit target
	// For simplicity, we'll use position P&L
	// In a real implementation, you would calculate this based on the entire iron condor
	if position.PnL > 0 && (position.PnL/position.AveragePrice*100) >= s.profitTarget {
		return &base.SignalResult{
			Action:     "BUY", // Opposite of the original position action
			Symbol:     position.TradingSymbol,
			Exchange:   position.Exchange,
			Quantity:   position.Quantity,
			OrderType:  "MARKET",
			Confidence: 0.9,
			Reason:     "Iron Condor profit target reached",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"exit_reason": "profit_target",
			},
		}, nil
	}
	
	// Check stop loss
	if position.PnL < 0 && (math.Abs(position.PnL)/position.AveragePrice*100) >= s.stopLoss {
		return &base.SignalResult{
			Action:     "BUY", // Opposite of the original position action
			Symbol:     position.TradingSymbol,
			Exchange:   position.Exchange,
			Quantity:   position.Quantity,
			OrderType:  "MARKET",
			Confidence: 0.9,
			Reason:     "Iron Condor stop loss triggered",
			Tags: map[string]string{
				"strategy": "iron_condor",
				"exit_reason": "stop_loss",
			},
		}, nil
	}
	
	return nil, nil
}

// CalculatePositionSize determines the appropriate size for an iron condor
func (s *IronCondorStrategy) CalculatePositionSize(ctx context.Context, capital float64, risk float64) (int, error) {
	// Simple position sizing based on capital and risk
	// In a real implementation, you would consider margin requirements, etc.
	
	// Calculate max risk per iron condor (width between strikes - credit received)
	// For simplicity, we'll assume a fixed risk per contract
	riskPerContract := 10000.0 // Rs. 10,000 per contract
	
	// Calculate max number of contracts based on risk tolerance
	maxRiskAmount := capital * (risk / 100)
	maxContracts := int(maxRiskAmount / riskPerContract)
	
	// Limit to configured maximum
	if maxContracts > s.maxPositionSize {
		maxContracts = s.maxPositionSize
	}
	
	return maxContracts, nil
}

// ManagePosition handles adjustments to an existing iron condor
func (s *IronCondorStrategy) ManagePosition(ctx context.Context, position *db.PositionRecord, data base.MarketDataMap) (*base.PositionAction, error) {
	// For now, skip strategy check since we're transitioning from Strategy to StrategyID
	// and need to handle existing positions that might have nil StrategyID
	
	// In a real implementation, you would implement adjustment logic
	// For example, rolling threatened sides, adding wings, etc.
	
	// For now, we'll just return a hold action
	return &base.PositionAction{
		Action: "HOLD",
		Reason: "No adjustment needed",
	}, nil
}

// Backtest runs a backtest of the iron condor strategy
func (s *IronCondorStrategy) Backtest(ctx context.Context, data []base.MarketDataMap, capital float64) (*base.BacktestResult, error) {
	// In a real implementation, you would simulate the strategy on historical data
	// For now, we'll return a placeholder result
	
	return &base.BacktestResult{
		Metrics: base.StrategyMetrics{
			TotalTrades:   10,
			WinningTrades: 7,
			LosingTrades:  3,
			WinRate:       70.0,
			ProfitFactor:  2.1,
			TotalPnL:      15000.0,
		},
		Trades:        []base.TradeRecord{},
		EquityCurve:   make(map[time.Time]float64),
		MonthlyReturns: make(map[string]float64),
		Parameters:    make(map[string]interface{}),
	}, nil
}
