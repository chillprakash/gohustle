package scheduler

import (
	"context"
	"time"

	"gohustle/logger"
	"gohustle/zerodha"
)

// InitializeStrategyPnLTracking sets up the strategy P&L tracking
func InitializeStrategyPnLTracking(ctx context.Context) {
	scheduler := GetScheduler()
	strategyPnLManager := zerodha.GetStrategyPnLManager()

	task := &Task{
		Name:     "StrategyPnLTracking",
		Interval: 1 * time.Second,
		Execute: func(ctx context.Context) error {
			if !isMarketOpen() {
				return nil
			}
			return strategyPnLManager.CalculateStrategyPnL(ctx)
		},
	}

	scheduler.AddTask(task)
	scheduler.Start(ctx)
	logger.L().Info("Initialized strategy P&L tracking", map[string]interface{}{
		"frequency": "1 second",
	})
}
