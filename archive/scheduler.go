package archive

import (
	"context"
	"gohustle/logger"
	"gohustle/scheduler"
	"time"
)

// isWithinMarketHours checks if the current time is within Indian stock market hours (9 AM to 3:35 PM IST)
func isWithinMarketHours() bool {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.L().Error("Failed to load IST timezone", map[string]interface{}{
			"error": err.Error(),
		})
		return false
	}

	now := time.Now().In(ist)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, ist)

	openTime := today.Add(9 * time.Hour)                  // 9:00 AM IST
	closeTime := today.Add(15*time.Hour + 35*time.Minute) // 3:35 PM IST

	// Check if it's a weekday (Monday = 1, Sunday = 7)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	// Check if current time is within market hours
	return now.After(openTime) && now.Before(closeTime)
}

// shouldRunConsolidationJob determines if we should run the consolidation job based on time
// Consolidation should only happen after market hours
func shouldRunConsolidationJob() bool {
	// Get current time in IST
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.L().Error("Failed to load IST timezone", map[string]interface{}{
			"error": err.Error(),
		})
		return false
	}

	now := time.Now().In(ist)
	
	// Check if it's a weekday
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		// Allow consolidation on weekends at any time
		return true
	}
	
	// On weekdays, only run consolidation after market closes (after 3:35 PM)
	// or before market opens (before 9 AM)
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, ist)
	openTime := today.Add(9 * time.Hour)
	closeTime := today.Add(15*time.Hour + 35*time.Minute)
	
	return now.After(closeTime) || now.Before(openTime)
}

// InitializeTickDataArchiving sets up periodic tick data archiving
func InitializeTickDataArchiving(ctx context.Context) {
	sched := scheduler.GetScheduler()
	
	// Create a task that runs hourly for archiving
	archiveTask := &scheduler.Task{
		Name:     "TickDataArchiving",
		Interval: 1 * time.Hour,
		Execute: func(ctx context.Context) error {
			logger.L().Info("Running hourly tick data archiving job", nil)
			return ExecuteTickArchiveJob(ctx)
		},
	}
	
	sched.AddTask(archiveTask)
	logger.L().Info("Initialized tick data archiving", map[string]interface{}{
		"frequency": "Every hour",
	})
}

// InitializeTickDataConsolidation sets up periodic tick data consolidation
func InitializeTickDataConsolidation(ctx context.Context) {
	sched := scheduler.GetScheduler()
	
	// Create a task that checks every hour if consolidation should run
	task := &scheduler.Task{
		Name:     "TickDataConsolidation",
		Interval: 1 * time.Hour,
		Execute: func(ctx context.Context) error {
			// Only run consolidation after market hours
			if !shouldRunConsolidationJob() {
				logger.L().Debug("Skipping tick data consolidation - market hours", nil)
				return nil
			}
			
			logger.L().Info("Running tick data consolidation job", nil)
			return ExecuteTickConsolidationJob(ctx)
		},
	}
	
	sched.AddTask(task)
	logger.L().Info("Initialized tick data consolidation", map[string]interface{}{
		"schedule": "Hourly checks, only executing outside market hours",
	})
}
