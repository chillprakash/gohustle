package scheduler

import (
	"context"
	"gohustle/logger"
	"gohustle/zerodha"
	"sync"
	"time"
)

// Task represents a scheduled task
type Task struct {
	Name     string
	Interval time.Duration
	Execute  func(context.Context) error
}

// Scheduler manages periodic tasks
type Scheduler struct {
	tasks    []*Task
	stopChan chan struct{}
	wg       sync.WaitGroup
	log      *logger.Logger
	mu       sync.RWMutex
}

var (
	instance *Scheduler
	once     sync.Once
)

// GetScheduler returns singleton instance of Scheduler
func GetScheduler() *Scheduler {
	once.Do(func() {
		instance = &Scheduler{
			tasks:    make([]*Task, 0),
			stopChan: make(chan struct{}),
			log:      logger.L(),
		}
	})
	return instance
}

// AddTask adds a new task to the scheduler
func (s *Scheduler) AddTask(task *Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, task)
	s.log.Info("Added new task to scheduler", map[string]interface{}{
		"task_name": task.Name,
		"interval":  task.Interval,
	})
}

// Start begins all scheduled tasks
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.RLock()
	tasks := make([]*Task, len(s.tasks))
	copy(tasks, s.tasks)
	s.mu.RUnlock()

	for _, task := range tasks {
		s.wg.Add(1)
		go s.runTask(ctx, task)
	}

	s.log.Info("Scheduler started", map[string]interface{}{
		"tasks_count": len(tasks),
	})
}

// Stop gracefully stops all scheduled tasks
func (s *Scheduler) Stop() {
	close(s.stopChan)
	s.wg.Wait()
	s.log.Info("Scheduler stopped", map[string]interface{}{})
}

// MarketHours represents the market trading hours in IST
type MarketHours struct {
	OpenTime  time.Time
	CloseTime time.Time
}

// isMarketOpen checks if the market is currently open
func isMarketOpen() bool {
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
	closeTime := today.Add(17*time.Hour + 35*time.Minute) // 3:35 PM IST

	// Check if it's a weekday (Monday = 1, Sunday = 7)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}

	// Check if current time is within market hours
	return now.After(openTime) && now.Before(closeTime)
}

// runTask executes a single task at the specified interval
func (s *Scheduler) runTask(ctx context.Context, task *Task) {
	defer s.wg.Done()
	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()

	// Execute immediately on start if market is open
	if isMarketOpen() {
		if err := task.Execute(ctx); err != nil {
			s.log.Error("Task execution failed", map[string]interface{}{
				"task_name": task.Name,
				"error":     err.Error(),
			})
		}
	}

	for {
		select {
		case <-ctx.Done():
			s.log.Info("Task stopped due to context cancellation", map[string]interface{}{
				"task_name": task.Name,
			})
			return
		case <-s.stopChan:
			s.log.Info("Task stopped due to scheduler shutdown", map[string]interface{}{
				"task_name": task.Name,
			})
			return
		case <-ticker.C:
			if !isMarketOpen() {
				s.log.Debug("Skipping task execution outside market hours", map[string]interface{}{
					"task_name": task.Name,
				})
				continue
			}

			if err := task.Execute(ctx); err != nil {
				s.log.Error("Task execution failed", map[string]interface{}{
					"task_name": task.Name,
					"error":     err.Error(),
				})
			}
		}
	}
}

// InitializePositionPolling sets up position polling task
func InitializePositionPolling(ctx context.Context) {
	scheduler := GetScheduler()
	positionManager := zerodha.GetPositionManager()

	task := &Task{
		Name:     "PositionPolling",
		Interval: time.Second,
		Execute: func(ctx context.Context) error {
			if !isMarketOpen() {
				return nil
			}
			return positionManager.PollPositionsAndUpdateInRedis(ctx)
		},
	}

	scheduler.AddTask(task)
	scheduler.Start(ctx)
}
