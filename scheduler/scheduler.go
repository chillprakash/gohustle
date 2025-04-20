package scheduler

import (
	"context"
	"sync"
	"time"

	"gohustle/logger"
	"gohustle/zerodha"
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

// runTask executes a single task at the specified interval
func (s *Scheduler) runTask(ctx context.Context, task *Task) {
	defer s.wg.Done()
	ticker := time.NewTicker(task.Interval)
	defer ticker.Stop()

	// Execute immediately on start
	if err := task.Execute(ctx); err != nil {
		s.log.Error("Task execution failed", map[string]interface{}{
			"task_name": task.Name,
			"error":     err.Error(),
		})
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
			return positionManager.PollPositionsAndUpdateInRedis(ctx)
		},
	}

	scheduler.AddTask(task)
	scheduler.Start(ctx)
}
