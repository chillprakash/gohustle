package strategies

import (
	"context"
	"fmt"
	"sync"

	"gohustle/db"
	"gohustle/logger"
	"gohustle/strategies/base"
)

// StrategyFactory creates new strategy instances
type StrategyFactory func(params map[string]interface{}) (base.Strategy, error)

// StrategyRegistry manages all available strategies
type StrategyRegistry struct {
	strategies map[string]StrategyFactory
	running    map[string]base.Strategy
	mu         sync.RWMutex
	log        *logger.Logger
}

var (
	registryInstance *StrategyRegistry
	registryOnce     sync.Once
)

// GetStrategyRegistry returns a singleton instance of StrategyRegistry
func GetStrategyRegistry() *StrategyRegistry {
	registryOnce.Do(func() {
		registryInstance = &StrategyRegistry{
			strategies: make(map[string]StrategyFactory),
			running:    make(map[string]base.Strategy),
			log:        logger.L(),
		}
	})
	return registryInstance
}

// RegisterStrategy adds a strategy to the registry
func (r *StrategyRegistry) RegisterStrategy(name string, factory StrategyFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.strategies[name] = factory
	r.log.Info("Strategy registered", map[string]interface{}{
		"strategy": name,
	})
}

// CreateStrategy instantiates a strategy by name
func (r *StrategyRegistry) CreateStrategy(name string, params map[string]interface{}) (base.Strategy, error) {
	r.mu.RLock()
	factory, exists := r.strategies[name]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("strategy %s not found", name)
	}

	strategy, err := factory(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create strategy %s: %w", name, err)
	}

	return strategy, nil
}

// StartStrategy initializes and starts a strategy
func (r *StrategyRegistry) StartStrategy(ctx context.Context, name string, params map[string]interface{}) error {
	// Check if strategy is already running
	r.mu.RLock()
	_, alreadyRunning := r.running[name]
	r.mu.RUnlock()

	if alreadyRunning {
		return fmt.Errorf("strategy %s is already running", name)
	}

	// Create strategy instance
	strategy, err := r.CreateStrategy(name, params)
	if err != nil {
		return err
	}

	// Initialize strategy
	if err := strategy.Initialize(ctx, params); err != nil {
		return fmt.Errorf("failed to initialize strategy %s: %w", name, err)
	}

	// Start strategy
	if err := strategy.Start(ctx); err != nil {
		return fmt.Errorf("failed to start strategy %s: %w", name, err)
	}

	// Add to running strategies
	r.mu.Lock()
	r.running[name] = strategy
	r.mu.Unlock()

	r.log.Info("Strategy started", map[string]interface{}{
		"strategy": name,
	})

	return nil
}

// StopStrategy stops a running strategy
func (r *StrategyRegistry) StopStrategy(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	strategy, exists := r.running[name]
	if !exists {
		return fmt.Errorf("strategy %s is not running", name)
	}

	if err := strategy.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop strategy %s: %w", name, err)
	}

	delete(r.running, name)

	r.log.Info("Strategy stopped", map[string]interface{}{
		"strategy": name,
	})

	return nil
}

// GetRunningStrategies returns a list of all running strategies
func (r *StrategyRegistry) GetRunningStrategies() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var strategies []string
	for name := range r.running {
		strategies = append(strategies, name)
	}

	return strategies
}

// GetStrategy returns a running strategy by name
func (r *StrategyRegistry) GetStrategy(name string) (base.Strategy, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	strategy, exists := r.running[name]
	if !exists {
		return nil, fmt.Errorf("strategy %s is not running", name)
	}

	return strategy, nil
}

// SaveStrategyToDB persists a strategy to the database
func (r *StrategyRegistry) SaveStrategyToDB(ctx context.Context, strategy base.Strategy, params map[string]interface{}) error {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return fmt.Errorf("timescale DB is nil")
	}

	// Create strategy record
	strategyRecord := &db.StrategyRecord{
		Name:           strategy.Name(),
		Description:    strategy.Description(),
		Type:           strategy.Type(),
		Parameters:     params,
		EntryRules:     params["entry_rules"],
		ExitRules:      params["exit_rules"],
		RiskParameters: params["risk_parameters"],
		Active:         true,
		CreatorID:      "system", // Default creator ID
	}

	// Insert strategy into database
	if err := timescaleDB.UpsertStrategy(ctx, strategyRecord); err != nil {
		r.log.Error("Failed to save strategy to database", map[string]interface{}{
			"strategy": strategy.Name(),
			"error":    err.Error(),
		})
		return err
	}

	r.log.Info("Strategy saved to database", map[string]interface{}{
		"strategy": strategy.Name(),
		"id":       strategyRecord.ID,
	})

	return nil
}

// LoadStrategyFromDB loads a strategy from the database by name
func (r *StrategyRegistry) LoadStrategyFromDB(ctx context.Context, name string) (base.Strategy, error) {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	// Get strategy from database
	strategyRecord, err := timescaleDB.GetStrategyByName(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategy from database: %w", err)
	}

	// Create strategy instance
	factory, exists := r.strategies[strategyRecord.Type]
	if !exists {
		return nil, fmt.Errorf("strategy type %s not found", strategyRecord.Type)
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

	strategy, err := factory(params)
	if err != nil {
		return nil, fmt.Errorf("failed to create strategy: %w", err)
	}

	r.log.Info("Strategy loaded from database", map[string]interface{}{
		"strategy": name,
		"id":       strategyRecord.ID,
	})

	return strategy, nil
}

// ListStrategiesFromDB returns all strategies from the database
func (r *StrategyRegistry) ListStrategiesFromDB(ctx context.Context) ([]*db.StrategyRecord, error) {
	timescaleDB := db.GetTimescaleDB()
	if timescaleDB == nil {
		return nil, fmt.Errorf("timescale DB is nil")
	}

	// Get all strategies from database
	strategies, err := timescaleDB.ListStrategies(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list strategies from database: %w", err)
	}

	return strategies, nil
}
