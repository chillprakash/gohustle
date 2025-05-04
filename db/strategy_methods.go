package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// StrategyRecord represents a row in the strategies table
type StrategyRecord struct {
	ID             int
	Name           string
	Description    string
	Type           string
	Parameters     interface{}
	EntryRules     interface{}
	ExitRules      interface{}
	RiskParameters interface{}
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Active         bool
	CreatorID      string
}

// UpsertStrategy inserts or updates a strategy record in the database
func (t *TimescaleDB) UpsertStrategy(ctx context.Context, strategy *StrategyRecord) error {
	// Convert parameters to JSON
	parametersJSON, err := json.Marshal(strategy.Parameters)
	if err != nil {
		return fmt.Errorf("failed to marshal parameters: %w", err)
	}

	// Convert entry rules to JSON
	var entryRulesJSON []byte
	if strategy.EntryRules != nil {
		entryRulesJSON, err = json.Marshal(strategy.EntryRules)
		if err != nil {
			return fmt.Errorf("failed to marshal entry rules: %w", err)
		}
	}

	// Convert exit rules to JSON
	var exitRulesJSON []byte
	if strategy.ExitRules != nil {
		exitRulesJSON, err = json.Marshal(strategy.ExitRules)
		if err != nil {
			return fmt.Errorf("failed to marshal exit rules: %w", err)
		}
	}

	// Convert risk parameters to JSON
	var riskParamsJSON []byte
	if strategy.RiskParameters != nil {
		riskParamsJSON, err = json.Marshal(strategy.RiskParameters)
		if err != nil {
			return fmt.Errorf("failed to marshal risk parameters: %w", err)
		}
	}

	// Check if strategy already exists
	var exists bool
	var id int
	err = t.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM strategies WHERE name = $1), id
		FROM strategies
		WHERE name = $1
	`, strategy.Name).Scan(&exists, &id)

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to check if strategy exists: %w", err)
	}

	if exists {
		// Update existing strategy
		_, err = t.pool.Exec(ctx, `
			UPDATE strategies
			SET description = $1,
				type = $2,
				parameters = $3,
				entry_rules = $4,
				exit_rules = $5,
				risk_parameters = $6,
				updated_at = NOW(),
				active = $7,
				creator_id = $8
			WHERE name = $9
		`,
			strategy.Description,
			strategy.Type,
			parametersJSON,
			entryRulesJSON,
			exitRulesJSON,
			riskParamsJSON,
			strategy.Active,
			strategy.CreatorID,
			strategy.Name,
		)

		if err != nil {
			return fmt.Errorf("failed to update strategy: %w", err)
		}

		strategy.ID = id
	} else {
		// Insert new strategy
		err = t.pool.QueryRow(ctx, `
			INSERT INTO strategies (
				name,
				description,
				type,
				parameters,
				entry_rules,
				exit_rules,
				risk_parameters,
				active,
				creator_id
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING id
		`,
			strategy.Name,
			strategy.Description,
			strategy.Type,
			parametersJSON,
			entryRulesJSON,
			exitRulesJSON,
			riskParamsJSON,
			strategy.Active,
			strategy.CreatorID,
		).Scan(&strategy.ID)

		if err != nil {
			return fmt.Errorf("failed to insert strategy: %w", err)
		}
	}

	return nil
}

// GetStrategyByID retrieves a strategy by its ID
func (t *TimescaleDB) GetStrategyByID(ctx context.Context, id int) (*StrategyRecord, error) {
	var strategy StrategyRecord
	var parametersJSON, entryRulesJSON, exitRulesJSON, riskParamsJSON []byte

	err := t.pool.QueryRow(ctx, `
		SELECT
			id,
			name,
			description,
			type,
			parameters,
			entry_rules,
			exit_rules,
			risk_parameters,
			created_at,
			updated_at,
			active,
			creator_id
		FROM strategies
		WHERE id = $1
	`, id).Scan(
		&strategy.ID,
		&strategy.Name,
		&strategy.Description,
		&strategy.Type,
		&parametersJSON,
		&entryRulesJSON,
		&exitRulesJSON,
		&riskParamsJSON,
		&strategy.CreatedAt,
		&strategy.UpdatedAt,
		&strategy.Active,
		&strategy.CreatorID,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("strategy with ID %d not found", id)
		}
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Parse JSON fields
	if len(parametersJSON) > 0 {
		var params map[string]interface{}
		if err := json.Unmarshal(parametersJSON, &params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal parameters: %w", err)
		}
		strategy.Parameters = params
	}

	if len(entryRulesJSON) > 0 {
		var rules map[string]interface{}
		if err := json.Unmarshal(entryRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry rules: %w", err)
		}
		strategy.EntryRules = rules
	}

	if len(exitRulesJSON) > 0 {
		var rules map[string]interface{}
		if err := json.Unmarshal(exitRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal exit rules: %w", err)
		}
		strategy.ExitRules = rules
	}

	if len(riskParamsJSON) > 0 {
		var params map[string]interface{}
		if err := json.Unmarshal(riskParamsJSON, &params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk parameters: %w", err)
		}
		strategy.RiskParameters = params
	}

	return &strategy, nil
}

// GetStrategyByName retrieves a strategy by its name
func (t *TimescaleDB) GetStrategyByName(ctx context.Context, name string) (*StrategyRecord, error) {
	var strategy StrategyRecord
	var parametersJSON, entryRulesJSON, exitRulesJSON, riskParamsJSON []byte

	err := t.pool.QueryRow(ctx, `
		SELECT
			id,
			name,
			description,
			type,
			parameters,
			entry_rules,
			exit_rules,
			risk_parameters,
			created_at,
			updated_at,
			active,
			creator_id
		FROM strategies
		WHERE name = $1
	`, name).Scan(
		&strategy.ID,
		&strategy.Name,
		&strategy.Description,
		&strategy.Type,
		&parametersJSON,
		&entryRulesJSON,
		&exitRulesJSON,
		&riskParamsJSON,
		&strategy.CreatedAt,
		&strategy.UpdatedAt,
		&strategy.Active,
		&strategy.CreatorID,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("strategy with name %s not found", name)
		}
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Parse JSON fields
	if len(parametersJSON) > 0 {
		var params map[string]interface{}
		if err := json.Unmarshal(parametersJSON, &params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal parameters: %w", err)
		}
		strategy.Parameters = params
	}

	if len(entryRulesJSON) > 0 {
		var rules map[string]interface{}
		if err := json.Unmarshal(entryRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry rules: %w", err)
		}
		strategy.EntryRules = rules
	}

	if len(exitRulesJSON) > 0 {
		var rules map[string]interface{}
		if err := json.Unmarshal(exitRulesJSON, &rules); err != nil {
			return nil, fmt.Errorf("failed to unmarshal exit rules: %w", err)
		}
		strategy.ExitRules = rules
	}

	if len(riskParamsJSON) > 0 {
		var params map[string]interface{}
		if err := json.Unmarshal(riskParamsJSON, &params); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk parameters: %w", err)
		}
		strategy.RiskParameters = params
	}

	return &strategy, nil
}

// ListStrategies retrieves all strategies from the database
func (t *TimescaleDB) ListStrategies(ctx context.Context) ([]*StrategyRecord, error) {
	rows, err := t.pool.Query(ctx, `
		SELECT
			id,
			name,
			description,
			type,
			parameters,
			entry_rules,
			exit_rules,
			risk_parameters,
			created_at,
			updated_at,
			active,
			creator_id
		FROM strategies
		ORDER BY name
	`)

	if err != nil {
		return nil, fmt.Errorf("failed to list strategies: %w", err)
	}
	defer rows.Close()

	var strategies []*StrategyRecord

	for rows.Next() {
		var strategy StrategyRecord
		var parametersJSON, entryRulesJSON, exitRulesJSON, riskParamsJSON []byte

		err := rows.Scan(
			&strategy.ID,
			&strategy.Name,
			&strategy.Description,
			&strategy.Type,
			&parametersJSON,
			&entryRulesJSON,
			&exitRulesJSON,
			&riskParamsJSON,
			&strategy.CreatedAt,
			&strategy.UpdatedAt,
			&strategy.Active,
			&strategy.CreatorID,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan strategy row: %w", err)
		}

		// Parse JSON fields
		if len(parametersJSON) > 0 {
			var params map[string]interface{}
			if err := json.Unmarshal(parametersJSON, &params); err != nil {
				return nil, fmt.Errorf("failed to unmarshal parameters: %w", err)
			}
			strategy.Parameters = params
		}

		if len(entryRulesJSON) > 0 {
			var rules map[string]interface{}
			if err := json.Unmarshal(entryRulesJSON, &rules); err != nil {
				return nil, fmt.Errorf("failed to unmarshal entry rules: %w", err)
			}
			strategy.EntryRules = rules
		}

		if len(exitRulesJSON) > 0 {
			var rules map[string]interface{}
			if err := json.Unmarshal(exitRulesJSON, &rules); err != nil {
				return nil, fmt.Errorf("failed to unmarshal exit rules: %w", err)
			}
			strategy.ExitRules = rules
		}

		if len(riskParamsJSON) > 0 {
			var params map[string]interface{}
			if err := json.Unmarshal(riskParamsJSON, &params); err != nil {
				return nil, fmt.Errorf("failed to unmarshal risk parameters: %w", err)
			}
			strategy.RiskParameters = params
		}

		strategies = append(strategies, &strategy)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating strategy rows: %w", err)
	}

	return strategies, nil
}

// DeleteStrategy removes a strategy from the database
func (t *TimescaleDB) DeleteStrategy(ctx context.Context, id int) error {
	_, err := t.pool.Exec(ctx, `
		DELETE FROM strategies
		WHERE id = $1
	`, id)

	if err != nil {
		return fmt.Errorf("failed to delete strategy: %w", err)
	}

	return nil
}

// GetPositionsByStrategy retrieves all positions for a specific strategy
func (t *TimescaleDB) GetPositionsByStrategy(ctx context.Context, strategyName string) ([]*PositionRecord, error) {
	rows, err := t.pool.Query(ctx, `
		SELECT
			id,
			position_id,
			trading_symbol,
			exchange,
			product,
			quantity,
			average_price,
			last_price,
			pnl,
			realized_pnl,
			unrealized_pnl,
			multiplier,
			buy_quantity,
			sell_quantity,
			paper_trading,
			strategy
		FROM positions
		WHERE strategy = $1
	`, strategyName)

	if err != nil {
		return nil, fmt.Errorf("failed to get positions by strategy: %w", err)
	}
	defer rows.Close()

	var positions []*PositionRecord

	for rows.Next() {
		var position PositionRecord

		err := rows.Scan(
			&position.ID,
			&position.PositionID,
			&position.TradingSymbol,
			&position.Exchange,
			&position.Product,
			&position.Quantity,
			&position.AveragePrice,
			&position.LastPrice,
			&position.PnL,
			&position.RealizedPnL,
			&position.UnrealizedPnL,
			&position.Multiplier,
			&position.BuyQuantity,
			&position.SellQuantity,
			&position.PaperTrading,
			&position.StrategyID,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan position row: %w", err)
		}

		positions = append(positions, &position)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating position rows: %w", err)
	}

	return positions, nil
}

// GetTradesByStrategy retrieves all trades for a specific strategy
func (t *TimescaleDB) GetTradesByStrategy(ctx context.Context, strategyName string) ([]*OrderRecord, error) {
	rows, err := t.pool.Query(ctx, `
		SELECT
			id,
			order_id,
			status,
			exchange,
			trading_symbol,
			order_type,
			quantity,
			price,
			trigger_price,
			product,
			validity,
			tag,
			user_id,
			placed_at,
			paper_trading
		FROM orders
		WHERE tag = $1
		ORDER BY placed_at DESC
	`, strategyName)

	if err != nil {
		return nil, fmt.Errorf("failed to get trades by strategy: %w", err)
	}
	defer rows.Close()

	var orders []*OrderRecord

	for rows.Next() {
		var order OrderRecord

		err := rows.Scan(
			&order.ID,
			&order.OrderID,
			&order.Status,
			&order.Exchange,
			&order.TradingSymbol,
			&order.OrderType,
			&order.Quantity,
			&order.Price,
			&order.TriggerPrice,
			&order.Product,
			&order.Validity,
			&order.Tag,
			&order.UserID,
			&order.PlacedAt,
			&order.PaperTrading,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan order row: %w", err)
		}

		orders = append(orders, &order)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating order rows: %w", err)
	}

	return orders, nil
}

// GetStrategyPerformance calculates performance metrics for a strategy
func (t *TimescaleDB) GetStrategyPerformance(ctx context.Context, strategyName string, startTime, endTime time.Time) (map[string]interface{}, error) {
	// Get all completed trades for this strategy in the time range
	rows, err := t.pool.Query(ctx, `
		SELECT
			order_type,
			quantity,
			price,
			placed_at
		FROM orders
		WHERE tag = $1
		AND status = 'COMPLETE'
		AND placed_at BETWEEN $2 AND $3
		ORDER BY placed_at
	`, strategyName, startTime, endTime)

	if err != nil {
		return nil, fmt.Errorf("failed to get strategy trades: %w", err)
	}
	defer rows.Close()

	// Calculate performance metrics
	var totalTrades, winningTrades, losingTrades int
	var totalPnL, totalWin, totalLoss float64

	// This is a simplified calculation - in a real implementation,
	// you would need to pair buy and sell orders to calculate actual P&L
	// For now, we'll just count the number of trades

	for rows.Next() {
		var orderType string
		var quantity int
		var price float64
		var placedAt time.Time

		err := rows.Scan(
			&orderType,
			&quantity,
			&price,
			&placedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan trade row: %w", err)
		}

		totalTrades++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating trade rows: %w", err)
	}

	// Get current positions for this strategy
	positions, err := t.GetPositionsByStrategy(ctx, strategyName)
	if err != nil {
		return nil, fmt.Errorf("failed to get positions: %w", err)
	}

	// Calculate unrealized P&L
	var unrealizedPnL float64
	for _, pos := range positions {
		unrealizedPnL += pos.PnL
	}

	// Calculate win rate
	var winRate float64
	if totalTrades > 0 {
		winRate = float64(winningTrades) / float64(totalTrades) * 100
	}

	// Calculate profit factor
	var profitFactor float64
	if totalLoss != 0 {
		profitFactor = totalWin / math.Abs(totalLoss)
	}

	// Return performance metrics
	return map[string]interface{}{
		"total_trades":    totalTrades,
		"winning_trades":  winningTrades,
		"losing_trades":   losingTrades,
		"win_rate":        winRate,
		"total_pnl":       totalPnL,
		"unrealized_pnl":  unrealizedPnL,
		"profit_factor":   profitFactor,
	}, nil
}
