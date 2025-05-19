package db

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v4"
)

// UpsertPosition inserts or updates a position record in the positions table
func (t *TimescaleDB) UpsertPosition(ctx context.Context, position *PositionRecord) error {
	// If ID is set, update existing position
	if position.ID > 0 {
		query := `
		UPDATE positions SET
			trading_symbol = $2,
			exchange = $3,
			product = $4,
			quantity = $5,
			average_price = $6,
			last_price = $7,
			pnl = $8,
			realized_pnl = $9,
			unrealized_pnl = $10,
			multiplier = $11,
			buy_quantity = $12,
			sell_quantity = $13,
			buy_price = $14,
			sell_price = $15,
			buy_value = $16,
			sell_value = $17,
			position_type = $18,
			strategy_id = $19,
			user_id = $20,
			updated_at = $21,
			paper_trading = $22,
			kite_response = $23
		WHERE id = $1
		RETURNING id`

		// Convert KiteResponse to JSON if it's not nil
		var kiteRespJSON interface{}
		if position.KiteResponse != nil {
			kiteRespBytes, err := json.Marshal(position.KiteResponse)
			if err != nil {
				t.log.Error("Failed to marshal position KiteResponse to JSON", map[string]interface{}{"error": err.Error()})
				return err
			}
			kiteRespJSON = kiteRespBytes
		}

		return t.pool.QueryRow(ctx, query,
			position.ID,
			position.TradingSymbol,
			position.Exchange,
			position.Product,
			position.Quantity,
			position.AveragePrice,
			position.LastPrice,
			position.PnL,
			position.RealizedPnL,
			position.UnrealizedPnL,
			position.Multiplier,
			position.BuyQuantity,
			position.SellQuantity,
			position.BuyPrice,
			position.SellPrice,
			position.BuyValue,
			position.SellValue,
			position.PositionType,
			position.StrategyID,
			position.UserID,
			position.UpdatedAt,
			position.PaperTrading,
			kiteRespJSON,
		).Scan(&position.ID)
	}

	// Otherwise insert new position
	query := `
	INSERT INTO positions (
		trading_symbol, exchange, product, quantity, 
		average_price, last_price, pnl, realized_pnl, unrealized_pnl, 
		multiplier, buy_quantity, sell_quantity, buy_price, sell_price, 
		buy_value, sell_value, position_type, strategy_id, user_id, updated_at, paper_trading, kite_response
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
	) 
	RETURNING id`

	// Convert KiteResponse to JSON if it's not nil
	var kiteRespJSON interface{}
	if position.KiteResponse != nil {
		kiteRespBytes, err := json.Marshal(position.KiteResponse)
		if err != nil {
			t.log.Error("Failed to marshal position KiteResponse to JSON", map[string]interface{}{"error": err.Error()})
			return err
		}
		kiteRespJSON = kiteRespBytes
	}

	return t.pool.QueryRow(ctx, query,
		position.TradingSymbol,
		position.Exchange,
		position.Product,
		position.Quantity,
		position.AveragePrice,
		position.LastPrice,
		position.PnL,
		position.RealizedPnL,
		position.UnrealizedPnL,
		position.Multiplier,
		position.BuyQuantity,
		position.SellQuantity,
		position.BuyPrice,
		position.SellPrice,
		position.BuyValue,
		position.SellValue,
		position.PositionType,
		position.StrategyID,
		position.UserID,
		position.UpdatedAt,
		position.PaperTrading,
		kiteRespJSON,
	).Scan(&position.ID)
}

// GetPositionByTradingSymbol fetches a position record by trading symbol
func (t *TimescaleDB) GetPositionByTradingSymbol(ctx context.Context, tradingSymbol string, paperTrading bool) (*PositionRecord, error) {
	query := `SELECT id, position_id, trading_symbol, exchange, product, quantity, average_price, last_price, pnl, realized_pnl, unrealized_pnl, multiplier, buy_quantity, sell_quantity, buy_price, sell_price, buy_value, sell_value, position_type, strategy_id, user_id, updated_at, paper_trading, kite_response FROM positions WHERE trading_symbol = $1 AND paper_trading = $2 ORDER BY updated_at DESC LIMIT 1`

	position := &PositionRecord{}
	var kiteRespBytes []byte
	err := t.pool.QueryRow(ctx, query, tradingSymbol, paperTrading).Scan(
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
		&position.BuyPrice,
		&position.SellPrice,
		&position.BuyValue,
		&position.SellValue,
		&position.PositionType,
		&position.StrategyID,
		&position.UserID,
		&position.UpdatedAt,
		&position.PaperTrading,
		&kiteRespBytes,
	)

	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No position found
		}
		t.log.Error("Failed to get position by trading symbol", map[string]interface{}{
			"error":          err.Error(),
			"trading_symbol": tradingSymbol,
		})
		return nil, err
	}

	// Parse KiteResponse JSON if it's not nil
	if kiteRespBytes != nil {
		var kiteResp interface{}
		if err := json.Unmarshal(kiteRespBytes, &kiteResp); err != nil {
			t.log.Error("Failed to unmarshal position KiteResponse", map[string]interface{}{"error": err.Error()})
		} else {
			position.KiteResponse = kiteResp
		}
	}

	return position, nil
}

// ListPositions fetches all position records from the positions table
func (t *TimescaleDB) ListPositions(ctx context.Context) ([]*PositionRecord, error) {
	query := `SELECT id, position_id, trading_symbol, exchange, product, quantity, average_price, last_price, pnl, realized_pnl, unrealized_pnl, multiplier, buy_quantity, sell_quantity, buy_price, sell_price, buy_value, sell_value, position_type, strategy_id, user_id, updated_at, paper_trading, kite_response FROM positions ORDER BY updated_at DESC`
	rows, err := t.pool.Query(ctx, query)
	if err != nil {
		t.log.Error("Failed to list positions", map[string]interface{}{"error": err.Error()})
		return nil, err
	}
	defer rows.Close()

	var positions []*PositionRecord
	for rows.Next() {
		position := &PositionRecord{}
		var kiteRespBytes []byte
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
			&position.BuyPrice,
			&position.SellPrice,
			&position.BuyValue,
			&position.SellValue,
			&position.PositionType,
			&position.StrategyID,
			&position.UserID,
			&position.UpdatedAt,
			&position.PaperTrading,
			&kiteRespBytes,
		)
		if err != nil {
			t.log.Error("Failed to scan position record", map[string]interface{}{"error": err.Error()})
			return nil, err
		}

		// Parse KiteResponse JSON if it's not nil
		if kiteRespBytes != nil {
			var kiteResp interface{}
			if err := json.Unmarshal(kiteRespBytes, &kiteResp); err != nil {
				t.log.Error("Failed to unmarshal position KiteResponse", map[string]interface{}{"error": err.Error()})
			} else {
				position.KiteResponse = kiteResp
			}
		}

		positions = append(positions, position)
	}

	return positions, nil
}

// DeletePosition deletes a position record from the positions table
func (t *TimescaleDB) DeletePosition(ctx context.Context, id int64) error {
	query := `DELETE FROM positions WHERE id = $1`
	_, err := t.pool.Exec(ctx, query, id)
	if err != nil {
		t.log.Error("Failed to delete position", map[string]interface{}{"error": err.Error(), "id": id})
		return err
	}
	return nil
}

// UpdateOrderStatus updates the status of an order in the database
func (t *TimescaleDB) UpdateOrderStatus(ctx context.Context, orderID string, status string, filledQuantity int) error {
	query := `UPDATE orders SET status = $1, quantity = $2 WHERE order_id = $3`
	_, err := t.pool.Exec(ctx, query, status, filledQuantity, orderID)
	if err != nil {
		t.log.Error("Failed to update order status", map[string]interface{}{"error": err.Error(), "order_id": orderID})
		return err
	}
	return nil
}
