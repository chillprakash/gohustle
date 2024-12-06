package consumer

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/queue"
	"gohustle/zerodha"
)

func StartTickConsumer(cfg *config.Config, asynqQueue *queue.AsynqQueue, kite *zerodha.KiteConnect) {
	log := logger.GetLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register consumer with ctx
	kite.ProcessTickTask(func(ctx context.Context, token uint32, tick *proto.TickData) error {
		log.Info("Processing tick", map[string]interface{}{
			"token":           tick.InstrumentToken,
			"is_tradable":     tick.IsTradable,
			"is_index":        tick.IsIndex,
			"mode":            tick.Mode,
			"timestamp":       time.Unix(tick.Timestamp, 0),
			"last_trade_time": time.Unix(tick.LastTradeTime, 0),
			"last_price":      tick.LastPrice,
			"last_traded_qty": tick.LastTradedQuantity,
			"total_buy_qty":   tick.TotalBuyQuantity,
			"total_sell_qty":  tick.TotalSellQuantity,
			"volume":          tick.VolumeTraded,
			"total_buy":       tick.TotalBuy,
			"total_sell":      tick.TotalSell,
			"avg_trade_price": tick.AverageTradePrice,
			"oi":              tick.Oi,
			"oi_day_high":     tick.OiDayHigh,
			"oi_day_low":      tick.OiDayLow,
			"net_change":      tick.NetChange,
			"ohlc":            tick.Ohlc,
		})
		return nil
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigChan:
		log.Info("Shutting down consumer...", nil)
	case <-ctx.Done():
		log.Info("Context cancelled...", nil)
	}
}
