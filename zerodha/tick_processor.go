package zerodha

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gohustle/logger"
	proto "gohustle/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	googleproto "google.golang.org/protobuf/proto"
)

type InstrumentInfo struct {
	Token     string
	Index     string
	Expiry    time.Time
	IsIndex   bool
	IsOptions bool
}

type TickProcessor struct {
	pool             *pgxpool.Pool
	instrumentLookup map[string]*InstrumentInfo
	baseDir          string
}

func NewTickProcessor(pool *pgxpool.Pool, instruments map[string]*InstrumentInfo) *TickProcessor {
	return &TickProcessor{
		pool:             pool,
		instrumentLookup: instruments,
		baseDir:          "data/ticks",
	}
}

func (p *TickProcessor) ProcessTick(ctx context.Context, tick *proto.TickData) error {
	// Save to TimeScale
	if err := p.saveToTimeScale(tick); err != nil {
		return fmt.Errorf("failed to save to TimeScale: %w", err)
	}

	// Save to file
	if err := p.saveToFile(tick); err != nil {
		return fmt.Errorf("failed to save to file: %w", err)
	}

	return nil
}

func (p *TickProcessor) saveToTimeScale(tick *proto.TickData) error {
	// Use pgx COPY protocol for efficient insert
	_, err := p.pool.CopyFrom(
		context.Background(),
		pgx.Identifier{"ticks"},
		[]string{
			"instrument_token",
			"timestamp",
			"last_trade_time",
			"last_price",
			"last_traded_quantity",
			"total_buy_quantity",
			"total_sell_quantity",
			"volume_traded",
			"total_buy",
			"total_sell",
			"average_trade_price",
			"oi",
			"oi_day_high",
			"oi_day_low",
			"net_change",
		},
		pgx.CopyFromSlice(1, func(i int) ([]interface{}, error) {
			return []interface{}{
				tick.InstrumentToken,
				time.Unix(tick.Timestamp, 0),
				time.Unix(tick.LastTradeTime, 0),
				tick.LastPrice,
				tick.LastTradedQuantity,
				tick.TotalBuyQuantity,
				tick.TotalSellQuantity,
				tick.VolumeTraded,
				tick.TotalBuy,
				tick.TotalSell,
				tick.AverageTradePrice,
				tick.Oi,
				tick.OiDayHigh,
				tick.OiDayLow,
				tick.NetChange,
			}, nil
		}),
	)
	return err
}

func (p *TickProcessor) saveToFile(tick *proto.TickData) error {
	filename := p.generateFileName(tick)

	// Create directory if not exists
	dirPath := filepath.Dir(filename)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Read existing file if it exists
	var existingBatch proto.TickBatch
	if data, err := os.ReadFile(filename); err == nil {
		if err := googleproto.Unmarshal(data, &existingBatch); err == nil {
			existingBatch.Ticks = append(existingBatch.Ticks, tick)
		}
	} else {
		existingBatch.Ticks = []*proto.TickData{tick}
	}

	// Sort ticks by timestamp
	sort.Slice(existingBatch.Ticks, func(i, j int) bool {
		return existingBatch.Ticks[i].Timestamp < existingBatch.Ticks[j].Timestamp
	})

	// Update metadata
	existingBatch.Metadata = &proto.BatchMetadata{
		Timestamp:  time.Now().Unix(),
		BatchSize:  int32(len(existingBatch.Ticks)),
		RetryCount: 0,
	}

	// Marshal and write
	data, err := googleproto.Marshal(&existingBatch)
	if err != nil {
		return fmt.Errorf("failed to marshal batch: %v", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %v", filename, err)
	}

	logger.GetLogger().Info("Wrote tick to file", map[string]interface{}{
		"filename":   filename,
		"batch_size": len(existingBatch.Ticks),
		"token":      tick.InstrumentToken,
		"timestamp":  time.Unix(tick.Timestamp, 0).Format("2006-01-02 15:04:05"),
	})

	return nil
}

func (p *TickProcessor) generateFileName(tick *proto.TickData) string {
	tickTime := time.Unix(tick.Timestamp, 0)

	// Get instrument info
	tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
	tokenInfo, exists := p.instrumentLookup[tokenStr]
	if !exists {
		// Default to NIFTY if not found
		return fmt.Sprintf("data/ticks/NIFTY_%02d%02d%d.pb",
			tickTime.Day(), tickTime.Month(), tickTime.Year())
	}

	if tokenInfo.IsIndex {
		return fmt.Sprintf("data/ticks/%s_%02d%02d%d.pb",
			tokenInfo.Index,
			tickTime.Day(), tickTime.Month(), tickTime.Year())
	}

	return fmt.Sprintf("data/ticks/%s_%s_%02d%02d%d.pb",
		tokenInfo.Index,
		tokenInfo.Expiry.Format("20060102"),
		tickTime.Day(), tickTime.Month(), tickTime.Year())
}

// Close cleans up resources
func (p *TickProcessor) Close() {
	if p.pool != nil {
		p.pool.Close()
	}
}
