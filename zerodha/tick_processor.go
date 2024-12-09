package zerodha

import (
	"context"
	"fmt"
	"gohustle/logger"
	"os"
	"sync"
	"time"

	"bufio"
	"encoding/binary"

	proto "gohustle/proto"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	googleproto "google.golang.org/protobuf/proto"
)

type TickProcessor struct {
	// Channels for buffering
	timescaleChan chan *proto.TickData
	protobufChan  chan *proto.TickData

	// Batch configurations
	batchConfig struct {
		// TimeScale batching
		DBBatchSize     int           // e.g., 1000 records
		DBFlushInterval time.Duration // e.g., 1 second

		// File batching
		FileBatchSize     int           // e.g., 5000 records
		FileFlushInterval time.Duration // e.g., 5 seconds
	}

	// Batch collectors
	dbBatch   []*proto.TickData
	fileBatch []*proto.TickData

	pool *pgxpool.Pool
}

func NewTickProcessor(pool *pgxpool.Pool) *TickProcessor {
	return &TickProcessor{
		pool:          pool,
		timescaleChan: make(chan *proto.TickData, 10000),
		protobufChan:  make(chan *proto.TickData, 10000),
		batchConfig: struct {
			DBBatchSize       int
			DBFlushInterval   time.Duration
			FileBatchSize     int
			FileFlushInterval time.Duration
		}{
			DBBatchSize:       1000,            // Write to DB every 1000 records
			DBFlushInterval:   time.Second,     // or every 1 second
			FileBatchSize:     5000,            // Write to file every 5000 records
			FileFlushInterval: 5 * time.Second, // or every 5 seconds
		},
	}
}

// TimeScale batch processor
func (p *KiteConnect) processTimeScaleBatch(ctx context.Context) {
	ticker := time.NewTicker(p.tickProcessor.batchConfig.DBFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush
			if len(p.tickProcessor.dbBatch) > 0 {
				p.tickProcessor.saveBatchToTimeScale(p.tickProcessor.dbBatch)
			}
			return

		case tick := <-p.tickProcessor.timescaleChan:
			p.tickProcessor.dbBatch = append(p.tickProcessor.dbBatch, tick)
			if len(p.tickProcessor.dbBatch) >= p.tickProcessor.batchConfig.DBBatchSize {
				p.tickProcessor.saveBatchToTimeScale(p.tickProcessor.dbBatch)
				p.tickProcessor.dbBatch = make([]*proto.TickData, 0, p.tickProcessor.batchConfig.DBBatchSize)
			}

		case <-ticker.C:
			if len(p.tickProcessor.dbBatch) > 0 {
				p.tickProcessor.saveBatchToTimeScale(p.tickProcessor.dbBatch)
				p.tickProcessor.dbBatch = make([]*proto.TickData, 0, p.tickProcessor.batchConfig.DBBatchSize)
			}
		}
	}
}

// Protobuf file processor
func (p *KiteConnect) processProtobufFile(ctx context.Context) {
	ticker := time.NewTicker(p.tickProcessor.batchConfig.FileFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Final flush
			if len(p.tickProcessor.fileBatch) > 0 {
				p.tickProcessor.appendBatchToProtobufFile(p.tickProcessor.fileBatch)
			}
			return

		case tick := <-p.tickProcessor.protobufChan:
			p.tickProcessor.fileBatch = append(p.tickProcessor.fileBatch, tick)
			if len(p.tickProcessor.fileBatch) >= p.tickProcessor.batchConfig.FileBatchSize {
				p.tickProcessor.appendBatchToProtobufFile(p.tickProcessor.fileBatch)
				p.tickProcessor.fileBatch = make([]*proto.TickData, 0, p.tickProcessor.batchConfig.FileBatchSize)
			}

		case <-ticker.C:
			if len(p.tickProcessor.fileBatch) > 0 {
				p.tickProcessor.appendBatchToProtobufFile(p.tickProcessor.fileBatch)
				p.tickProcessor.fileBatch = make([]*proto.TickData, 0, p.tickProcessor.batchConfig.FileBatchSize)
			}
		}
	}
}

func (p *TickProcessor) saveBatchToTimeScale(batch []*proto.TickData) error {
	if len(batch) == 0 {
		return nil
	}

	// Use pgx COPY protocol for efficient batch insert
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
		pgx.CopyFromSlice(len(batch), func(i int) ([]interface{}, error) {
			return []interface{}{
				batch[i].InstrumentToken,
				time.Unix(batch[i].Timestamp, 0),
				time.Unix(batch[i].LastTradeTime, 0),
				batch[i].LastPrice,
				batch[i].LastTradedQuantity,
				batch[i].TotalBuyQuantity,
				batch[i].TotalSellQuantity,
				batch[i].VolumeTraded,
				batch[i].TotalBuy,
				batch[i].TotalSell,
				batch[i].AverageTradePrice,
				batch[i].Oi,
				batch[i].OiDayHigh,
				batch[i].OiDayLow,
				batch[i].NetChange,
			}, nil
		}),
	)
	return err
}

func (p *TickProcessor) appendBatchToProtobufFile(batch []*proto.TickData) error {
	if len(batch) == 0 {
		return nil
	}

	// Create directory if not exists
	date := time.Now().Format("2006-01-02")
	dirPath := fmt.Sprintf("data/ticks/%s", date)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}

	// Open file in append mode
	filename := fmt.Sprintf("%s/ticks.pb", dirPath)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Use buffered writer for efficiency
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write each message with length prefix
	for _, tick := range batch {
		data, err := googleproto.Marshal(tick)
		if err != nil {
			continue
		}

		// Write length prefix (4 bytes) + data
		length := uint32(len(data))
		binary.Write(writer, binary.LittleEndian, length)
		writer.Write(data)
	}

	return nil
}

// StartProcessing starts both TimeScale and Protobuf processors
func (p *KiteConnect) StartProcessing(ctx context.Context) error {
	log := logger.GetLogger()

	// Create a wait group for graceful shutdown
	var wg sync.WaitGroup
	wg.Add(2)

	// Start TimeScale processor
	go func() {
		defer wg.Done()
		log.Info("Starting TimeScale processor")
		p.processTimeScaleBatch(ctx)
	}()

	// Start Protobuf processor
	go func() {
		defer wg.Done()
		log.Info("Starting Protobuf processor")
		p.processProtobufFile(ctx)
	}()

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		log.Info("Shutting down processors")
		wg.Wait()
		log.Info("All processors stopped")
	}()

	return nil
}

// Add cleanup methods to TickProcessor
func (p *TickProcessor) FlushTimeScale(ctx context.Context) {
	if len(p.dbBatch) > 0 {
		p.saveBatchToTimeScale(p.dbBatch)
	}
}

func (p *TickProcessor) FlushProtobuf(ctx context.Context) {
	if len(p.fileBatch) > 0 {
		p.appendBatchToProtobufFile(p.fileBatch)
	}
}

// Close cleans up resources used by the TickProcessor
func (t *TickProcessor) Close() {
	// Flush any remaining batches
	if len(t.dbBatch) > 0 {
		t.saveBatchToTimeScale(t.dbBatch)
	}
	if len(t.fileBatch) > 0 {
		t.appendBatchToProtobufFile(t.fileBatch)
	}

	// Close channels
	close(t.timescaleChan)
	close(t.protobufChan)
}

func (t *TickProcessor) ProcessTick(tick *proto.TickData) {
	log := logger.GetLogger()
	log.Info("Processing tick", map[string]interface{}{
		"token":     tick.InstrumentToken,
		"timestamp": time.Unix(tick.Timestamp, 0),
		"lastPrice": tick.LastPrice,
		"volume":    tick.VolumeTraded,
		"oi":        tick.Oi,
	})

	// Add to TimeScale batch
	t.timescaleChan <- tick
	// Add to Protobuf batch
	t.protobufChan <- tick
}
