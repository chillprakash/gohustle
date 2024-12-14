package filestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gohustle/logger"
	"gohustle/proto"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	defaultNumWorkers = 10
	defaultBufferSize = 1000
	defaultQueueSize  = 10000
	ticksDir          = "data/ticks" // Base directory for tick data
)

type WriteRequest struct {
	TargetFile string
	Tick       *proto.TickData
	ResultChan chan error
}

type WriterWorker struct {
	id         int
	workChan   chan *WriteRequest
	buffer     map[string][]*proto.TickData
	bufferSize int
	ctx        context.Context
	log        *logger.Logger
	baseDir    string // Base directory for writing files
}

type WriterPool struct {
	workers    []*WriterWorker
	workChan   chan *WriteRequest
	numWorkers int
	bufferSize int
	ctx        context.Context
	cancel     context.CancelFunc
	log        *logger.Logger
	wg         sync.WaitGroup // Added for worker synchronization
}

// Start starts all workers
func (p *WriterPool) Start() {
	for _, worker := range p.workers {
		p.wg.Add(1)
		go p.runWorker(worker)
	}
}

// runWorker runs the main worker loop
func (p *WriterPool) runWorker(w *WriterWorker) {
	defer p.wg.Done()

	// Add periodic flush
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			w.flushAll() // Flush before exit
			return
		case <-ticker.C:
			// Periodic flush
			for targetFile := range w.buffer {
				if len(w.buffer[targetFile]) > 0 {
					w.log.Info("Periodic flush triggered", map[string]interface{}{
						"worker_id":   w.id,
						"target_file": targetFile,
						"buffer_size": len(w.buffer[targetFile]),
					})
					if err := w.flushFile(targetFile); err != nil {
						w.log.Error("Failed periodic flush", map[string]interface{}{
							"error":       err.Error(),
							"worker_id":   w.id,
							"target_file": targetFile,
						})
					}
				}
			}
		case req := <-w.workChan:
			w.processRequest(req)
		}
	}
}

func (w *WriterWorker) processRequest(req *WriteRequest) {
	// Add single tick to appropriate buffer
	w.buffer[req.TargetFile] = append(w.buffer[req.TargetFile], req.Tick)

	w.log.Debug("Added tick to buffer", map[string]interface{}{
		"worker_id":    w.id,
		"target_file":  req.TargetFile,
		"buffer_size":  len(w.buffer[req.TargetFile]),
		"buffer_limit": w.bufferSize,
	})

	// Check buffer size
	if len(w.buffer[req.TargetFile]) >= w.bufferSize {
		w.log.Info("Buffer full, flushing", map[string]interface{}{
			"worker_id":   w.id,
			"target_file": req.TargetFile,
			"buffer_size": len(w.buffer[req.TargetFile]),
		})

		if err := w.flushFile(req.TargetFile); err != nil {
			w.log.Error("Failed to flush file", map[string]interface{}{
				"error":       err.Error(),
				"worker_id":   w.id,
				"target_file": req.TargetFile,
			})
			req.ResultChan <- err
			return
		}
	}

	// Send success response
	req.ResultChan <- nil
}

func (w *WriterWorker) flushFile(targetFile string) error {
	ticks := w.buffer[targetFile]
	if len(ticks) == 0 {
		return nil
	}

	fullPath := filepath.Join(w.baseDir, targetFile)

	w.log.Info("Preparing to write ticks", map[string]interface{}{
		"worker_id":   w.id,
		"file":        targetFile,
		"ticks_count": len(ticks),
		"full_path":   fullPath,
		"buffer_size": len(w.buffer[targetFile]),
	})

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		w.log.Error("Failed to create directory", map[string]interface{}{
			"error": err.Error(),
			"dir":   dir,
		})
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to parquet file
	if err := writeTicksToParquet(fullPath, ticks); err != nil {
		return err
	}

	// Clear buffer after successful write
	w.buffer[targetFile] = w.buffer[targetFile][:0]
	return nil
}

func (w *WriterWorker) flushAll() {
	for targetFile := range w.buffer {
		if err := w.flushFile(targetFile); err != nil {
			w.log.Error("Failed to flush file", map[string]interface{}{
				"error":       err.Error(),
				"target_file": targetFile,
				"worker_id":   w.id,
			})
		}
	}
}

func NewWriterPool() *WriterPool {
	ctx, cancel := context.WithCancel(context.Background())

	// Using hardcoded values
	numWorkers := defaultNumWorkers // 10 workers
	bufferSize := defaultBufferSize // 1000 ticks per buffer

	// Get project root directory and create data directory
	rootDir, err := os.Getwd()
	if err != nil {
		panic(fmt.Sprintf("failed to get working directory: %v", err))
	}

	// Construct base directory for ticks
	baseDir := filepath.Join(rootDir, ticksDir)

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		panic(fmt.Sprintf("failed to create ticks directory: %v", err))
	}

	log := logger.GetLogger()
	log.Info("Initialized writer pool", map[string]interface{}{
		"root_dir":    rootDir,
		"base_dir":    baseDir,
		"num_workers": numWorkers,
		"buffer_size": bufferSize,
		"queue_size":  defaultQueueSize,
	})

	pool := &WriterPool{
		workers:    make([]*WriterWorker, numWorkers),
		workChan:   make(chan *WriteRequest, defaultQueueSize),
		numWorkers: numWorkers,
		bufferSize: bufferSize,
		ctx:        ctx,
		cancel:     cancel,
		log:        logger.GetLogger(),
	}

	// Initialize workers with base directory
	for i := 0; i < numWorkers; i++ {
		pool.workers[i] = &WriterWorker{
			id:         i,
			workChan:   pool.workChan,
			buffer:     make(map[string][]*proto.TickData),
			bufferSize: bufferSize,
			ctx:        ctx,
			log:        logger.GetLogger(),
			baseDir:    baseDir,
		}
	}

	return pool
}

func (p *WriterPool) Write(req *WriteRequest) error {
	select {
	case p.workChan <- req:
		return nil
	case <-p.ctx.Done():
		return fmt.Errorf("writer pool is shutting down")
	}
}

func (p *WriterPool) Stop() {
	p.cancel()
	p.wg.Wait()
}

// Define a type for our schema
type TickParquetSchema struct {
	// Basic info
	InstrumentToken int64  `parquet:"name=instrument_token, type=INT64"`
	IsTradable      bool   `parquet:"name=is_tradable, type=BOOLEAN"`
	IsIndex         bool   `parquet:"name=is_index, type=BOOLEAN"`
	Mode            string `parquet:"name=mode, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Timestamps
	Timestamp     int64 `parquet:"name=timestamp, type=INT64"`
	LastTradeTime int64 `parquet:"name=last_trade_time, type=INT64"`

	// Price and quantity
	LastPrice          float64 `parquet:"name=last_price, type=DOUBLE"`
	LastTradedQuantity int64   `parquet:"name=last_traded_quantity, type=INT64"`
	TotalBuyQuantity   int64   `parquet:"name=total_buy_quantity, type=INT64"`
	TotalSellQuantity  int64   `parquet:"name=total_sell_quantity, type=INT64"`
	VolumeTraded       int64   `parquet:"name=volume_traded, type=INT64"`
	TotalBuy           int64   `parquet:"name=total_buy, type=INT64"`
	TotalSell          int64   `parquet:"name=total_sell, type=INT64"`
	AverageTradePrice  float64 `parquet:"name=average_trade_price, type=DOUBLE"`

	// OI related
	Oi        int64   `parquet:"name=oi, type=INT64"`
	OiDayHigh int64   `parquet:"name=oi_day_high, type=INT64"`
	OiDayLow  int64   `parquet:"name=oi_day_low, type=INT64"`
	NetChange float64 `parquet:"name=net_change, type=DOUBLE"`

	// OHLC data
	OhlcOpen  float64 `parquet:"name=ohlc_open, type=DOUBLE"`
	OhlcHigh  float64 `parquet:"name=ohlc_high, type=DOUBLE"`
	OhlcLow   float64 `parquet:"name=ohlc_low, type=DOUBLE"`
	OhlcClose float64 `parquet:"name=ohlc_close, type=DOUBLE"`

	// Market depth - Buy
	DepthBuyPrice1  float64 `parquet:"name=depth_buy_price_1, type=DOUBLE"`
	DepthBuyQty1    int64   `parquet:"name=depth_buy_qty_1, type=INT64"`
	DepthBuyOrders1 int64   `parquet:"name=depth_buy_orders_1, type=INT64"`
	// ... repeat for all 5 depth levels ...
	DepthBuyPrice5  float64 `parquet:"name=depth_buy_price_5, type=DOUBLE"`
	DepthBuyQty5    int64   `parquet:"name=depth_buy_qty_5, type=INT64"`
	DepthBuyOrders5 int64   `parquet:"name=depth_buy_orders_5, type=INT64"`

	// Market depth - Sell
	DepthSellPrice1  float64 `parquet:"name=depth_sell_price_1, type=DOUBLE"`
	DepthSellQty1    int64   `parquet:"name=depth_sell_qty_1, type=INT64"`
	DepthSellOrders1 int64   `parquet:"name=depth_sell_orders_1, type=INT64"`
	// ... repeat for all 5 depth levels ...
	DepthSellPrice5  float64 `parquet:"name=depth_sell_price_5, type=DOUBLE"`
	DepthSellQty5    int64   `parquet:"name=depth_sell_qty_5, type=INT64"`
	DepthSellOrders5 int64   `parquet:"name=depth_sell_orders_5, type=INT64"`

	// Additional metadata
	ChangePercent       float64 `parquet:"name=change_percent, type=DOUBLE"`
	LastTradePrice      float64 `parquet:"name=last_trade_price, type=DOUBLE"`
	OpenInterest        int64   `parquet:"name=open_interest, type=INT64"`
	OpenInterestDayHigh int64   `parquet:"name=open_interest_day_high, type=INT64"`
	OpenInterestDayLow  int64   `parquet:"name=open_interest_day_low, type=INT64"`
	TargetFile          string  `parquet:"name=target_file, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// Helper function to write ticks to parquet file
func writeTicksToParquet(filePath string, ticks []*proto.TickData) error {
	log := logger.GetLogger()

	log.Info("Starting parquet write", map[string]interface{}{
		"file_path":   filePath,
		"ticks_count": len(ticks),
	})

	// Create parquet file
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		log.Error("Failed to create file writer", map[string]interface{}{
			"error":     err.Error(),
			"file_path": filePath,
		})
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create parquet writer with schema
	pw, err := writer.NewParquetWriter(fw, new(TickParquetSchema), 4)
	if err != nil {
		log.Error("Failed to create parquet writer", map[string]interface{}{
			"error":     err.Error(),
			"file_path": filePath,
		})
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Important: Defer WriteStop AFTER error check
	defer pw.WriteStop()

	// Set compression and row group size
	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write ticks
	for i, tick := range ticks {
		// Convert tick to schema with explicit type conversion
		row := &TickParquetSchema{
			InstrumentToken:     int64(tick.InstrumentToken),
			IsTradable:          tick.IsTradable,
			IsIndex:             tick.IsIndex,
			Mode:                tick.Mode,
			Timestamp:           tick.Timestamp,
			LastTradeTime:       tick.LastTradeTime,
			LastPrice:           tick.LastPrice,
			LastTradedQuantity:  int64(tick.LastTradedQuantity),
			TotalBuyQuantity:    int64(tick.TotalBuyQuantity),
			TotalSellQuantity:   int64(tick.TotalSellQuantity),
			VolumeTraded:        int64(tick.VolumeTraded),
			TotalBuy:            int64(tick.TotalBuy),
			TotalSell:           int64(tick.TotalSell),
			AverageTradePrice:   tick.AverageTradePrice,
			Oi:                  int64(tick.Oi),
			OiDayHigh:           int64(tick.OiDayHigh),
			OiDayLow:            int64(tick.OiDayLow),
			NetChange:           tick.NetChange,
			OhlcOpen:            tick.Ohlc.Open,
			OhlcHigh:            tick.Ohlc.High,
			OhlcLow:             tick.Ohlc.Low,
			OhlcClose:           tick.Ohlc.Close,
			DepthBuyPrice1:      tick.Depth.Buy[0].Price,
			DepthBuyQty1:        int64(tick.Depth.Buy[0].Quantity),
			DepthBuyOrders1:     int64(tick.Depth.Buy[0].Orders),
			DepthBuyPrice5:      tick.Depth.Buy[4].Price,
			DepthBuyQty5:        int64(tick.Depth.Buy[4].Quantity),
			DepthBuyOrders5:     int64(tick.Depth.Buy[4].Orders),
			DepthSellPrice1:     tick.Depth.Sell[0].Price,
			DepthSellQty1:       int64(tick.Depth.Sell[0].Quantity),
			DepthSellOrders1:    int64(tick.Depth.Sell[0].Orders),
			DepthSellPrice5:     tick.Depth.Sell[4].Price,
			DepthSellQty5:       int64(tick.Depth.Sell[4].Quantity),
			DepthSellOrders5:    int64(tick.Depth.Sell[4].Orders),
			ChangePercent:       tick.ChangePercent,
			LastTradePrice:      tick.LastTradePrice,
			OpenInterest:        int64(tick.OpenInterest),
			OpenInterestDayHigh: int64(tick.OpenInterestDayHigh),
			OpenInterestDayLow:  int64(tick.OpenInterestDayLow),
			TargetFile:          tick.TargetFile,
		}

		if err := pw.Write(row); err != nil {
			log.Error("Failed to write tick", map[string]interface{}{
				"error":      err.Error(),
				"file_path":  filePath,
				"tick_index": i,
			})
			return fmt.Errorf("failed to write tick: %w", err)
		}
	}

	// Explicitly call WriteStop before closing
	if err := pw.WriteStop(); err != nil {
		log.Error("Failed to finalize parquet file", map[string]interface{}{
			"error":     err.Error(),
			"file_path": filePath,
		})
		return fmt.Errorf("failed to finalize parquet file: %w", err)
	}

	log.Info("Successfully wrote parquet file", map[string]interface{}{
		"file_path":   filePath,
		"ticks_count": len(ticks),
		"file_size":   getFileSize(filePath),
	})

	return nil
}

// Helper to get file size
func getFileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// ... rest of the implementation (getOrCreateWriter, Flush methods etc.)
