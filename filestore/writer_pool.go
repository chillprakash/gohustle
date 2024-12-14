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
	"github.com/xitongsys/parquet-go/reader"
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
	InstrumentToken int64  `parquet:"name=instrument_token, type=INT64, repetitiontype=REQUIRED"`
	IsTradable      bool   `parquet:"name=is_tradable, type=BOOLEAN, repetitiontype=REQUIRED"`
	IsIndex         bool   `parquet:"name=is_index, type=BOOLEAN, repetitiontype=REQUIRED"`
	Mode            string `parquet:"name=mode, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"`

	// Timestamps
	Timestamp     int64 `parquet:"name=timestamp, type=INT64, repetitiontype=REQUIRED"`
	LastTradeTime int64 `parquet:"name=last_trade_time, type=INT64, repetitiontype=REQUIRED"`

	// Price and quantity
	LastPrice          float64 `parquet:"name=last_price, type=DOUBLE, repetitiontype=REQUIRED"`
	LastTradedQuantity int64   `parquet:"name=last_traded_quantity, type=INT64, repetitiontype=REQUIRED"`
	TotalBuyQuantity   int64   `parquet:"name=total_buy_quantity, type=INT64, repetitiontype=REQUIRED"`
	TotalSellQuantity  int64   `parquet:"name=total_sell_quantity, type=INT64, repetitiontype=REQUIRED"`
	VolumeTraded       int64   `parquet:"name=volume_traded, type=INT64, repetitiontype=REQUIRED"`
	TotalBuy           int64   `parquet:"name=total_buy, type=INT64, repetitiontype=REQUIRED"`
	TotalSell          int64   `parquet:"name=total_sell, type=INT64, repetitiontype=REQUIRED"`
	AverageTradePrice  float64 `parquet:"name=average_trade_price, type=DOUBLE, repetitiontype=REQUIRED"`

	// OI related
	Oi        int64   `parquet:"name=oi, type=INT64, repetitiontype=REQUIRED"`
	OiDayHigh int64   `parquet:"name=oi_day_high, type=INT64, repetitiontype=REQUIRED"`
	OiDayLow  int64   `parquet:"name=oi_day_low, type=INT64, repetitiontype=REQUIRED"`
	NetChange float64 `parquet:"name=net_change, type=DOUBLE, repetitiontype=REQUIRED"`

	// OHLC data
	OhlcOpen  float64 `parquet:"name=ohlc_open, type=DOUBLE, repetitiontype=REQUIRED"`
	OhlcHigh  float64 `parquet:"name=ohlc_high, type=DOUBLE, repetitiontype=REQUIRED"`
	OhlcLow   float64 `parquet:"name=ohlc_low, type=DOUBLE, repetitiontype=REQUIRED"`
	OhlcClose float64 `parquet:"name=ohlc_close, type=DOUBLE, repetitiontype=REQUIRED"`

	// Market depth - Buy
	DepthBuyPrice1  float64 `parquet:"name=depth_buy_price_1, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthBuyQty1    int64   `parquet:"name=depth_buy_qty_1, type=INT64, repetitiontype=REQUIRED"`
	DepthBuyOrders1 int64   `parquet:"name=depth_buy_orders_1, type=INT64, repetitiontype=REQUIRED"`

	DepthBuyPrice2  float64 `parquet:"name=depth_buy_price_2, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthBuyQty2    int64   `parquet:"name=depth_buy_qty_2, type=INT64, repetitiontype=REQUIRED"`
	DepthBuyOrders2 int64   `parquet:"name=depth_buy_orders_2, type=INT64, repetitiontype=REQUIRED"`

	DepthBuyPrice3  float64 `parquet:"name=depth_buy_price_3, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthBuyQty3    int64   `parquet:"name=depth_buy_qty_3, type=INT64, repetitiontype=REQUIRED"`
	DepthBuyOrders3 int64   `parquet:"name=depth_buy_orders_3, type=INT64, repetitiontype=REQUIRED"`

	DepthBuyPrice4  float64 `parquet:"name=depth_buy_price_4, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthBuyQty4    int64   `parquet:"name=depth_buy_qty_4, type=INT64, repetitiontype=REQUIRED"`
	DepthBuyOrders4 int64   `parquet:"name=depth_buy_orders_4, type=INT64, repetitiontype=REQUIRED"`

	DepthBuyPrice5  float64 `parquet:"name=depth_buy_price_5, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthBuyQty5    int64   `parquet:"name=depth_buy_qty_5, type=INT64, repetitiontype=REQUIRED"`
	DepthBuyOrders5 int64   `parquet:"name=depth_buy_orders_5, type=INT64, repetitiontype=REQUIRED"`

	// Market depth - Sell
	DepthSellPrice1  float64 `parquet:"name=depth_sell_price_1, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthSellQty1    int64   `parquet:"name=depth_sell_qty_1, type=INT64, repetitiontype=REQUIRED"`
	DepthSellOrders1 int64   `parquet:"name=depth_sell_orders_1, type=INT64, repetitiontype=REQUIRED"`

	DepthSellPrice2  float64 `parquet:"name=depth_sell_price_2, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthSellQty2    int64   `parquet:"name=depth_sell_qty_2, type=INT64, repetitiontype=REQUIRED"`
	DepthSellOrders2 int64   `parquet:"name=depth_sell_orders_2, type=INT64, repetitiontype=REQUIRED"`

	DepthSellPrice3  float64 `parquet:"name=depth_sell_price_3, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthSellQty3    int64   `parquet:"name=depth_sell_qty_3, type=INT64, repetitiontype=REQUIRED"`
	DepthSellOrders3 int64   `parquet:"name=depth_sell_orders_3, type=INT64, repetitiontype=REQUIRED"`

	DepthSellPrice4  float64 `parquet:"name=depth_sell_price_4, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthSellQty4    int64   `parquet:"name=depth_sell_qty_4, type=INT64, repetitiontype=REQUIRED"`
	DepthSellOrders4 int64   `parquet:"name=depth_sell_orders_4, type=INT64, repetitiontype=REQUIRED"`

	DepthSellPrice5  float64 `parquet:"name=depth_sell_price_5, type=DOUBLE, repetitiontype=REQUIRED"`
	DepthSellQty5    int64   `parquet:"name=depth_sell_qty_5, type=INT64, repetitiontype=REQUIRED"`
	DepthSellOrders5 int64   `parquet:"name=depth_sell_orders_5, type=INT64, repetitiontype=REQUIRED"`

	// Additional metadata
	ChangePercent       float64 `parquet:"name=change_percent, type=DOUBLE, repetitiontype=REQUIRED"`
	LastTradePrice      float64 `parquet:"name=last_trade_price, type=DOUBLE, repetitiontype=REQUIRED"`
	OpenInterest        int64   `parquet:"name=open_interest, type=INT64, repetitiontype=REQUIRED"`
	OpenInterestDayHigh int64   `parquet:"name=open_interest_day_high, type=INT64, repetitiontype=REQUIRED"`
	OpenInterestDayLow  int64   `parquet:"name=open_interest_day_low, type=INT64, repetitiontype=REQUIRED"`
	TargetFile          string  `parquet:"name=target_file, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED"`
}

// Helper function to write ticks to parquet file
func writeTicksToParquet(filePath string, ticks []*proto.TickData) error {
	log := logger.GetLogger()

	log.Info("Starting parquet write", map[string]interface{}{
		"file_path":   filePath,
		"ticks_count": len(ticks),
	})

	// Create new file
	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		log.Error("Failed to create file writer", map[string]interface{}{
			"error":     err.Error(),
			"file_path": filePath,
		})
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create parquet writer
	pw, err := writer.NewParquetWriter(fw, new(TickParquetSchema), 4)
	if err != nil {
		log.Error("Failed to create parquet writer", map[string]interface{}{
			"error":     err.Error(),
			"file_path": filePath,
		})
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer pw.WriteStop()

	// Set writer properties
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write ticks
	for i, tick := range ticks {
		row := convertTickToParquet(tick)
		if row == nil {
			log.Error("Skipping nil tick", map[string]interface{}{
				"index": i,
			})
			continue
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

	return nil
}

// Helper function to read existing parquet file
func readExistingParquet(filePath string) ([]*proto.TickData, error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file reader: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(TickParquetSchema), 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	ticks := make([]*proto.TickData, 0, num)

	// Read existing data
	for i := 0; i < num; i++ {
		row := new(TickParquetSchema)
		if err := pr.Read(row); err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		// Convert back to TickData
		tick := convertToTickData(row)
		ticks = append(ticks, tick)
	}

	return ticks, nil
}

// Helper function to convert schema back to TickData
func convertToTickData(row *TickParquetSchema) *proto.TickData {
	// Implement conversion from schema to TickData
	return &proto.TickData{
		// ... implement conversion ...
	}
}

// Helper to get file size
func getFileSize(filePath string) int64 {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0
	}
	return info.Size()
}

// Helper functions for default values
func getDefaultDepth() *proto.TickData_MarketDepth {
	return &proto.TickData_MarketDepth{
		Buy:  make([]*proto.TickData_DepthItem, 5),
		Sell: make([]*proto.TickData_DepthItem, 5),
	}
}

func getDefaultOHLC() *proto.TickData_OHLC {
	return &proto.TickData_OHLC{
		Open:  0.0,
		High:  0.0,
		Low:   0.0,
		Close: 0.0,
	}
}

// Helper to safely get depth item
func getDepthItem(items []*proto.TickData_DepthItem, index int) *proto.TickData_DepthItem {
	if items == nil || index >= len(items) {
		return &proto.TickData_DepthItem{
			Price:    0.0,
			Quantity: 0,
			Orders:   0,
		}
	}
	if items[index] == nil {
		items[index] = &proto.TickData_DepthItem{
			Price:    0.0,
			Quantity: 0,
			Orders:   0,
		}
	}
	return items[index]
}

// Convert tick to parquet schema with validation
func convertTickToParquet(tick *proto.TickData) *TickParquetSchema {
	if tick == nil {
		return nil
	}

	// Initialize OHLC if nil
	if tick.Ohlc == nil {
		tick.Ohlc = getDefaultOHLC()
	}

	// Initialize Depth if nil
	if tick.Depth == nil {
		tick.Depth = getDefaultDepth()
	}

	// Ensure Buy and Sell arrays exist
	if tick.Depth.Buy == nil {
		tick.Depth.Buy = make([]*proto.TickData_DepthItem, 5)
	}
	if tick.Depth.Sell == nil {
		tick.Depth.Sell = make([]*proto.TickData_DepthItem, 5)
	}

	// Create parquet row
	row := &TickParquetSchema{
		// Basic info
		InstrumentToken: int64(tick.InstrumentToken),
		IsTradable:      tick.IsTradable,
		IsIndex:         tick.IsIndex,
		Mode:            tick.Mode,

		// Timestamps
		Timestamp:     tick.Timestamp,
		LastTradeTime: tick.LastTradeTime,

		// Price and quantity
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: int64(tick.LastTradedQuantity),
		TotalBuyQuantity:   int64(tick.TotalBuyQuantity),
		TotalSellQuantity:  int64(tick.TotalSellQuantity),
		VolumeTraded:       int64(tick.VolumeTraded),
		TotalBuy:           int64(tick.TotalBuy),
		TotalSell:          int64(tick.TotalSell),
		AverageTradePrice:  tick.AverageTradePrice,

		// OHLC
		OhlcOpen:  tick.Ohlc.Open,
		OhlcHigh:  tick.Ohlc.High,
		OhlcLow:   tick.Ohlc.Low,
		OhlcClose: tick.Ohlc.Close,

		// Buy Depth
		DepthBuyPrice1:  getDepthItem(tick.Depth.Buy, 0).Price,
		DepthBuyQty1:    int64(getDepthItem(tick.Depth.Buy, 0).Quantity),
		DepthBuyOrders1: int64(getDepthItem(tick.Depth.Buy, 0).Orders),

		DepthBuyPrice2:  getDepthItem(tick.Depth.Buy, 1).Price,
		DepthBuyQty2:    int64(getDepthItem(tick.Depth.Buy, 1).Quantity),
		DepthBuyOrders2: int64(getDepthItem(tick.Depth.Buy, 1).Orders),

		// ... repeat for all depth levels ...

		// Additional metadata
		ChangePercent:       tick.ChangePercent,
		LastTradePrice:      tick.LastTradePrice,
		OpenInterest:        int64(tick.OpenInterest),
		OpenInterestDayHigh: int64(tick.OpenInterestDayHigh),
		OpenInterestDayLow:  int64(tick.OpenInterestDayLow),
		TargetFile:          tick.TargetFile,
	}

	return row
}

// ... rest of the implementation (getOrCreateWriter, Flush methods etc.)
