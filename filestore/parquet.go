package filestore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/jackc/pgx/v5/pgtype"

	"gohustle/logger"
)

const (
	DefaultParquetDir = "data/parquet"
)

var (
	parquetStoreInstance *ParquetStore
	parquetStoreOnce     sync.Once
	parquetStoreMu       sync.RWMutex
)

// ParquetStore manages daily Parquet files per index
type ParquetStore struct {
	writers    map[string]*ParquetWriter // map[indexName]writer
	writersMu  sync.RWMutex
	log        *logger.Logger
	currentDay string
}

// GetParquetStore returns the singleton instance of ParquetStore
func GetParquetStore() *ParquetStore {
	parquetStoreMu.RLock()
	if parquetStoreInstance != nil {
		parquetStoreMu.RUnlock()
		return parquetStoreInstance
	}
	parquetStoreMu.RUnlock()

	parquetStoreMu.Lock()
	defer parquetStoreMu.Unlock()

	parquetStoreOnce.Do(func() {
		parquetStoreInstance = &ParquetStore{
			writers:    make(map[string]*ParquetWriter),
			log:        logger.L(),
			currentDay: time.Now().Format("2006-01-02"),
		}
	})

	return parquetStoreInstance
}

// getFilePath returns the path for a given index and date
func (ps *ParquetStore) getFilePath(index string) string {
	return filepath.Join(DefaultParquetDir, fmt.Sprintf("%s_%s.parquet", index, ps.currentDay))
}

// rotateFile handles the file rotation when day changes
func (ps *ParquetStore) rotateFile(index string, currentDay string) error {
	// Close existing writer if day changed
	if writer, exists := ps.writers[index]; exists {
		if err := writer.Close(); err != nil {
			ps.log.Error("Failed to close writer during rotation", map[string]interface{}{
				"error": err.Error(),
				"index": index,
			})
		}
		delete(ps.writers, index)
	}

	// Create new writer
	filePath := ps.getFilePath(index)
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Check if file exists
	if _, err := os.Stat(filePath); err == nil {
		// File exists, create a backup with timestamp
		backupPath := filePath + fmt.Sprintf(".%s.bak", time.Now().Format("150405"))
		if err := os.Rename(filePath, backupPath); err != nil {
			return fmt.Errorf("failed to create backup of existing file: %w", err)
		}
		ps.log.Info("Created backup of existing file", map[string]interface{}{
			"original": filePath,
			"backup":   backupPath,
		})
	}

	writer, err := NewParquetWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}

	ps.writers[index] = writer
	ps.currentDay = currentDay

	return nil
}

// GetOrCreateWriter gets existing writer or creates new one for the index
func (ps *ParquetStore) GetOrCreateWriter(index string) (*ParquetWriter, error) {
	currentDay := time.Now().Format("2006-01-02")

	ps.writersMu.RLock()
	writer, exists := ps.writers[index]
	if exists && ps.currentDay == currentDay {
		ps.writersMu.RUnlock()
		return writer, nil
	}
	ps.writersMu.RUnlock()

	// Need to create or rotate writer
	ps.writersMu.Lock()
	defer ps.writersMu.Unlock()

	// Double check after acquiring write lock
	if writer, exists := ps.writers[index]; exists && ps.currentDay == currentDay {
		return writer, nil
	}

	// Rotate file if day changed
	if err := ps.rotateFile(index, currentDay); err != nil {
		return nil, fmt.Errorf("failed to rotate file: %w", err)
	}

	return ps.writers[index], nil
}

// WriteTick writes a single tick to the appropriate daily file
func (ps *ParquetStore) WriteTick(index string, record TickRecord) error {
	writer, err := ps.GetOrCreateWriter(index)
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}

	return writer.Write(record)
}

// WriteBatch writes a batch of ticks to the appropriate daily file
func (ps *ParquetStore) WriteBatch(index string, records []TickRecord) error {
	if len(records) == 0 {
		return nil
	}

	writer, err := ps.GetOrCreateWriter(index)
	if err != nil {
		return fmt.Errorf("failed to get writer: %w", err)
	}

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

// Close closes all open writers
func (ps *ParquetStore) Close() error {
	ps.writersMu.Lock()
	defer ps.writersMu.Unlock()

	var lastErr error
	for index, writer := range ps.writers {
		if err := writer.Close(); err != nil {
			ps.log.Error("Failed to close writer", map[string]interface{}{
				"error": err.Error(),
				"index": index,
			})
			lastErr = err
		}
		delete(ps.writers, index)
	}

	return lastErr
}

// WriterStats holds statistics about the parquet writing process
type WriterStats struct {
	RecordCount      int64   // Number of records written
	UncompressedSize int64   // Estimated size of data before compression
	CompressedSize   int64   // Actual size of data after compression
	CompressionRatio float64 // Ratio of uncompressed to compressed size
}

// TickRecord represents a single tick record for parquet writing
type TickRecord struct {
	ID              int64
	InstrumentToken int64
	Timestamp       pgtype.Timestamp
	IsTradable      bool
	IsIndex         bool
	Mode            string

	// Price information
	LastPrice          float64
	LastTradedQuantity int32
	AverageTradePrice  float64
	VolumeTraded       int32
	TotalBuyQuantity   int32
	TotalSellQuantity  int32
	TotalBuy           int32
	TotalSell          int32

	// OHLC
	OhlcOpen  float64
	OhlcHigh  float64
	OhlcLow   float64
	OhlcClose float64

	// Market Depth - Buy
	DepthBuyPrice1    float64
	DepthBuyQuantity1 int32
	DepthBuyOrders1   int32
	DepthBuyPrice2    float64
	DepthBuyQuantity2 int32
	DepthBuyOrders2   int32
	DepthBuyPrice3    float64
	DepthBuyQuantity3 int32
	DepthBuyOrders3   int32
	DepthBuyPrice4    float64
	DepthBuyQuantity4 int32
	DepthBuyOrders4   int32
	DepthBuyPrice5    float64
	DepthBuyQuantity5 int32
	DepthBuyOrders5   int32

	// Market Depth - Sell
	DepthSellPrice1    float64
	DepthSellQuantity1 int32
	DepthSellOrders1   int32
	DepthSellPrice2    float64
	DepthSellQuantity2 int32
	DepthSellOrders2   int32
	DepthSellPrice3    float64
	DepthSellQuantity3 int32
	DepthSellOrders3   int32
	DepthSellPrice4    float64
	DepthSellQuantity4 int32
	DepthSellOrders4   int32
	DepthSellPrice5    float64
	DepthSellQuantity5 int32
	DepthSellOrders5   int32

	// Additional fields
	LastTradeTime pgtype.Timestamp
	NetChange     float64

	// Metadata
	TickReceivedTime   pgtype.Timestamp
	TickStoredInDbTime pgtype.Timestamp
}

type ParquetWriter struct {
	file             *os.File
	writer           *pqarrow.FileWriter
	recordBldr       *array.RecordBuilder
	recordCount      int64
	uncompressedSize int64
	closed           bool
	flushSize        int64
}

func NewParquetWriter(filePath string) (*ParquetWriter, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Open file in append mode or create if doesn't exist
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file info to check if it's new
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// If file exists and not empty, we need to handle existing data
	if fi.Size() > 0 {
		log.Printf("File %s exists with size %d bytes, data will be appended", filePath, fi.Size())
	}

	// Define metadata for SQL compatibility
	metadata := arrow.NewMetadata([]string{
		"writer.type",
		"writer.version",
		"table.name",
		"table.description",
		"created_at",
		"updated_at",
	}, []string{
		"gohustle",
		"1.0",
		"market_ticks",
		"Market tick data with OHLC and depth information",
		time.Now().Format(time.RFC3339),
		time.Now().Format(time.RFC3339),
	})

	// Define complete schema matching TimescaleDB
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "instrument_token", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "is_tradable", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "is_index", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
			{Name: "mode", Type: arrow.BinaryTypes.String, Nullable: false},
			{Name: "last_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "last_traded_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "average_trade_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "volume_traded", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "total_buy_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "total_sell_quantity", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "total_buy", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "total_sell", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "ohlc_open", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "ohlc_high", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "ohlc_low", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "ohlc_close", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_price_1", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_quantity_1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_orders_1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_price_2", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_quantity_2", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_orders_2", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_price_3", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_quantity_3", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_orders_3", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_price_4", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_quantity_4", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_orders_4", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_price_5", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_buy_quantity_5", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_buy_orders_5", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_price_1", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_sell_quantity_1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_orders_1", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_price_2", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_sell_quantity_2", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_orders_2", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_price_3", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_sell_quantity_3", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_orders_3", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_price_4", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_sell_quantity_4", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_orders_4", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_price_5", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "depth_sell_quantity_5", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "depth_sell_orders_5", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "last_trade_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
			{Name: "net_change", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "tick_received_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "tick_stored_in_db_time", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
		},
		&metadata,
	)

	// Create Arrow memory allocator
	pool := memory.NewGoAllocator()

	// Create writer properties optimized for SQL compatibility
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1024*1024),                // 1MB page size
		parquet.WithDictionaryPageSizeLimit(128*1024*1024), // 128MB dictionary size
		parquet.WithMaxRowGroupLength(128*1024*1024),       // 128MB row groups
		parquet.WithCreatedBy("GoHustle"),
		parquet.WithVersion(parquet.V2_LATEST),
	)

	// Create Arrow writer properties with SQL compatibility options
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithAllocator(pool),
	)

	// Create parquet writer
	writer, err := pqarrow.NewFileWriter(
		schema,
		f,
		writerProps,
		arrowProps,
	)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	return &ParquetWriter{
		file:             f,
		writer:           writer,
		recordBldr:       array.NewRecordBuilder(pool, schema),
		recordCount:      0,
		uncompressedSize: 0,
		closed:           false,
		flushSize:        10000,
	}, nil
}

func (w *ParquetWriter) Write(record TickRecord) error {
	if w.closed {
		return fmt.Errorf("writer is closed")
	}

	// Validate required fields
	if record.InstrumentToken == 0 {
		return fmt.Errorf("instrument token is required")
	}

	if !record.Timestamp.Valid {
		return fmt.Errorf("timestamp is required")
	}

	// Estimate uncompressed size (rough approximation)
	// Each numeric field ~8 bytes, string ~32 bytes, timestamp ~8 bytes
	w.uncompressedSize += 8*30 + 32*1 + 8*4 // 30 numeric fields, 1 string field, 4 timestamps

	// Build the record
	// Basic fields
	w.recordBldr.Field(0).(*array.Int64Builder).Append(record.ID)
	w.recordBldr.Field(1).(*array.Int64Builder).Append(record.InstrumentToken)
	w.recordBldr.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(record.Timestamp.Time.UnixMicro()))
	w.recordBldr.Field(3).(*array.BooleanBuilder).Append(record.IsTradable)
	w.recordBldr.Field(4).(*array.BooleanBuilder).Append(record.IsIndex)
	w.recordBldr.Field(5).(*array.StringBuilder).Append(record.Mode)

	// Price information
	w.recordBldr.Field(6).(*array.Float64Builder).Append(record.LastPrice)
	w.recordBldr.Field(7).(*array.Int32Builder).Append(record.LastTradedQuantity)
	w.recordBldr.Field(8).(*array.Float64Builder).Append(record.AverageTradePrice)
	w.recordBldr.Field(9).(*array.Int32Builder).Append(record.VolumeTraded)
	w.recordBldr.Field(10).(*array.Int32Builder).Append(record.TotalBuyQuantity)
	w.recordBldr.Field(11).(*array.Int32Builder).Append(record.TotalSellQuantity)
	w.recordBldr.Field(12).(*array.Int32Builder).Append(record.TotalBuy)
	w.recordBldr.Field(13).(*array.Int32Builder).Append(record.TotalSell)

	// OHLC
	w.recordBldr.Field(14).(*array.Float64Builder).Append(record.OhlcOpen)
	w.recordBldr.Field(15).(*array.Float64Builder).Append(record.OhlcHigh)
	w.recordBldr.Field(16).(*array.Float64Builder).Append(record.OhlcLow)
	w.recordBldr.Field(17).(*array.Float64Builder).Append(record.OhlcClose)

	// Market Depth - Buy Level 1
	w.recordBldr.Field(18).(*array.Float64Builder).Append(record.DepthBuyPrice1)
	w.recordBldr.Field(19).(*array.Int32Builder).Append(record.DepthBuyQuantity1)
	w.recordBldr.Field(20).(*array.Int32Builder).Append(record.DepthBuyOrders1)

	// Market Depth - Buy Level 2
	w.recordBldr.Field(21).(*array.Float64Builder).Append(record.DepthBuyPrice2)
	w.recordBldr.Field(22).(*array.Int32Builder).Append(record.DepthBuyQuantity2)
	w.recordBldr.Field(23).(*array.Int32Builder).Append(record.DepthBuyOrders2)

	// Market Depth - Buy Level 3
	w.recordBldr.Field(24).(*array.Float64Builder).Append(record.DepthBuyPrice3)
	w.recordBldr.Field(25).(*array.Int32Builder).Append(record.DepthBuyQuantity3)
	w.recordBldr.Field(26).(*array.Int32Builder).Append(record.DepthBuyOrders3)

	// Market Depth - Buy Level 4
	w.recordBldr.Field(27).(*array.Float64Builder).Append(record.DepthBuyPrice4)
	w.recordBldr.Field(28).(*array.Int32Builder).Append(record.DepthBuyQuantity4)
	w.recordBldr.Field(29).(*array.Int32Builder).Append(record.DepthBuyOrders4)

	// Market Depth - Buy Level 5
	w.recordBldr.Field(30).(*array.Float64Builder).Append(record.DepthBuyPrice5)
	w.recordBldr.Field(31).(*array.Int32Builder).Append(record.DepthBuyQuantity5)
	w.recordBldr.Field(32).(*array.Int32Builder).Append(record.DepthBuyOrders5)

	// Market Depth - Sell Level 1
	w.recordBldr.Field(33).(*array.Float64Builder).Append(record.DepthSellPrice1)
	w.recordBldr.Field(34).(*array.Int32Builder).Append(record.DepthSellQuantity1)
	w.recordBldr.Field(35).(*array.Int32Builder).Append(record.DepthSellOrders1)

	// Market Depth - Sell Level 2
	w.recordBldr.Field(36).(*array.Float64Builder).Append(record.DepthSellPrice2)
	w.recordBldr.Field(37).(*array.Int32Builder).Append(record.DepthSellQuantity2)
	w.recordBldr.Field(38).(*array.Int32Builder).Append(record.DepthSellOrders2)

	// Market Depth - Sell Level 3
	w.recordBldr.Field(39).(*array.Float64Builder).Append(record.DepthSellPrice3)
	w.recordBldr.Field(40).(*array.Int32Builder).Append(record.DepthSellQuantity3)
	w.recordBldr.Field(41).(*array.Int32Builder).Append(record.DepthSellOrders3)

	// Market Depth - Sell Level 4
	w.recordBldr.Field(42).(*array.Float64Builder).Append(record.DepthSellPrice4)
	w.recordBldr.Field(43).(*array.Int32Builder).Append(record.DepthSellQuantity4)
	w.recordBldr.Field(44).(*array.Int32Builder).Append(record.DepthSellOrders4)

	// Market Depth - Sell Level 5
	w.recordBldr.Field(45).(*array.Float64Builder).Append(record.DepthSellPrice5)
	w.recordBldr.Field(46).(*array.Int32Builder).Append(record.DepthSellQuantity5)
	w.recordBldr.Field(47).(*array.Int32Builder).Append(record.DepthSellOrders5)

	// Additional fields
	if record.LastTradeTime.Valid {
		w.recordBldr.Field(48).(*array.TimestampBuilder).Append(arrow.Timestamp(record.LastTradeTime.Time.UnixMicro()))
	} else {
		w.recordBldr.Field(48).(*array.TimestampBuilder).AppendNull()
	}
	w.recordBldr.Field(49).(*array.Float64Builder).Append(record.NetChange)

	// Metadata
	if record.TickReceivedTime.Valid {
		w.recordBldr.Field(50).(*array.TimestampBuilder).Append(arrow.Timestamp(record.TickReceivedTime.Time.UnixMicro()))
	} else {
		w.recordBldr.Field(50).(*array.TimestampBuilder).AppendNull()
	}
	if record.TickStoredInDbTime.Valid {
		w.recordBldr.Field(51).(*array.TimestampBuilder).Append(arrow.Timestamp(record.TickStoredInDbTime.Time.UnixMicro()))
	} else {
		w.recordBldr.Field(51).(*array.TimestampBuilder).AppendNull()
	}

	w.recordCount++

	// Check if we need to flush
	if w.recordCount >= w.flushSize {
		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush records: %w", err)
		}
	}

	return nil
}

// flush writes the current batch of records to the parquet file
func (w *ParquetWriter) flush() error {
	if w.recordCount == 0 {
		return nil
	}

	// Create record batch
	rec := w.recordBldr.NewRecord()
	defer rec.Release()

	// Log debug info
	log.Printf("Flushing %d records to parquet file %s", w.recordCount, w.file.Name())

	// Write record batch
	if err := w.writer.Write(rec); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Reset record count and builder
	prevCount := w.recordCount
	w.recordCount = 0
	w.recordBldr.Reserve(int(w.flushSize)) // Pre-allocate space for next batch

	// Monitor compression ratio
	if stats, err := w.GetStats(); err == nil {
		log.Printf("Flush complete - Records: %d, Total Size: %.2f MB, Compression: %.2fx",
			prevCount,
			float64(stats.CompressedSize)/(1024*1024),
			stats.CompressionRatio,
		)
		if stats.CompressionRatio < 2.0 {
			log.Printf("Warning: Low compression ratio (%.2f) for file %s",
				stats.CompressionRatio,
				w.file.Name(),
			)
		}
	}

	return nil
}

func (w *ParquetWriter) Close() error {
	if w.closed {
		return nil // Already closed
	}

	// Ensure any remaining records are flushed
	if w.recordCount > 0 {
		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush remaining records during close: %w", err)
		}
	}

	// Close the writer
	if err := w.writer.Close(); err != nil {
		w.file.Close() // Best effort to close file
		w.writer = nil
		w.closed = true
		return fmt.Errorf("failed to close writer: %w", err)
	}

	// Close the file
	if err := w.file.Close(); err != nil {
		w.writer = nil
		w.closed = true
		return fmt.Errorf("failed to close file: %w", err)
	}

	w.writer = nil
	w.closed = true

	// Log final stats
	if stats, err := w.GetStats(); err == nil {
		log.Printf("Final parquet file stats - Records: %d, Size: %.2f MB, Compression: %.2fx",
			stats.RecordCount,
			float64(stats.CompressedSize)/(1024*1024),
			stats.CompressionRatio,
		)
	}

	return nil
}

func (w *ParquetWriter) GetStats() (WriterStats, error) {
	stats := WriterStats{
		RecordCount:      w.recordCount,
		UncompressedSize: w.uncompressedSize,
	}

	if w.file == nil {
		return stats, fmt.Errorf("file is nil")
	}

	fileInfo, err := w.file.Stat()
	if err != nil {
		return stats, fmt.Errorf("failed to get file stats: %w", err)
	}

	stats.CompressedSize = fileInfo.Size()
	if stats.CompressedSize > 0 && stats.UncompressedSize > 0 {
		stats.CompressionRatio = float64(stats.UncompressedSize) / float64(stats.CompressedSize)
	}

	return stats, nil
}

// UncompressedSize returns the current uncompressed size of the parquet file
func (w *ParquetWriter) UncompressedSize() int64 {
	return w.uncompressedSize
}
