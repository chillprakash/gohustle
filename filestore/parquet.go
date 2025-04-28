package filestore

import (
	"context"
	"fmt"
	"gohustle/utils"
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
	"github.com/apache/arrow/go/v14/parquet/file"
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
			currentDay: utils.NowIST().Format("2006-01-02"),
		}

		// Create data directory if it doesn't exist
		if err := os.MkdirAll(DefaultParquetDir, 0755); err != nil {
			parquetStoreInstance.log.Error("Failed to create parquet directory", map[string]interface{}{
				"error": err.Error(),
				"path":  DefaultParquetDir,
			})
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
		backupPath := filePath + fmt.Sprintf(".%s.bak", utils.NowIST().Format("150405"))
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

// GetOrCreateWriter gets existing writer or creates new one for the given path
func (ps *ParquetStore) GetOrCreateWriter(path string) (*ParquetWriter, error) {
	ps.writersMu.RLock()
	writer, exists := ps.writers[path]
	if exists {
		ps.writersMu.RUnlock()
		return writer, nil
	}
	ps.writersMu.RUnlock()

	// Need to create new writer
	ps.writersMu.Lock()
	defer ps.writersMu.Unlock()

	// Double check after acquiring write lock
	if writer, exists := ps.writers[path]; exists {
		return writer, nil
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create new writer
	writer, err := NewParquetWriter(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}

	ps.writers[path] = writer
	return writer, nil
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

	// Write records in smaller batches to avoid memory issues
	const batchSize = 10000
	totalRecords := len(records)
	processedRecords := 0

	for i := 0; i < totalRecords; i += batchSize {
		end := i + batchSize
		if end > totalRecords {
			end = totalRecords
		}

		// Write current batch
		for _, record := range records[i:end] {
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("failed to write record: %w", err)
			}
			processedRecords++
		}

		// Force flush after each batch
		if err := writer.flush(); err != nil {
			return fmt.Errorf("failed to flush batch: %w", err)
		}

		// Log progress
		if processedRecords%100000 == 0 {
			ps.log.Info("Writing progress", map[string]interface{}{
				"index":             index,
				"processed_records": processedRecords,
				"total_records":     totalRecords,
				"percent_complete":  float64(processedRecords) / float64(totalRecords) * 100,
			})
		}
	}

	// Final flush to ensure all records are written
	if err := writer.flush(); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
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
	ID                int64
	InstrumentToken   int64
	ExchangeTimestamp pgtype.Timestamp
	LastPrice         float64
	OpenInterest      int32
	VolumeTraded      int32
	AverageTradePrice float64
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

	// Always create a new file, overwriting any existing one
	// This ensures proper Parquet file format from start to end
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
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
			{Name: "exchange_unix_timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "last_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "open_interest", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "volume_traded", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "average_trade_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
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
		parquet.WithBatchSize(1024), // Smaller batch size for better memory usage
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
		flushSize:        250000,
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

	if !record.ExchangeTimestamp.Valid {
		return fmt.Errorf("timestamp is required")
	}

	// Debug timestamp
	log.Printf("Writing timestamp - Original: %v, Unix: %d, UnixNano: %d",
		record.ExchangeTimestamp.Time,
		record.ExchangeTimestamp.Time.Unix(),
		record.ExchangeTimestamp.Time.UnixNano())

	// Estimate uncompressed size (rough approximation)
	// Each numeric field ~8 bytes, string ~32 bytes, timestamp ~8 bytes
	w.uncompressedSize += 8*30 + 32*1 + 8*4 // 30 numeric fields, 1 string field, 4 timestamps

	// Build the record
	// Basic fields
	w.recordBldr.Field(0).(*array.Int64Builder).Append(record.ID)
	w.recordBldr.Field(1).(*array.Int64Builder).Append(record.InstrumentToken)

	// Convert timestamp to Unix seconds
	unixTimestamp := record.ExchangeTimestamp.Time.Unix()
	w.recordBldr.Field(2).(*array.Int64Builder).Append(unixTimestamp)

	w.recordBldr.Field(3).(*array.Float64Builder).Append(record.LastPrice)
	w.recordBldr.Field(4).(*array.Int32Builder).Append(record.OpenInterest)
	w.recordBldr.Field(5).(*array.Int32Builder).Append(record.VolumeTraded)
	w.recordBldr.Field(6).(*array.Float64Builder).Append(record.AverageTradePrice)

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

	// Get stats before closing
	stats, _ := w.GetStats()
	initialRecordCount := stats.RecordCount

	// Close the writer (this writes the Parquet footer and closes the underlying file)
	if err := w.writer.Close(); err != nil {
		w.closed = true
		return fmt.Errorf("failed to close writer: %w", err)
	}

	w.file = nil
	w.writer = nil
	w.closed = true

	// Log final stats
	log.Printf("Final parquet file stats - Records: %d, Size: %.2f MB",
		initialRecordCount,
		float64(stats.CompressedSize)/(1024*1024))

	return nil
}

func (w *ParquetWriter) GetStats() (WriterStats, error) {
	if w.file == nil {
		return WriterStats{}, fmt.Errorf("file is nil")
	}

	// Get current file info
	fileInfo, err := w.file.Stat()
	if err != nil {
		return WriterStats{}, fmt.Errorf("failed to get file stats: %w", err)
	}

	// Get current record count from the writer
	stats := WriterStats{
		RecordCount:      w.recordCount,
		UncompressedSize: w.uncompressedSize,
		CompressedSize:   fileInfo.Size(),
	}

	if stats.CompressedSize > 0 && stats.UncompressedSize > 0 {
		stats.CompressionRatio = float64(stats.UncompressedSize) / float64(stats.CompressedSize)
	}

	return stats, nil
}

// UncompressedSize returns the current uncompressed size of the parquet file
func (w *ParquetWriter) UncompressedSize() int64 {
	return w.uncompressedSize
}

// ReadSamples reads the first n samples from a parquet file
func (ps *ParquetStore) ReadSamples(filePath string, numSamples int) ([]TickRecord, error) {
	ps.log.Info("Reading parquet samples", map[string]interface{}{
		"file":              filePath,
		"samples_requested": numSamples,
	})

	// Open and read the parquet file directly
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	// Create parquet file reader
	pf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file reader: %w", err)
	}
	defer pf.Close()

	// Create Arrow reader
	readProps := pqarrow.ArrowReadProperties{}
	arrowReader, err := pqarrow.NewFileReader(pf, readProps, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read the table with context
	ctx := context.Background()
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read table: %w", err)
	}
	defer table.Release()

	// Get number of rows to read
	numRows := int(table.NumRows())
	if numSamples > numRows {
		numSamples = numRows
	}

	ps.log.Info("Converting samples", map[string]interface{}{
		"total_rows":      numRows,
		"samples_to_read": numSamples,
	})

	// Convert records
	samples := make([]TickRecord, 0, numSamples)
	for i := 0; i < numSamples; i++ {
		sample := TickRecord{}

		// Get chunks for each column we're interested in
		if chunk := table.Column(1).Data().Chunk(0); chunk != nil {
			if v, ok := chunk.(*array.Int64); ok && v.IsValid(i) {
				sample.InstrumentToken = v.Value(i)
			}
		}
		if chunk := table.Column(3).Data().Chunk(0); chunk != nil {
			if v, ok := chunk.(*array.Float64); ok && v.IsValid(i) {
				sample.LastPrice = v.Value(i)
			}
		}
		if chunk := table.Column(5).Data().Chunk(0); chunk != nil {
			if v, ok := chunk.(*array.Int32); ok && v.IsValid(i) {
				sample.VolumeTraded = v.Value(i)
			}
		}

		// Handle timestamps
		if chunk := table.Column(2).Data().Chunk(0); chunk != nil {
			if ts, ok := chunk.(*array.Int64); ok && ts.IsValid(i) {
				unixSeconds := ts.Value(i)
				sample.ExchangeTimestamp.Time = time.Unix(unixSeconds, 0)
				sample.ExchangeTimestamp.Valid = true

				// Debug final timestamp
				log.Printf("Final timestamp: %v (Unix: %d)", sample.ExchangeTimestamp.Time, unixSeconds)
			}
		}

		samples = append(samples, sample)
	}

	ps.log.Info("Successfully read samples", map[string]interface{}{
		"samples_read": len(samples),
	})

	return samples, nil
}
