package filestore

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"gohustle/logger"
	pb "gohustle/proto"
)

// ArrowConverter handles the conversion of WAL data to Parquet format using Apache Arrow
type ArrowConverter struct {
	pool      memory.Allocator
	schema    *arrow.Schema
	batchSize int
	log       *logger.Logger
}

// NewArrowConverter creates a new instance of ArrowConverter
func NewArrowConverter() *ArrowConverter {
	// Define metadata for SQL compatibility
	metadata := arrow.NewMetadata([]string{
		"writer.type",
		"writer.version",
		"table.name",
		"table.description",
		"created_at",
		"updated_at",
	}, []string{
		"gohustle_arrow",
		"1.0",
		"market_ticks",
		"Market tick data with OHLC and depth information",
		time.Now().Format(time.RFC3339),
		time.Now().Format(time.RFC3339),
	})

	// Define schema matching our tick data structure
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "instrument_token", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "exchange_unix_timestamp", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: false},
			{Name: "last_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
			{Name: "open_interest", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "volume_traded", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "average_trade_price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		},
		&metadata,
	)

	return &ArrowConverter{
		pool:      memory.NewGoAllocator(),
		schema:    schema,
		batchSize: 100000, // Configurable batch size
		log:       logger.L(),
	}
}

// ConvertWALToParquet converts WAL data to Parquet format using Arrow as an intermediate step
func (ac *ArrowConverter) ConvertWALToParquet(indexName, date, outputPath string) error {
	// Create output file
	outFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Create Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1024*1024),                // 1MB page size
		parquet.WithDictionaryPageSizeLimit(128*1024*1024), // 128MB dictionary size
		parquet.WithMaxRowGroupLength(128*1024*1024),       // 128MB row groups
		parquet.WithCreatedBy("GoHustle-Arrow"),
		parquet.WithVersion(parquet.V2_LATEST),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithAllocator(ac.pool),
	)

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(
		ac.schema,
		outFile,
		writerProps,
		arrowProps,
	)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Create channels for parallel processing
	tickChan := make(chan *pb.TickData, ac.batchSize)
	errChan := make(chan error, 2)
	var wg sync.WaitGroup
	var recordsProcessed int64

	// Start WAL reader goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tickChan)

		ac.log.Info("Starting WAL reading", map[string]interface{}{
			"index": indexName,
			"date":  date,
		})

		if err := GetTickStore().StreamTicks(indexName, date, tickChan); err != nil {
			errChan <- fmt.Errorf("failed to stream ticks: %w", err)
			return
		}

		ac.log.Info("Completed WAL reading", map[string]interface{}{
			"index": indexName,
			"date":  date,
		})
	}()

	// Start Arrow conversion and writing goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		recordBuilder := array.NewRecordBuilder(ac.pool, ac.schema)
		defer recordBuilder.Release()

		batch := make([]*pb.TickData, 0, ac.batchSize)

		ac.log.Info("Starting Arrow conversion", map[string]interface{}{
			"batch_size": ac.batchSize,
		})

		for tick := range tickChan {
			batch = append(batch, tick)

			if len(batch) >= ac.batchSize {
				if err := ac.processBatch(batch, recordBuilder, writer); err != nil {
					errChan <- fmt.Errorf("failed to process batch: %w", err)
					return
				}

				atomic.AddInt64(&recordsProcessed, int64(len(batch)))
				ac.log.Info("Processed batch", map[string]interface{}{
					"records_processed": atomic.LoadInt64(&recordsProcessed),
				})

				batch = batch[:0]
			}
		}

		// Process remaining records
		if len(batch) > 0 {
			if err := ac.processBatch(batch, recordBuilder, writer); err != nil {
				errChan <- fmt.Errorf("failed to process final batch: %w", err)
				return
			}
			atomic.AddInt64(&recordsProcessed, int64(len(batch)))
		}

		ac.log.Info("Completed Arrow conversion", map[string]interface{}{
			"total_records": atomic.LoadInt64(&recordsProcessed),
		})
	}()

	// Wait for completion
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

// processBatch converts a batch of tick data to Arrow format and writes to Parquet
func (ac *ArrowConverter) processBatch(batch []*pb.TickData, builder *array.RecordBuilder, writer *pqarrow.FileWriter) error {
	// Reset builder for new batch
	builder.Reserve(len(batch))

	for _, tick := range batch {
		// Basic fields
		builder.Field(0).(*array.Int64Builder).AppendNull() // id is auto-generated
		builder.Field(1).(*array.Int64Builder).Append(int64(tick.InstrumentToken))
		builder.Field(2).(*array.TimestampBuilder).Append(arrow.Timestamp(tick.ExchangeUnixTimestamp))
		builder.Field(3).(*array.Float64Builder).Append(tick.LastPrice)
		builder.Field(4).(*array.Int32Builder).Append(int32(tick.OpenInterest))
		builder.Field(5).(*array.Int32Builder).Append(int32(tick.VolumeTraded))
		builder.Field(6).(*array.Float64Builder).Append(tick.AverageTradePrice)
	}

	// Create record batch
	record := builder.NewRecord()
	defer record.Release()

	// Write record batch
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}
