package filestore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gohustle/proto"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/compress"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
)

// WriteTickBatchToParquet writes a batch of ticks to a parquet file
func WriteTickBatchToParquet(index string, batch *proto.TickBatch) error {
	if len(batch.Ticks) == 0 {
		return nil
	}

	// Generate filename based on first tick's timestamp
	firstTick := batch.Ticks[0]
	tickTime := time.Unix(firstTick.Timestamp, 0)

	// Format: data/NIFTY/YYYY-MM-DD/HH-MM.parquet
	filename := fmt.Sprintf("data/%s/%s/%02d-%02d.parquet",
		index,
		tickTime.Format("2006-01-02"),
		tickTime.Hour(),
		tickTime.Minute(),
	)

	// Ensure directory exists
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Convert batch to Arrow table
	table, err := convertBatchToArrowTable(batch)
	if err != nil {
		return fmt.Errorf("failed to convert batch to arrow table: %w", err)
	}
	defer table.Release()

	// Write to parquet file
	if err := writeTableToParquet(table, filename); err != nil {
		return fmt.Errorf("failed to write parquet file: %w", err)
	}

	return nil
}

// writeTableToParquet writes an Arrow table to a parquet file
func writeTableToParquet(table arrow.Table, filename string) error {
	// Open file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Write table to parquet
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	return pqarrow.WriteTable(table, file, 0, writerProps, arrowProps)
}

// convertBatchToArrowTable converts a batch of ticks to an Arrow table
func convertBatchToArrowTable(batch *proto.TickBatch) (arrow.Table, error) {
	// Create memory allocator
	pool := memory.NewGoAllocator()

	// Create builders for each column
	builders := struct {
		// Basic info
		instrumentToken *array.Uint32Builder
		isTradable      *array.BooleanBuilder
		isIndex         *array.BooleanBuilder
		mode            *array.StringBuilder

		// Timestamps
		timestamp     *array.Int64Builder
		lastTradeTime *array.Int64Builder

		// Price and quantity
		lastPrice          *array.Float64Builder
		lastTradedQuantity *array.Uint32Builder
		totalBuyQuantity   *array.Uint32Builder
		totalSellQuantity  *array.Uint32Builder
		volumeTraded       *array.Uint32Builder
		totalBuy           *array.Uint32Builder
		totalSell          *array.Uint32Builder
		averageTradePrice  *array.Float64Builder

		// OI related
		oi        *array.Uint32Builder
		oiDayHigh *array.Uint32Builder
		oiDayLow  *array.Uint32Builder
		netChange *array.Float64Builder

		// OHLC
		ohlcOpen  *array.Float64Builder
		ohlcHigh  *array.Float64Builder
		ohlcLow   *array.Float64Builder
		ohlcClose *array.Float64Builder

		// Additional metadata
		changePercent       *array.Float64Builder
		lastTradePrice      *array.Float64Builder
		openInterest        *array.Uint32Builder
		openInterestDayHigh *array.Uint32Builder
		openInterestDayLow  *array.Uint32Builder
	}{
		// Initialize all builders
		instrumentToken: array.NewUint32Builder(pool),
		isTradable:      array.NewBooleanBuilder(pool),
		isIndex:         array.NewBooleanBuilder(pool),
		mode:            array.NewStringBuilder(pool),
		// ... initialize other builders similarly
	}

	// Process each tick
	for _, tick := range batch.Ticks {
		builders.instrumentToken.Append(tick.InstrumentToken)
		builders.isTradable.Append(tick.IsTradable)
		builders.isIndex.Append(tick.IsIndex)
		builders.mode.Append(tick.Mode)
		// ... append other fields similarly
	}

	// Create schema and arrays similar to CollectIndexTicks
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "instrument_token", Type: arrow.PrimitiveTypes.Uint32},
			{Name: "is_tradable", Type: arrow.FixedWidthTypes.Boolean},
			// ... other fields
		},
		nil,
	)

	// Create arrays and columns
	arrays := []arrow.Array{
		builders.instrumentToken.NewArray(),
		builders.isTradable.NewArray(),
		// ... other arrays
	}

	cols := make([]arrow.Column, len(arrays))
	for i, arr := range arrays {
		chunked := arrow.NewChunked(arr.DataType(), []arrow.Array{arr})
		cols[i] = *arrow.NewColumn(schema.Field(i), chunked)
	}

	// Create and return table
	return array.NewTable(schema, cols, int64(builders.timestamp.Len())), nil
}
