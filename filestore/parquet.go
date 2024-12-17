package filestore

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"
	"github.com/jackc/pgx/v5/pgtype"
)

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
	file       *os.File
	writer     *pqarrow.FileWriter
	recordBldr *array.RecordBuilder
}

func NewParquetWriter(filePath string) (*ParquetWriter, error) {
	// Create directory if it doesn't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// Define complete schema matching TimescaleDB
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "instrument_token", Type: arrow.PrimitiveTypes.Int64},
			{Name: "timestamp", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "is_tradable", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "is_index", Type: arrow.FixedWidthTypes.Boolean},
			{Name: "mode", Type: arrow.BinaryTypes.String},

			// Price information
			{Name: "last_price", Type: arrow.PrimitiveTypes.Float64},
			{Name: "last_traded_quantity", Type: arrow.PrimitiveTypes.Int32},
			{Name: "average_trade_price", Type: arrow.PrimitiveTypes.Float64},
			{Name: "volume_traded", Type: arrow.PrimitiveTypes.Int32},
			{Name: "total_buy_quantity", Type: arrow.PrimitiveTypes.Int32},
			{Name: "total_sell_quantity", Type: arrow.PrimitiveTypes.Int32},
			{Name: "total_buy", Type: arrow.PrimitiveTypes.Int32},
			{Name: "total_sell", Type: arrow.PrimitiveTypes.Int32},

			// OHLC
			{Name: "ohlc_open", Type: arrow.PrimitiveTypes.Float64},
			{Name: "ohlc_high", Type: arrow.PrimitiveTypes.Float64},
			{Name: "ohlc_low", Type: arrow.PrimitiveTypes.Float64},
			{Name: "ohlc_close", Type: arrow.PrimitiveTypes.Float64},

			// Market Depth - Buy
			{Name: "depth_buy_price_1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_buy_quantity_1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_orders_1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_price_2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_buy_quantity_2", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_orders_2", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_price_3", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_buy_quantity_3", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_orders_3", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_price_4", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_buy_quantity_4", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_orders_4", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_price_5", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_buy_quantity_5", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_buy_orders_5", Type: arrow.PrimitiveTypes.Int32},

			// Market Depth - Sell
			{Name: "depth_sell_price_1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_sell_quantity_1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_orders_1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_price_2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_sell_quantity_2", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_orders_2", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_price_3", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_sell_quantity_3", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_orders_3", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_price_4", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_sell_quantity_4", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_orders_4", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_price_5", Type: arrow.PrimitiveTypes.Float64},
			{Name: "depth_sell_quantity_5", Type: arrow.PrimitiveTypes.Int32},
			{Name: "depth_sell_orders_5", Type: arrow.PrimitiveTypes.Int32},

			// Additional fields
			{Name: "last_trade_time", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "net_change", Type: arrow.PrimitiveTypes.Float64},

			// Metadata
			{Name: "tick_received_time", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "tick_stored_in_db_time", Type: arrow.FixedWidthTypes.Timestamp_us},
		},
		nil,
	)

	// Create Arrow memory allocator
	pool := memory.NewGoAllocator()

	// Create writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageSize(1024*1024), // 1MB
		parquet.WithBatchSize(1000),
	)

	// Create Arrow writer properties
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	// Create parquet writer
	writer, err := pqarrow.NewFileWriter(
		schema,
		file,
		writerProps,
		arrowProps,
	)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create parquet writer: %w", err)
	}

	return &ParquetWriter{
		file:       file,
		writer:     writer,
		recordBldr: array.NewRecordBuilder(pool, schema),
	}, nil
}

func (w *ParquetWriter) Write(record TickRecord) error {
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
	w.recordBldr.Field(48).(*array.TimestampBuilder).Append(arrow.Timestamp(record.LastTradeTime.Time.UnixMicro()))
	w.recordBldr.Field(49).(*array.Float64Builder).Append(record.NetChange)

	// Metadata
	w.recordBldr.Field(50).(*array.TimestampBuilder).Append(arrow.Timestamp(record.TickReceivedTime.Time.UnixMicro()))
	w.recordBldr.Field(51).(*array.TimestampBuilder).Append(arrow.Timestamp(record.TickStoredInDbTime.Time.UnixMicro()))

	// Create record batch
	rec := w.recordBldr.NewRecord()
	defer rec.Release()

	// Write record batch
	if err := w.writer.Write(rec); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func (w *ParquetWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("failed to close writer: %w", err)
	}
	return w.file.Close()
}
