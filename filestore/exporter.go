package filestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gohustle/logger"
	"gohustle/proto"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/pgzip"
	googleproto "google.golang.org/protobuf/proto"
)

type Exporter struct {
	pool    *pgxpool.Pool
	logger  *logger.Logger
	baseDir string
}

func NewExporter(pool *pgxpool.Pool, baseDir string) *Exporter {
	return &Exporter{
		pool:    pool,
		logger:  logger.GetLogger(),
		baseDir: baseDir,
	}
}

// ExportTableToProto exports a table to a compressed protobuf file
func (e *Exporter) ExportTableToProto(ctx context.Context, tableName string) (string, error) {
	startTime := time.Now()
	e.logger.Info("Starting proto export", map[string]interface{}{
		"table": tableName,
		"time":  startTime,
	})

	// Create export directory
	exportDir := filepath.Join(e.baseDir, time.Now().Format("20060102"))
	if err := os.MkdirAll(exportDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create export directory: %w", err)
	}

	// Create proto file with gzip compression
	protoFile := filepath.Join(exportDir, fmt.Sprintf("%s_%s.pb.gz",
		tableName,
		time.Now().Format("20060102_150405"),
	))

	// Create gzip writer
	file, err := os.Create(protoFile)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	gzWriter := pgzip.NewWriter(file)
	defer gzWriter.Close()

	// Query data in batches
	const batchSize = 100000
	var offset int64 = 0
	var totalRows int64 = 0
	var totalBytes int64 = 0

	for {
		query := fmt.Sprintf(`
			SELECT * FROM %s 
			WHERE timestamp::date = CURRENT_DATE
			ORDER BY timestamp
			LIMIT %d OFFSET %d
		`, tableName, batchSize, offset)

		rows, err := e.pool.Query(ctx, query)
		if err != nil {
			return "", fmt.Errorf("query failed: %w", err)
		}

		rowCount := 0
		for rows.Next() {
			var record TickRecord
			if err := rows.Scan(
				&record.ID,
				&record.InstrumentToken,
				&record.Timestamp,
				&record.IsTradable,
				&record.IsIndex,
				&record.Mode,
				&record.LastPrice,
				&record.LastTradedQuantity,
				&record.AverageTradePrice,
				&record.VolumeTraded,
				&record.TotalBuyQuantity,
				&record.TotalSellQuantity,
				&record.TotalBuy,
				&record.TotalSell,
				&record.OhlcOpen,
				&record.OhlcHigh,
				&record.OhlcLow,
				&record.OhlcClose,
				&record.DepthBuyPrice1,
				&record.DepthBuyQuantity1,
				&record.DepthBuyOrders1,
				&record.DepthBuyPrice2,
				&record.DepthBuyQuantity2,
				&record.DepthBuyOrders2,
				&record.DepthBuyPrice3,
				&record.DepthBuyQuantity3,
				&record.DepthBuyOrders3,
				&record.DepthBuyPrice4,
				&record.DepthBuyQuantity4,
				&record.DepthBuyOrders4,
				&record.DepthBuyPrice5,
				&record.DepthBuyQuantity5,
				&record.DepthBuyOrders5,
				&record.DepthSellPrice1,
				&record.DepthSellQuantity1,
				&record.DepthSellOrders1,
				&record.DepthSellPrice2,
				&record.DepthSellQuantity2,
				&record.DepthSellOrders2,
				&record.DepthSellPrice3,
				&record.DepthSellQuantity3,
				&record.DepthSellOrders3,
				&record.DepthSellPrice4,
				&record.DepthSellQuantity4,
				&record.DepthSellOrders4,
				&record.DepthSellPrice5,
				&record.DepthSellQuantity5,
				&record.DepthSellOrders5,
				&record.LastTradeTime,
				&record.NetChange,
				&record.TickReceivedTime,
				&record.TickStoredInDbTime,
			); err != nil {
				rows.Close()
				return "", fmt.Errorf("scan failed: %w", err)
			}

			// Convert to proto message
			protoMsg := &proto.TickData{
				InstrumentToken:    uint32(record.InstrumentToken),
				Timestamp:          record.Timestamp.Time.Unix(),
				IsTradable:         record.IsTradable,
				IsIndex:            record.IsIndex,
				Mode:               record.Mode,
				LastPrice:          record.LastPrice,
				LastTradedQuantity: uint32(record.LastTradedQuantity),
				AverageTradePrice:  record.AverageTradePrice,
				VolumeTraded:       uint32(record.VolumeTraded),
				TotalBuyQuantity:   uint32(record.TotalBuyQuantity),
				TotalSellQuantity:  uint32(record.TotalSellQuantity),
				TotalBuy:           uint32(record.TotalBuy),
				TotalSell:          uint32(record.TotalSell),

				// OHLC as a nested message
				Ohlc: &proto.TickData_OHLC{
					Open:  record.OhlcOpen,
					High:  record.OhlcHigh,
					Low:   record.OhlcLow,
					Close: record.OhlcClose,
				},

				NetChange:     record.NetChange,
				LastTradeTime: record.LastTradeTime.Time.Unix(),

				// Market depth as a nested message
				Depth: &proto.TickData_MarketDepth{
					Buy: []*proto.TickData_DepthItem{
						{
							Price:    record.DepthBuyPrice1,
							Quantity: uint32(record.DepthBuyQuantity1),
							Orders:   uint32(record.DepthBuyOrders1),
						},
						{
							Price:    record.DepthBuyPrice2,
							Quantity: uint32(record.DepthBuyQuantity2),
							Orders:   uint32(record.DepthBuyOrders2),
						},
						{
							Price:    record.DepthBuyPrice3,
							Quantity: uint32(record.DepthBuyQuantity3),
							Orders:   uint32(record.DepthBuyOrders3),
						},
						{
							Price:    record.DepthBuyPrice4,
							Quantity: uint32(record.DepthBuyQuantity4),
							Orders:   uint32(record.DepthBuyOrders4),
						},
						{
							Price:    record.DepthBuyPrice5,
							Quantity: uint32(record.DepthBuyQuantity5),
							Orders:   uint32(record.DepthBuyOrders5),
						},
					},
					Sell: []*proto.TickData_DepthItem{
						{
							Price:    record.DepthSellPrice1,
							Quantity: uint32(record.DepthSellQuantity1),
							Orders:   uint32(record.DepthSellOrders1),
						},
						{
							Price:    record.DepthSellPrice2,
							Quantity: uint32(record.DepthSellQuantity2),
							Orders:   uint32(record.DepthSellOrders2),
						},
						{
							Price:    record.DepthSellPrice3,
							Quantity: uint32(record.DepthSellQuantity3),
							Orders:   uint32(record.DepthSellOrders3),
						},
						{
							Price:    record.DepthSellPrice4,
							Quantity: uint32(record.DepthSellQuantity4),
							Orders:   uint32(record.DepthSellOrders4),
						},
						{
							Price:    record.DepthSellPrice5,
							Quantity: uint32(record.DepthSellQuantity5),
							Orders:   uint32(record.DepthSellOrders5),
						},
					},
				},
			}

			// Serialize the proto message
			data, err := googleproto.Marshal(protoMsg)
			if err != nil {
				rows.Close()
				return "", fmt.Errorf("marshal failed: %w", err)
			}

			// Write size of message first (for reading back)
			size := uint32(len(data))
			if err := binary.Write(gzWriter, binary.LittleEndian, size); err != nil {
				rows.Close()
				return "", fmt.Errorf("size write failed: %w", err)
			}

			// Write the message
			if _, err := gzWriter.Write(data); err != nil {
				rows.Close()
				return "", fmt.Errorf("data write failed: %w", err)
			}

			totalBytes += int64(size) + 4 // 4 bytes for size
			rowCount++
		}

		rows.Close()

		if rowCount == 0 {
			break
		}

		totalRows += int64(rowCount)
		offset += int64(rowCount)

		e.logger.Info("Export progress", map[string]interface{}{
			"table":      tableName,
			"batch":      offset / batchSize,
			"total_rows": totalRows,
			"bytes":      totalBytes,
		})
	}

	// Flush and close writers
	if err := gzWriter.Close(); err != nil {
		return "", fmt.Errorf("failed to close gzip writer: %w", err)
	}

	if err := file.Close(); err != nil {
		return "", fmt.Errorf("failed to close file: %w", err)
	}

	// Get final file size
	fileInfo, err := os.Stat(protoFile)
	if err == nil {
		e.logger.Info("Export completed", map[string]interface{}{
			"table":             tableName,
			"file":              protoFile,
			"rows":              totalRows,
			"uncompressed_mb":   float64(totalBytes) / 1024 / 1024,
			"compressed_mb":     float64(fileInfo.Size()) / 1024 / 1024,
			"compression_ratio": float64(totalBytes) / float64(fileInfo.Size()),
			"duration":          time.Since(startTime),
		})
	}

	return protoFile, nil
}
