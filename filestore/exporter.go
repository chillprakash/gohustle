package filestore

import (
	"context"
	"encoding/binary"
	"fmt"
	"gohustle/logger"
	"gohustle/utils"
	"os"
	"path/filepath"
	"time"

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
		logger:  logger.L(),
		baseDir: baseDir,
	}
}

// ExportTableToProto exports a table to a compressed protobuf file
func (e *Exporter) ExportTableToProto(ctx context.Context, tableName string) (string, error) {
	startTime := utils.NowIST()
	e.logger.Info("Starting proto export", map[string]interface{}{
		"table": tableName,
		"time":  startTime,
	})

	// Create export directory
	exportDir := filepath.Join(e.baseDir)
	if err := os.MkdirAll(exportDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create export directory: %w", err)
	}

	// Create proto file with gzip compression
	protoFile := filepath.Join(exportDir, fmt.Sprintf("%s_%s.pb.gz",
		tableName,
		time.Now().Format("20060102"),
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
				&record.ExchangeTimestamp,
				&record.LastPrice,
				&record.OpenInterest,
				&record.VolumeTraded,
				&record.AverageTradePrice,
			); err != nil {
				rows.Close()
				return "", fmt.Errorf("scan failed: %w", err)
			}

			// Convert to proto message
			protoMsg := &proto.TickData{
				InstrumentToken:       uint32(record.InstrumentToken),
				ExchangeUnixTimestamp: record.ExchangeTimestamp.Time.Unix(),
				LastPrice:             record.LastPrice,
				OpenInterest:          uint32(record.OpenInterest),
				VolumeTraded:          uint32(record.VolumeTraded),
				AverageTradePrice:     record.AverageTradePrice,
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
