package filestore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"gohustle/logger"

	"github.com/jackc/pgx/v5/pgxpool"
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

// ExportTable exports a table using COPY command and compresses it
func (e *Exporter) ExportTable(ctx context.Context, tableName string) (string, error) {
	startTime := time.Now()
	e.logger.Info("Starting table export", map[string]interface{}{
		"table": tableName,
		"time":  startTime,
	})

	// Create export directory if not exists
	exportDir := filepath.Join(e.baseDir, time.Now().Format("20060102"))
	if err := os.MkdirAll(exportDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create export directory: %w", err)
	}

	// Create temporary CSV file
	tempCSV := filepath.Join(exportDir, fmt.Sprintf("%s.csv", tableName))
	csvFile, err := os.Create(tempCSV)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer func() {
		csvFile.Close()
		// Clean up CSV after compression
		os.Remove(tempCSV)
	}()

	// Execute COPY command
	copyQuery := fmt.Sprintf(`
        COPY (
            SELECT * FROM %s 
            WHERE timestamp::date = CURRENT_DATE
            ORDER BY timestamp
        ) TO STDOUT WITH CSV HEADER
    `, tableName)

	e.logger.Info("Starting COPY operation", map[string]interface{}{
		"table": tableName,
	})

	conn, err := e.pool.Acquire(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Conn().PgConn().CopyTo(ctx, csvFile, copyQuery); err != nil {
		return "", fmt.Errorf("COPY operation failed: %w", err)
	}

	// Get file size for logging
	fileInfo, err := csvFile.Stat()
	if err == nil {
		e.logger.Info("COPY completed", map[string]interface{}{
			"table":    tableName,
			"size_mb":  float64(fileInfo.Size()) / 1024 / 1024,
			"duration": time.Since(startTime),
		})
	}

	// Flush to disk
	if err := csvFile.Sync(); err != nil {
		return "", fmt.Errorf("failed to flush file: %w", err)
	}

	// Close the file before compression
	csvFile.Close()

	// Compress using gzip with maximum compression
	gzipFile := filepath.Join(exportDir, fmt.Sprintf("%s_%s.csv.gz",
		tableName,
		time.Now().Format("20060102_150405"),
	))

	e.logger.Info("Starting compression", map[string]interface{}{
		"table": tableName,
		"file":  gzipFile,
	})

	cmd := exec.Command("gzip", "-9", "-c", tempCSV)
	outFile, err := os.Create(gzipFile)
	if err != nil {
		return "", fmt.Errorf("failed to create gzip file: %w", err)
	}
	defer outFile.Close()

	cmd.Stdout = outFile
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("compression failed: %w", err)
	}

	// Get compressed file size
	if compressedInfo, err := os.Stat(gzipFile); err == nil {
		compressionRatio := float64(fileInfo.Size()) / float64(compressedInfo.Size())
		e.logger.Info("Export completed", map[string]interface{}{
			"table":              tableName,
			"original_size_mb":   float64(fileInfo.Size()) / 1024 / 1024,
			"compressed_size_mb": float64(compressedInfo.Size()) / 1024 / 1024,
			"compression_ratio":  compressionRatio,
			"duration":           time.Since(startTime),
		})
	}

	return gzipFile, nil
}
