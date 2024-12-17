package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/jmoiron/sqlx"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDB struct {
	db           *sqlx.DB
	exportPath   string
	exportTicker *time.Ticker
	log          *logger.Logger
	mu           sync.RWMutex
	tables       map[string]bool
	tablesMu     sync.RWMutex
}

func newDuckDB() (*DuckDB, error) {
	log := logger.GetLogger()
	cfg := config.GetConfig()

	// Create data directory if not exists
	if err := os.MkdirAll(cfg.DuckDB.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Simple connection string with basic settings
	connStr := fmt.Sprintf("?threads=%d&memory_limit=%s",
		cfg.DuckDB.Threads,
		cfg.DuckDB.MemoryLimit,
	)

	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Basic connection pool settings
	db.SetMaxOpenConns(cfg.DuckDB.MaxOpenConns)
	db.SetMaxIdleConns(cfg.DuckDB.MaxIdleConns)

	connLifetime, err := time.ParseDuration(cfg.DuckDB.ConnLifetime)
	if err != nil {
		return nil, fmt.Errorf("invalid connection lifetime: %w", err)
	}
	db.SetConnMaxLifetime(connLifetime)

	duck := &DuckDB{
		db:           sqlx.NewDb(db, "duckdb"),
		exportPath:   filepath.Join(cfg.DuckDB.DataDir, "exports"),
		exportTicker: time.NewTicker(1 * time.Hour),
		log:          log,
		tables:       make(map[string]bool),
	}

	// Initialize existing tables map
	if err := duck.loadExistingTables(); err != nil {
		db.Close()
		return nil, err
	}

	// Start export routine
	go duck.runExporter()

	log.Info("DuckDB initialized successfully", map[string]interface{}{
		"data_dir":      cfg.DuckDB.DataDir,
		"threads":       cfg.DuckDB.Threads,
		"memory_limit":  cfg.DuckDB.MemoryLimit,
		"max_open_conn": cfg.DuckDB.MaxOpenConns,
	})
	return duck, nil
}

func (d *DuckDB) loadExistingTables() error {
	d.tablesMu.Lock()
	defer d.tablesMu.Unlock()

	rows, err := d.db.Query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
    `)
	if err != nil {
		return fmt.Errorf("failed to query existing tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			continue
		}
		d.tables[tableName] = true
	}

	return nil
}

func (d *DuckDB) ensureTableExists(tableName string) error {
	d.tablesMu.RLock()
	exists := d.tables[tableName]
	d.tablesMu.RUnlock()

	if exists {
		return nil
	}

	d.tablesMu.Lock()
	defer d.tablesMu.Unlock()

	// Double-check after acquiring write lock
	if d.tables[tableName] {
		return nil
	}

	// Create table using the standard schema
	if err := d.createTable(tableName); err != nil {
		return err
	}

	d.tables[tableName] = true
	return nil
}

func (d *DuckDB) createTable(tableName string) error {
	log := logger.GetLogger()
	log.Info("Creating table schema", map[string]interface{}{
		"table": tableName,
	})

	// Modified SQL to use dynamic table name
	_, err := d.db.Exec(fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            -- Add a sequence for unique IDs
            id BIGINT,
            
            -- Basic info
            instrument_token BIGINT,
            timestamp BIGINT,
            is_tradable BOOLEAN,
            is_index BOOLEAN,
            mode VARCHAR,
            
            -- Price info
            last_price DOUBLE,
            last_traded_quantity BIGINT,
            average_trade_price DOUBLE,
            volume_traded BIGINT,
            total_buy_quantity BIGINT,
            total_sell_quantity BIGINT,
            total_buy BIGINT,
            total_sell BIGINT,
            
            -- OHLC
            ohlc_open DOUBLE,
            ohlc_high DOUBLE,
            ohlc_low DOUBLE,
            ohlc_close DOUBLE,
            
            -- Market Depth - Buy
            depth_buy_price_1 DOUBLE,
            depth_buy_quantity_1 BIGINT,
            depth_buy_orders_1 BIGINT,
            depth_buy_price_2 DOUBLE,
            depth_buy_quantity_2 BIGINT,
            depth_buy_orders_2 BIGINT,
            depth_buy_price_3 DOUBLE,
            depth_buy_quantity_3 BIGINT,
            depth_buy_orders_3 BIGINT,
            depth_buy_price_4 DOUBLE,
            depth_buy_quantity_4 BIGINT,
            depth_buy_orders_4 BIGINT,
            depth_buy_price_5 DOUBLE,
            depth_buy_quantity_5 BIGINT,
            depth_buy_orders_5 BIGINT,
            
            -- Market Depth - Sell
            depth_sell_price_1 DOUBLE,
            depth_sell_quantity_1 BIGINT,
            depth_sell_orders_1 BIGINT,
            depth_sell_price_2 DOUBLE,
            depth_sell_quantity_2 BIGINT,
            depth_sell_orders_2 BIGINT,
            depth_sell_price_3 DOUBLE,
            depth_sell_quantity_3 BIGINT,
            depth_sell_orders_3 BIGINT,
            depth_sell_price_4 DOUBLE,
            depth_sell_quantity_4 BIGINT,
            depth_sell_orders_4 BIGINT,
            depth_sell_price_5 DOUBLE,
            depth_sell_quantity_5 BIGINT,
            depth_sell_orders_5 BIGINT,
            
            -- Additional fields
            last_trade_time BIGINT,
            oi BIGINT,
            oi_day_high BIGINT,
            oi_day_low BIGINT,
            net_change DOUBLE,
            
            -- Metadata
            target_file VARCHAR,
            tick_received_time BIGINT,
            tick_stored_in_db_time BIGINT
        )
    `, tableName))

	if err != nil {
		log.Error("Failed to create table", map[string]interface{}{
			"error": err.Error(),
			"table": tableName,
		})
		return err
	}

	// Create index with dynamic table name
	_, err = d.db.Exec(fmt.Sprintf(`
        CREATE INDEX IF NOT EXISTS idx_%s_token_time ON %s (instrument_token, timestamp)
    `, tableName, tableName))

	if err != nil {
		log.Error("Failed to create index", map[string]interface{}{
			"error": err.Error(),
			"table": tableName,
		})
		return err
	}

	// Verify table creation with dynamic table name
	var tableExists bool
	err = d.db.QueryRow(fmt.Sprintf(`
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = '%s'
        )
    `, tableName)).Scan(&tableExists)

	if err != nil {
		log.Error("Failed to verify table creation", map[string]interface{}{
			"error": err.Error(),
			"table": tableName,
		})
		return err
	}

	if tableExists {
		log.Info("Table created successfully", map[string]interface{}{
			"table": tableName,
		})
	} else {
		log.Error("Table was not created", map[string]interface{}{
			"table": tableName,
		})
		return fmt.Errorf("table '%s' was not created", tableName)
	}

	return nil
}

// Helper function to count columns and values
func (d *DuckDB) logColumnCount() {
	var columnCount int
	err := d.db.QueryRow(`
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name = 'ticks'
    `).Scan(&columnCount)

	if err == nil {
		d.log.Info("Table column count", map[string]interface{}{
			"count": columnCount,
		})
	}
}

// WriteTick writes a single tick to DuckDB
func (d *DuckDB) WriteTick(tick *proto.TickData) error {
	// Add detailed entry logging
	d.log.Info("WriteTick entry", map[string]interface{}{
		"instrument_token": tick.InstrumentToken,
		"timestamp":        tick.Timestamp,
		"target_file":      tick.TargetFile,
		"last_price":       tick.LastPrice,
		"is_index":         tick.IsIndex,
	})

	if tick.TargetFile == "" {
		return fmt.Errorf("target file is empty")
	}

	// Generate and sanitize table name
	tableName := strings.TrimSuffix(tick.TargetFile, ".parquet")
	tableName = strings.ReplaceAll(tableName, "-", "_")
	tableName = strings.ToUpper(tableName) // Ensure uppercase

	// Log table creation attempt
	d.log.Info("Ensuring table exists", map[string]interface{}{
		"table": tableName,
	})

	// Ensure table exists
	if err := d.ensureTableExists(tableName); err != nil {
		d.log.Error("Failed to ensure table exists", map[string]interface{}{
			"error": err.Error(),
			"table": tableName,
		})
		return fmt.Errorf("failed to ensure table exists: %w", err)
	}

	// Verify table exists after creation
	var exists bool
	err := d.db.QueryRow(fmt.Sprintf(`
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = '%s'
        )
    `, tableName)).Scan(&exists)

	if err != nil || !exists {
		d.log.Error("Table verification failed", map[string]interface{}{
			"error":  err,
			"exists": exists,
			"table":  tableName,
		})
		return fmt.Errorf("table verification failed: %v", err)
	}

	// Log the actual insert attempt
	d.log.Info("Attempting insert", map[string]interface{}{
		"table":      tableName,
		"timestamp":  tick.Timestamp,
		"last_price": tick.LastPrice,
	})

	// Insert into specific table
	query := fmt.Sprintf(`
        INSERT INTO %s (
            instrument_token, timestamp, is_tradable, is_index, mode,
            last_price, last_traded_quantity, average_trade_price,
            volume_traded, total_buy_quantity, total_sell_quantity,
            total_buy, total_sell,
            ohlc_open, ohlc_high, ohlc_low, ohlc_close,
            depth_buy_price_1, depth_buy_quantity_1, depth_buy_orders_1,
            depth_buy_price_2, depth_buy_quantity_2, depth_buy_orders_2,
            depth_buy_price_3, depth_buy_quantity_3, depth_buy_orders_3,
            depth_buy_price_4, depth_buy_quantity_4, depth_buy_orders_4,
            depth_buy_price_5, depth_buy_quantity_5, depth_buy_orders_5,
            depth_sell_price_1, depth_sell_quantity_1, depth_sell_orders_1,
            depth_sell_price_2, depth_sell_quantity_2, depth_sell_orders_2,
            depth_sell_price_3, depth_sell_quantity_3, depth_sell_orders_3,
            depth_sell_price_4, depth_sell_quantity_4, depth_sell_orders_4,
            depth_sell_price_5, depth_sell_quantity_5, depth_sell_orders_5,
            last_trade_time, oi, oi_day_high, oi_day_low,
            net_change, target_file, tick_received_time, tick_stored_in_db_time
        ) VALUES (
            :instrument_token, :timestamp, :is_tradable, :is_index, :mode,
            :last_price, :last_traded_quantity, :average_trade_price,
            :volume_traded, :total_buy_quantity, :total_sell_quantity,
            :total_buy, :total_sell,
            :ohlc_open, :ohlc_high, :ohlc_low, :ohlc_close,
            :depth_buy_price_1, :depth_buy_quantity_1, :depth_buy_orders_1,
            :depth_buy_price_2, :depth_buy_quantity_2, :depth_buy_orders_2,
            :depth_buy_price_3, :depth_buy_quantity_3, :depth_buy_orders_3,
            :depth_buy_price_4, :depth_buy_quantity_4, :depth_buy_orders_4,
            :depth_buy_price_5, :depth_buy_quantity_5, :depth_buy_orders_5,
            :depth_sell_price_1, :depth_sell_quantity_1, :depth_sell_orders_1,
            :depth_sell_price_2, :depth_sell_quantity_2, :depth_sell_orders_2,
            :depth_sell_price_3, :depth_sell_quantity_3, :depth_sell_orders_3,
            :depth_sell_price_4, :depth_sell_quantity_4, :depth_sell_orders_4,
            :depth_sell_price_5, :depth_sell_quantity_5, :depth_sell_orders_5,
            :last_trade_time, :oi, :oi_day_high, :oi_day_low,
            :net_change, :target_file, :tick_received_time, :tick_stored_in_db_time
        )
    `, tableName)
	logger.GetLogger().Info("Table Name ", map[string]interface{}{
		"table": tableName,
	})
	// Prepare the data for insertion
	tickData := struct {
		InstrumentToken    uint32  `db:"instrument_token"`
		Timestamp          int64   `db:"timestamp"`
		IsTradable         bool    `db:"is_tradable"`
		IsIndex            bool    `db:"is_index"`
		Mode               string  `db:"mode"`
		LastPrice          float64 `db:"last_price"`
		LastTradedQuantity uint32  `db:"last_traded_quantity"`
		AverageTradePrice  float64 `db:"average_trade_price"`
		VolumeTraded       uint32  `db:"volume_traded"`
		TotalBuyQuantity   uint32  `db:"total_buy_quantity"`
		TotalSellQuantity  uint32  `db:"total_sell_quantity"`
		TotalBuy           uint32  `db:"total_buy"`
		TotalSell          uint32  `db:"total_sell"`
		OhlcOpen           float64 `db:"ohlc_open"`
		OhlcHigh           float64 `db:"ohlc_high"`
		OhlcLow            float64 `db:"ohlc_low"`
		OhlcClose          float64 `db:"ohlc_close"`
		// Buy depth
		DepthBuyPrice1    float64 `db:"depth_buy_price_1"`
		DepthBuyQuantity1 int64   `db:"depth_buy_quantity_1"`
		DepthBuyOrders1   int64   `db:"depth_buy_orders_1"`
		DepthBuyPrice2    float64 `db:"depth_buy_price_2"`
		DepthBuyQuantity2 int64   `db:"depth_buy_quantity_2"`
		DepthBuyOrders2   int64   `db:"depth_buy_orders_2"`
		DepthBuyPrice3    float64 `db:"depth_buy_price_3"`
		DepthBuyQuantity3 int64   `db:"depth_buy_quantity_3"`
		DepthBuyOrders3   int64   `db:"depth_buy_orders_3"`
		DepthBuyPrice4    float64 `db:"depth_buy_price_4"`
		DepthBuyQuantity4 int64   `db:"depth_buy_quantity_4"`
		DepthBuyOrders4   int64   `db:"depth_buy_orders_4"`
		DepthBuyPrice5    float64 `db:"depth_buy_price_5"`
		DepthBuyQuantity5 int64   `db:"depth_buy_quantity_5"`
		DepthBuyOrders5   int64   `db:"depth_buy_orders_5"`
		// Sell depth
		DepthSellPrice1    float64 `db:"depth_sell_price_1"`
		DepthSellQuantity1 int64   `db:"depth_sell_quantity_1"`
		DepthSellOrders1   int64   `db:"depth_sell_orders_1"`
		DepthSellPrice2    float64 `db:"depth_sell_price_2"`
		DepthSellQuantity2 int64   `db:"depth_sell_quantity_2"`
		DepthSellOrders2   int64   `db:"depth_sell_orders_2"`
		DepthSellPrice3    float64 `db:"depth_sell_price_3"`
		DepthSellQuantity3 int64   `db:"depth_sell_quantity_3"`
		DepthSellOrders3   int64   `db:"depth_sell_orders_3"`
		DepthSellPrice4    float64 `db:"depth_sell_price_4"`
		DepthSellQuantity4 int64   `db:"depth_sell_quantity_4"`
		DepthSellOrders4   int64   `db:"depth_sell_orders_4"`
		DepthSellPrice5    float64 `db:"depth_sell_price_5"`
		DepthSellQuantity5 int64   `db:"depth_sell_quantity_5"`
		DepthSellOrders5   int64   `db:"depth_sell_orders_5"`
		// Additional fields
		LastTradeTime      int64   `db:"last_trade_time"`
		Oi                 uint32  `db:"oi"`
		OiDayHigh          uint32  `db:"oi_day_high"`
		OiDayLow           uint32  `db:"oi_day_low"`
		NetChange          float64 `db:"net_change"`
		TargetFile         string  `db:"target_file"`
		TickReceivedTime   int64   `db:"tick_received_time"`
		TickStoredInDbTime int64   `db:"tick_stored_in_db_time"`
	}{
		InstrumentToken:    tick.InstrumentToken,
		Timestamp:          tick.Timestamp * 1000, // Convert to milliseconds
		IsTradable:         tick.IsTradable,
		IsIndex:            tick.IsIndex,
		Mode:               tick.Mode,
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: tick.LastTradedQuantity,
		AverageTradePrice:  tick.AverageTradePrice,
		VolumeTraded:       tick.VolumeTraded,
		TotalBuyQuantity:   tick.TotalBuyQuantity,
		TotalSellQuantity:  tick.TotalSellQuantity,
		TotalBuy:           tick.TotalBuy,
		TotalSell:          tick.TotalSell,
		OhlcOpen:           tick.Ohlc.Open,
		OhlcHigh:           tick.Ohlc.High,
		OhlcLow:            tick.Ohlc.Low,
		OhlcClose:          tick.Ohlc.Close,
		// Buy depth
		// DepthBuyPrice1:    getDepthValue(tick.Depth.Buy, 0, "price"),
		// DepthBuyQuantity1: int64(getDepthValue(tick.Depth.Buy, 0, "quantity")),
		// DepthBuyOrders1:   int64(getDepthValue(tick.Depth.Buy, 0, "orders")),
		// DepthBuyPrice2:    getDepthValue(tick.Depth.Buy, 1, "price"),
		// DepthBuyQuantity2: int64(getDepthValue(tick.Depth.Buy, 1, "quantity")),
		// DepthBuyOrders2:   int64(getDepthValue(tick.Depth.Buy, 1, "orders")),
		// DepthBuyPrice3:    getDepthValue(tick.Depth.Buy, 2, "price"),
		// DepthBuyQuantity3: int64(getDepthValue(tick.Depth.Buy, 2, "quantity")),
		// DepthBuyOrders3:   int64(getDepthValue(tick.Depth.Buy, 2, "orders")),
		// DepthBuyPrice4:    getDepthValue(tick.Depth.Buy, 3, "price"),
		// DepthBuyQuantity4: int64(getDepthValue(tick.Depth.Buy, 3, "quantity")),
		// DepthBuyOrders4:   int64(getDepthValue(tick.Depth.Buy, 3, "orders")),
		// DepthBuyPrice5:    getDepthValue(tick.Depth.Buy, 4, "price"),
		// DepthBuyQuantity5: int64(getDepthValue(tick.Depth.Buy, 4, "quantity")),
		// DepthBuyOrders5:   int64(getDepthValue(tick.Depth.Buy, 4, "orders")),
		// // Sell depth
		// DepthSellPrice1:    getDepthValue(tick.Depth.Sell, 0, "price"),
		// DepthSellQuantity1: int64(getDepthValue(tick.Depth.Sell, 0, "quantity")),
		// DepthSellOrders1:   int64(getDepthValue(tick.Depth.Sell, 0, "orders")),
		// DepthSellPrice2:    getDepthValue(tick.Depth.Sell, 1, "price"),
		// DepthSellQuantity2: int64(getDepthValue(tick.Depth.Sell, 1, "quantity")),
		// DepthSellOrders2:   int64(getDepthValue(tick.Depth.Sell, 1, "orders")),
		// DepthSellPrice3:    getDepthValue(tick.Depth.Sell, 2, "price"),
		// DepthSellQuantity3: int64(getDepthValue(tick.Depth.Sell, 2, "quantity")),
		// DepthSellOrders3:   int64(getDepthValue(tick.Depth.Sell, 2, "orders")),
		// DepthSellPrice4:    getDepthValue(tick.Depth.Sell, 3, "price"),
		// DepthSellQuantity4: int64(getDepthValue(tick.Depth.Sell, 3, "quantity")),
		// DepthSellOrders4:   int64(getDepthValue(tick.Depth.Sell, 3, "orders")),
		// DepthSellPrice5:    getDepthValue(tick.Depth.Sell, 4, "price"),
		// DepthSellQuantity5: int64(getDepthValue(tick.Depth.Sell, 4, "quantity")),
		// DepthSellOrders5:   int64(getDepthValue(tick.Depth.Sell, 4, "orders")),
		// Additional fields
		LastTradeTime:      tick.LastTradeTime * 1000, // Convert to milliseconds
		Oi:                 tick.Oi,
		OiDayHigh:          tick.OiDayHigh,
		OiDayLow:           tick.OiDayLow,
		NetChange:          tick.NetChange,
		TargetFile:         tick.TargetFile,
		TickReceivedTime:   time.Now().UnixMilli(),
		TickStoredInDbTime: time.Now().UnixMilli(),
	}

	result, err := d.db.NamedExec(query, tickData)
	if err != nil {
		d.log.Error("Failed to insert tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
			"table": tableName,
		})
		return fmt.Errorf("failed to insert tick into %s: %w", tableName, err)
	}

	// Get the number of rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		d.log.Error("Failed to get rows affected", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
			"table": tableName,
		})
	} else {
		// Log successful write with confirmation
		d.log.Info("Successfully wrote tick", map[string]interface{}{
			"table":            tableName,
			"timestamp":        tick.Timestamp,
			"instrument_token": tick.InstrumentToken,
			"rows_affected":    rowsAffected,
			"last_price":       tick.LastPrice,
			"write_time":       time.Now().UnixMilli(),
		})

		// Optional: Verify the write
		if rowsAffected != 1 {
			d.log.Error("Unexpected number of rows affected", map[string]interface{}{
				"rows_affected":    rowsAffected,
				"table":            tableName,
				"instrument_token": tick.InstrumentToken,
			})
		}
	}

	// After insert, verify the data was written
	var count int64
	err = d.db.QueryRow(fmt.Sprintf(`
        SELECT COUNT(*) 
        FROM %s 
        WHERE instrument_token = ? 
        AND timestamp = ?
    `, tableName), tick.InstrumentToken, tick.Timestamp*1000).Scan(&count)

	if err != nil {
		d.log.Error("Failed to verify write", map[string]interface{}{
			"error": err.Error(),
			"table": tableName,
		})
	} else {
		d.log.Info("Write verification", map[string]interface{}{
			"table":      tableName,
			"count":      count,
			"timestamp":  tick.Timestamp,
			"last_price": tick.LastPrice,
		})
	}

	return nil
}

// Export functions
func (d *DuckDB) runExporter() {
	for range d.exportTicker.C {
		if err := d.ExportToParquet(); err != nil {
			d.log.Error("Failed to export to parquet", map[string]interface{}{
				"error": err.Error(),
			})
		}
	}
}

func (d *DuckDB) ExportToParquet() error {
	d.tablesMu.RLock()
	tables := make([]string, 0, len(d.tables))
	for table := range d.tables {
		tables = append(tables, table)
	}
	d.tablesMu.RUnlock()

	for _, tableName := range tables {
		filename := fmt.Sprintf("%s.parquet", tableName)
		exportPath := filepath.Join(d.exportPath, filename)

		if err := d.exportTable(tableName, exportPath); err != nil {
			d.log.Error("Failed to export table", map[string]interface{}{
				"table": tableName,
				"error": err.Error(),
			})
			continue
		}
	}

	return nil
}

func (d *DuckDB) exportTable(tableName, exportPath string) error {
	_, err := d.db.Exec(fmt.Sprintf(`
        COPY (
            SELECT * FROM %s
        ) TO '%s' (FORMAT 'parquet')
    `, tableName, exportPath))
	return err
}

// Query helpers
func (d *DuckDB) GetTickStats(instrumentToken int64) (map[string]interface{}, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Declare variables to scan into
	var (
		count    int64
		avgPrice float64
		dayHigh  float64
		dayLow   float64
		volume   int64
	)

	row := d.db.QueryRow(`
        SELECT 
            COUNT(*) as tick_count,
            AVG(last_price) as avg_price,
            MAX(ohlc_high) as day_high,
            MIN(ohlc_low) as day_low,
            SUM(volume_traded) as total_volume
			price
        FROM ticks 
        WHERE instrument_token = ?
    `, instrumentToken)

	// Scan into local variables
	err := row.Scan(&count, &avgPrice, &dayHigh, &dayLow, &volume)
	if err != nil {
		return nil, fmt.Errorf("failed to scan tick stats: %w", err)
	}

	// Create map with scanned values
	stats := map[string]interface{}{
		"count":     count,
		"avg_price": avgPrice,
		"high":      dayHigh,
		"low":       dayLow,
		"volume":    volume,
	}

	return stats, nil
}

// Cleanup old data
func (d *DuckDB) CleanupOldData(days int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	_, err := d.db.Exec(`
        DELETE FROM ticks 
        WHERE timestamp < NOW() - INTERVAL '? days'`,
		days)
	return err
}

// Close database connection
func (d *DuckDB) Close() error {
	d.exportTicker.Stop()
	return d.db.Close()
}

// Helper function to safely get depth values
// func getDepthValue(depth []*proto.TickData_DepthItem, index int, field string) float64 {
// 	// Check for nil or out of bounds
// 	if depth == nil || index >= len(depth) || depth[index] == nil {
// 		return 0.0
// 	}

// 	// Return appropriate field value
// 	switch field {
// 	case "price":
// 		return depth[index].Price
// 	case "quantity":
// 		return float64(depth[index].Quantity)
// 	case "orders":
// 		return float64(depth[index].Orders)
// 	default:
// 		return 0.0
// 	}
// }

// Optional: Add a more detailed version with error handling
func getDepthValueWithError(depth []*proto.TickData_DepthItem, index int, field string) (float64, error) {
	if depth == nil {
		return 0.0, fmt.Errorf("depth array is nil")
	}

	if index >= len(depth) {
		return 0.0, fmt.Errorf("index %d out of bounds for depth length %d", index, len(depth))
	}

	if depth[index] == nil {
		return 0.0, fmt.Errorf("depth item at index %d is nil", index)
	}

	switch field {
	case "price":
		return depth[index].Price, nil
	case "quantity":
		return float64(depth[index].Quantity), nil
	case "orders":
		return float64(depth[index].Orders), nil
	default:
		return 0.0, fmt.Errorf("unknown field: %s", field)
	}
}

type TickData struct {
	InstrumentToken    uint32  `db:"instrument_token"`
	Timestamp          int64   `db:"timestamp"`
	IsTradable         bool    `db:"is_tradable"`
	IsIndex            bool    `db:"is_index"`
	Mode               string  `db:"mode"`
	LastPrice          float64 `db:"last_price"`
	LastTradedQuantity uint32  `db:"last_traded_quantity"`
	AverageTradePrice  float64 `db:"average_trade_price"`
	VolumeTraded       uint32  `db:"volume_traded"`
	TotalBuyQuantity   uint32  `db:"total_buy_quantity"`
	TotalSellQuantity  uint32  `db:"total_sell_quantity"`
	TotalBuy           uint32  `db:"total_buy"`
	TotalSell          uint32  `db:"total_sell"`
	OHLC               OHLC    `db:"ohlc"`

	// Flattened Depth Buy Fields
	DepthBuy1 PriceQuantityOrder `db:"depth_buy_1"`
	DepthBuy2 PriceQuantityOrder `db:"depth_buy_2"`
	DepthBuy3 PriceQuantityOrder `db:"depth_buy_3"`
	DepthBuy4 PriceQuantityOrder `db:"depth_buy_4"`
	DepthBuy5 PriceQuantityOrder `db:"depth_buy_5"`

	// Flattened Depth Sell Fields
	DepthSell1 PriceQuantityOrder `db:"depth_sell_1"`
	DepthSell2 PriceQuantityOrder `db:"depth_sell_2"`
	DepthSell3 PriceQuantityOrder `db:"depth_sell_3"`
	DepthSell4 PriceQuantityOrder `db:"depth_sell_4"`
	DepthSell5 PriceQuantityOrder `db:"depth_sell_5"`

	LastTradeTime      int64   `db:"last_trade_time"`
	Oi                 uint32  `db:"oi"`
	OiDayHigh          uint32  `db:"oi_day_high"`
	OiDayLow           uint32  `db:"oi_day_low"`
	NetChange          float64 `db:"net_change"`
	TargetFile         string  `db:"target_file"`
	TickStoredInDbTime int64   `db:"tick_stored_in_db_time"`
}

type PriceQuantityOrder struct {
	Price    float64
	Quantity uint32
	Orders   uint32
}

type OHLC struct {
	Open  float64 `db:"ohlc_open"`
	High  float64 `db:"ohlc_high"`
	Low   float64 `db:"ohlc_low"`
	Close float64 `db:"ohlc_close"`
}

// Add a method to handle graceful shutdown
func (d *DuckDB) Shutdown() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Stop the export ticker
	d.exportTicker.Stop()

	// Export any remaining data
	if err := d.ExportToParquet(); err != nil {
		d.log.Error("Failed to export data during shutdown", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Optimize tables before closing
	d.tablesMu.RLock()
	for tableName := range d.tables {
		if _, err := d.db.Exec(fmt.Sprintf("ANALYZE %s", tableName)); err != nil {
			d.log.Error("Failed to analyze table", map[string]interface{}{
				"error": err.Error(),
				"table": tableName,
			})
		}
	}
	d.tablesMu.RUnlock()

	// Close database connection
	return d.db.Close()
}

// Add this helper function
func (d *DuckDB) VerifyTableContents(tableName string) error {
	// Check if table exists
	var exists bool
	err := d.db.QueryRow(fmt.Sprintf(`
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = '%s'
        )
    `, tableName)).Scan(&exists)

	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// Get table stats
	var (
		rowCount    int64
		latestTs    int64
		distinctIds int64
	)

	err = d.db.QueryRow(fmt.Sprintf(`
        SELECT 
            COUNT(*) as row_count,
            MAX(timestamp) as latest_ts,
            COUNT(DISTINCT instrument_token) as distinct_ids
        FROM %s
    `, tableName)).Scan(&rowCount, &latestTs, &distinctIds)

	if err != nil {
		return fmt.Errorf("failed to get table stats: %w", err)
	}

	d.log.Info("Table verification", map[string]interface{}{
		"table":        tableName,
		"row_count":    rowCount,
		"latest_time":  time.Unix(latestTs/1000, 0).Format(time.RFC3339),
		"distinct_ids": distinctIds,
		"current_time": time.Now().Format(time.RFC3339),
	})

	// Get sample of recent data
	rows, err := d.db.Query(fmt.Sprintf(`
        SELECT instrument_token, timestamp, last_price
        FROM %s
        ORDER BY timestamp DESC
        LIMIT 5
    `, tableName))
	if err != nil {
		return fmt.Errorf("failed to get recent data: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			token     uint32
			ts        int64
			lastPrice float64
		)
		if err := rows.Scan(&token, &ts, &lastPrice); err != nil {
			continue
		}
		d.log.Info("Recent tick", map[string]interface{}{
			"table":            tableName,
			"instrument_token": token,
			"timestamp":        time.Unix(ts/1000, 0).Format(time.RFC3339),
			"last_price":       lastPrice,
		})
	}

	return nil
}
