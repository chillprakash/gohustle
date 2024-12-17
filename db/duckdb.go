package db

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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
}

var (
	instance *DuckDB
	once     sync.Once
)

// GetDuckDB returns singleton instance
func GetDuckDB() *DuckDB {
	once.Do(func() {
		var err error
		instance, err = newDuckDB()
		if err != nil {
			panic(fmt.Sprintf("Failed to initialize DuckDB: %v", err))
		}
	})
	return instance
}

func newDuckDB() (*DuckDB, error) {
	log := logger.GetLogger()

	// Create data directory if not exists
	dataDir := "data/db"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open DuckDB connection
	db, err := sql.Open("duckdb", filepath.Join(dataDir, "ticks.duckdb"))
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Initialize tables
	if err := initializeTables(db); err != nil {
		db.Close()
		return nil, err
	}

	duck := &DuckDB{
		db:           sqlx.NewDb(db, "duckdb"),
		exportPath:   "data/ticks",
		exportTicker: time.NewTicker(1 * time.Hour),
		log:          log,
	}

	// Start export routine
	go duck.runExporter()

	log.Info("DuckDB initialized successfully", nil)
	return duck, nil
}

func initializeTables(db *sql.DB) error {
	// Print the number of columns for debugging
	log := logger.GetLogger()
	log.Info("Creating table schema", nil)

	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS ticks (
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
            change_percent DOUBLE,
            last_trade_time BIGINT,
            oi BIGINT,
            oi_day_high BIGINT,
            oi_day_low BIGINT,
            net_change DOUBLE,
            
            -- Metadata
            target_file VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Indexes
            PRIMARY KEY (instrument_token, timestamp)
        );
    `)

	// Count columns for verification
	var columnCount int
	err = db.QueryRow(`
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_name = 'ticks'
    `).Scan(&columnCount)

	if err == nil {
		log.Info("Table schema created", map[string]interface{}{
			"column_count": columnCount,
		})
	}

	return err
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
	d.mu.Lock()
	defer d.mu.Unlock()

	d.log.Debug("Writing tick to DuckDB", map[string]interface{}{
		"instrument_token": tick.InstrumentToken,
		"timestamp":        tick.Timestamp,
	})

	// Convert proto.TickData to models.TickData
	tickData := TickData{
		InstrumentToken:    int64(tick.InstrumentToken),
		Timestamp:          tick.Timestamp * 1000,
		IsTradable:         tick.IsTradable,
		IsIndex:            tick.IsIndex,
		Mode:               tick.Mode,
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: int64(tick.LastTradedQuantity),
		AverageTradePrice:  tick.AverageTradePrice,
		VolumeTraded:       int64(tick.VolumeTraded),
		TotalBuyQuantity:   int64(tick.TotalBuyQuantity),
		TotalSellQuantity:  int64(tick.TotalSellQuantity),
		TotalBuy:           int64(tick.TotalBuy),
		TotalSell:          int64(tick.TotalSell),
		OhlcOpen:           tick.Ohlc.Open,
		OhlcHigh:           tick.Ohlc.High,
		OhlcLow:            tick.Ohlc.Low,
		OhlcClose:          tick.Ohlc.Close,
		DepthBuyPrice1:     getDepthValue(tick.Depth.Buy, 0, "price"),
		DepthBuyQuantity1:  int64(getDepthValue(tick.Depth.Buy, 0, "quantity")),
		DepthBuyOrders1:    int64(getDepthValue(tick.Depth.Buy, 0, "orders")),
		DepthBuyPrice2:     getDepthValue(tick.Depth.Buy, 1, "price"),
		DepthBuyQuantity2:  int64(getDepthValue(tick.Depth.Buy, 1, "quantity")),
		DepthBuyOrders2:    int64(getDepthValue(tick.Depth.Buy, 1, "orders")),
		DepthBuyPrice3:     getDepthValue(tick.Depth.Buy, 2, "price"),
		DepthBuyQuantity3:  int64(getDepthValue(tick.Depth.Buy, 2, "quantity")),
		DepthBuyOrders3:    int64(getDepthValue(tick.Depth.Buy, 2, "orders")),
		DepthBuyPrice4:     getDepthValue(tick.Depth.Buy, 3, "price"),
		DepthBuyQuantity4:  int64(getDepthValue(tick.Depth.Buy, 3, "quantity")),
		DepthBuyOrders4:    int64(getDepthValue(tick.Depth.Buy, 3, "orders")),
		DepthBuyPrice5:     getDepthValue(tick.Depth.Buy, 4, "price"),
		DepthBuyQuantity5:  int64(getDepthValue(tick.Depth.Buy, 4, "quantity")),
		DepthBuyOrders5:    int64(getDepthValue(tick.Depth.Buy, 4, "orders")),
		DepthSellPrice1:    getDepthValue(tick.Depth.Sell, 0, "price"),
		DepthSellQuantity1: int64(getDepthValue(tick.Depth.Sell, 0, "quantity")),
		DepthSellOrders1:   int64(getDepthValue(tick.Depth.Sell, 0, "orders")),
		DepthSellPrice2:    getDepthValue(tick.Depth.Sell, 1, "price"),
		DepthSellQuantity2: int64(getDepthValue(tick.Depth.Sell, 1, "quantity")),
		DepthSellOrders2:   int64(getDepthValue(tick.Depth.Sell, 1, "orders")),
		DepthSellPrice3:    getDepthValue(tick.Depth.Sell, 2, "price"),
		DepthSellQuantity3: int64(getDepthValue(tick.Depth.Sell, 2, "quantity")),
		DepthSellOrders3:   int64(getDepthValue(tick.Depth.Sell, 2, "orders")),
		DepthSellPrice4:    getDepthValue(tick.Depth.Sell, 3, "price"),
		DepthSellQuantity4: int64(getDepthValue(tick.Depth.Sell, 3, "quantity")),
		DepthSellOrders4:   int64(getDepthValue(tick.Depth.Sell, 3, "orders")),
		DepthSellPrice5:    getDepthValue(tick.Depth.Sell, 4, "price"),
		DepthSellQuantity5: int64(getDepthValue(tick.Depth.Sell, 4, "quantity")),
		DepthSellOrders5:   int64(getDepthValue(tick.Depth.Sell, 4, "orders")),
		ChangePercent:      tick.ChangePercent,
		LastTradeTime:      tick.LastTradeTime,
		Oi:                 int64(tick.Oi),
		OiDayHigh:          int64(tick.OiDayHigh),
		OiDayLow:           int64(tick.OiDayLow),
		NetChange:          tick.NetChange,
		TargetFile:         tick.TargetFile,
	}

	// Use sqlx.NamedExec to insert using struct
	_, err := d.db.NamedExec(`
        INSERT INTO ticks (
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
            change_percent, last_trade_time, oi, oi_day_high, oi_day_low,
            net_change, target_file, created_at
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
            :change_percent, :last_trade_time, :oi, :oi_day_high, :oi_day_low,
            :net_change, :target_file, CURRENT_TIMESTAMP
        )`, tickData)

	if err != nil {
		d.log.Error("Failed to insert tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return fmt.Errorf("failed to insert tick: %w", err)
	}

	return nil
}

// WriteTicks writes multiple ticks in a transaction
func (d *DuckDB) WriteTicks(tick *proto.TickData) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return err
	}

	// Use the same SQL as WriteTick
	stmt, err := tx.Prepare(`
        INSERT INTO ticks (
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
            change_percent, last_trade_time, oi, oi_day_high, oi_day_low,
            net_change, target_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                 ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	result, err := stmt.Exec(
		// Basic info
		tick.InstrumentToken, tick.Timestamp, tick.IsTradable, tick.IsIndex, tick.Mode,
		// Price info
		tick.LastPrice, tick.LastTradedQuantity, tick.AverageTradePrice,
		tick.VolumeTraded, tick.TotalBuyQuantity, tick.TotalSellQuantity,
		tick.TotalBuy, tick.TotalSell,
		// OHLC
		tick.Ohlc.Open, tick.Ohlc.High, tick.Ohlc.Low, tick.Ohlc.Close,
		// Market Depth - Buy
		getDepthValue(tick.Depth.Buy, 0, "price"), getDepthValue(tick.Depth.Buy, 0, "quantity"), getDepthValue(tick.Depth.Buy, 0, "orders"),
		getDepthValue(tick.Depth.Buy, 1, "price"), getDepthValue(tick.Depth.Buy, 1, "quantity"), getDepthValue(tick.Depth.Buy, 1, "orders"),
		getDepthValue(tick.Depth.Buy, 2, "price"), getDepthValue(tick.Depth.Buy, 2, "quantity"), getDepthValue(tick.Depth.Buy, 2, "orders"),
		getDepthValue(tick.Depth.Buy, 3, "price"), getDepthValue(tick.Depth.Buy, 3, "quantity"), getDepthValue(tick.Depth.Buy, 3, "orders"),
		getDepthValue(tick.Depth.Buy, 4, "price"), getDepthValue(tick.Depth.Buy, 4, "quantity"), getDepthValue(tick.Depth.Buy, 4, "orders"),
		// Market Depth - Sell
		getDepthValue(tick.Depth.Sell, 0, "price"), getDepthValue(tick.Depth.Sell, 0, "quantity"), getDepthValue(tick.Depth.Sell, 0, "orders"),
		getDepthValue(tick.Depth.Sell, 1, "price"), getDepthValue(tick.Depth.Sell, 1, "quantity"), getDepthValue(tick.Depth.Sell, 1, "orders"),
		getDepthValue(tick.Depth.Sell, 2, "price"), getDepthValue(tick.Depth.Sell, 2, "quantity"), getDepthValue(tick.Depth.Sell, 2, "orders"),
		getDepthValue(tick.Depth.Sell, 3, "price"), getDepthValue(tick.Depth.Sell, 3, "quantity"), getDepthValue(tick.Depth.Sell, 3, "orders"),
		getDepthValue(tick.Depth.Sell, 4, "price"), getDepthValue(tick.Depth.Sell, 4, "quantity"), getDepthValue(tick.Depth.Sell, 4, "orders"),
		// Additional fields
		tick.ChangePercent, tick.LastTradeTime, tick.Oi, tick.OiDayHigh, tick.OiDayLow,
		tick.NetChange, tick.TargetFile,
	)
	d.log.Debug("Inserted tick", map[string]interface{}{
		"result": result,
	})
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to insert tick: %w", err)
	}

	return tx.Commit()
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
	d.mu.RLock()
	defer d.mu.RUnlock()

	filename := fmt.Sprintf("ticks_%s.parquet",
		time.Now().Format("2006-01-02_15"))

	_, err := d.db.Exec(fmt.Sprintf(`
        COPY (
            SELECT * FROM ticks 
        ) TO '%s' (FORMAT 'parquet')`,
		filepath.Join(d.exportPath, filename)))

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
func getDepthValue(depth []*proto.TickData_DepthItem, index int, field string) float64 {
	// Check for nil or out of bounds
	if depth == nil || index >= len(depth) || depth[index] == nil {
		return 0.0
	}

	// Return appropriate field value
	switch field {
	case "price":
		return depth[index].Price
	case "quantity":
		return float64(depth[index].Quantity)
	case "orders":
		return float64(depth[index].Orders)
	default:
		return 0.0
	}
}

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
	InstrumentToken    int64   `db:"instrument_token"`
	Timestamp          int64   `db:"timestamp"`
	IsTradable         bool    `db:"is_tradable"`
	IsIndex            bool    `db:"is_index"`
	Mode               string  `db:"mode"`
	LastPrice          float64 `db:"last_price"`
	LastTradedQuantity int64   `db:"last_traded_quantity"`
	AverageTradePrice  float64 `db:"average_trade_price"`
	VolumeTraded       int64   `db:"volume_traded"`
	TotalBuyQuantity   int64   `db:"total_buy_quantity"`
	TotalSellQuantity  int64   `db:"total_sell_quantity"`
	TotalBuy           int64   `db:"total_buy"`
	TotalSell          int64   `db:"total_sell"`
	OhlcOpen           float64 `db:"ohlc_open"`
	OhlcHigh           float64 `db:"ohlc_high"`
	OhlcLow            float64 `db:"ohlc_low"`
	OhlcClose          float64 `db:"ohlc_close"`
	DepthBuyPrice1     float64 `db:"depth_buy_price_1"`
	DepthBuyQuantity1  int64   `db:"depth_buy_quantity_1"`
	DepthBuyOrders1    int64   `db:"depth_buy_orders_1"`
	DepthBuyPrice2     float64 `db:"depth_buy_price_2"`
	DepthBuyQuantity2  int64   `db:"depth_buy_quantity_2"`
	DepthBuyOrders2    int64   `db:"depth_buy_orders_2"`
	DepthBuyPrice3     float64 `db:"depth_buy_price_3"`
	DepthBuyQuantity3  int64   `db:"depth_buy_quantity_3"`
	DepthBuyOrders3    int64   `db:"depth_buy_orders_3"`
	DepthBuyPrice4     float64 `db:"depth_buy_price_4"`
	DepthBuyQuantity4  int64   `db:"depth_buy_quantity_4"`
	DepthBuyOrders4    int64   `db:"depth_buy_orders_4"`
	DepthBuyPrice5     float64 `db:"depth_buy_price_5"`
	DepthBuyQuantity5  int64   `db:"depth_buy_quantity_5"`
	DepthBuyOrders5    int64   `db:"depth_buy_orders_5"`
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
	ChangePercent      float64 `db:"change_percent"`
	LastTradeTime      int64   `db:"last_trade_time"`
	Oi                 int64   `db:"oi"`
	OiDayHigh          int64   `db:"oi_day_high"`
	OiDayLow           int64   `db:"oi_day_low"`
	NetChange          float64 `db:"net_change"`
	TargetFile         string  `db:"target_file"`
	CreatedAt          string  `db:"created_at"` // Assuming this is a timestamp
}
