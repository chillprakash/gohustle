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
	log := logger.GetLogger()
	log.Info("Creating table schema", nil)

	// Modified SQL to use DuckDB's supported syntax
	_, err := db.Exec(`
        CREATE TABLE IF NOT EXISTS ticks (
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
        );
    `)
	if err != nil {
		log.Error("Failed to create table", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Create index in a separate statement
	_, err = db.Exec(`
        CREATE INDEX IF NOT EXISTS idx_token_time ON ticks (instrument_token, timestamp);
    `)
	if err != nil {
		log.Error("Failed to create index", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	// Verify table creation
	var tableExists bool
	err = db.QueryRow(`
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.tables 
            WHERE table_name = 'ticks'
        );
    `).Scan(&tableExists)

	if err != nil {
		log.Error("Failed to verify table creation", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}

	if tableExists {
		log.Info("Table 'ticks' created successfully", nil)
	} else {
		log.Error("Table 'ticks' was not created", nil)
		return fmt.Errorf("table 'ticks' was not created")
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
	d.mu.Lock()
	defer d.mu.Unlock()

	d.log.Debug("Writing tick to DuckDB", map[string]interface{}{
		"instrument_token": tick.InstrumentToken,
		"timestamp":        tick.Timestamp,
	})

	tickData := TickData{
		InstrumentToken:    uint32(tick.InstrumentToken),
		Timestamp:          tick.Timestamp * 1000,
		IsTradable:         tick.IsTradable,
		IsIndex:            tick.IsIndex,
		Mode:               tick.Mode,
		LastPrice:          tick.LastPrice,
		LastTradedQuantity: uint32(tick.LastTradedQuantity),
		AverageTradePrice:  tick.AverageTradePrice,
		VolumeTraded:       uint32(tick.VolumeTraded),
		TotalBuyQuantity:   uint32(tick.TotalBuyQuantity),
		TotalSellQuantity:  uint32(tick.TotalSellQuantity),
		TotalBuy:           uint32(tick.TotalBuy),
		TotalSell:          uint32(tick.TotalSell),
		OHLC:               OHLC{Open: tick.Ohlc.Open, High: tick.Ohlc.High, Low: tick.Ohlc.Low, Close: tick.Ohlc.Close},
		LastTradeTime:      tick.LastTradeTime,
		Oi:                 uint32(tick.Oi),
		OiDayHigh:          uint32(tick.OiDayHigh),
		OiDayLow:           uint32(tick.OiDayLow),
		NetChange:          tick.NetChange,
		TargetFile:         tick.TargetFile,
		TickStoredInDbTime: time.Now().Unix(),
	}

	// Flatten Depth Buy
	tickData.DepthBuy1 = PriceQuantityOrder{
		Price:    tick.Depth.Buy[0].Price,
		Quantity: tick.Depth.Buy[0].Quantity,
		Orders:   tick.Depth.Buy[0].Orders,
	}
	tickData.DepthBuy2 = PriceQuantityOrder{
		Price:    tick.Depth.Buy[1].Price,
		Quantity: tick.Depth.Buy[1].Quantity,
		Orders:   tick.Depth.Buy[1].Orders,
	}
	tickData.DepthBuy3 = PriceQuantityOrder{
		Price:    tick.Depth.Buy[2].Price,
		Quantity: tick.Depth.Buy[2].Quantity,
		Orders:   tick.Depth.Buy[2].Orders,
	}
	tickData.DepthBuy4 = PriceQuantityOrder{
		Price:    tick.Depth.Buy[3].Price,
		Quantity: tick.Depth.Buy[3].Quantity,
		Orders:   tick.Depth.Buy[3].Orders,
	}
	tickData.DepthBuy5 = PriceQuantityOrder{
		Price:    tick.Depth.Buy[4].Price,
		Quantity: tick.Depth.Buy[4].Quantity,
		Orders:   tick.Depth.Buy[4].Orders,
	}

	// Flatten Depth Sell
	tickData.DepthSell1 = PriceQuantityOrder{
		Price:    tick.Depth.Sell[0].Price,
		Quantity: tick.Depth.Sell[0].Quantity,
		Orders:   tick.Depth.Sell[0].Orders,
	}
	tickData.DepthSell2 = PriceQuantityOrder{
		Price:    tick.Depth.Sell[1].Price,
		Quantity: tick.Depth.Sell[1].Quantity,
		Orders:   tick.Depth.Sell[1].Orders,
	}
	tickData.DepthSell3 = PriceQuantityOrder{
		Price:    tick.Depth.Sell[2].Price,
		Quantity: tick.Depth.Sell[2].Quantity,
		Orders:   tick.Depth.Sell[2].Orders,
	}
	tickData.DepthSell4 = PriceQuantityOrder{
		Price:    tick.Depth.Sell[3].Price,
		Quantity: tick.Depth.Sell[3].Quantity,
		Orders:   tick.Depth.Sell[3].Orders,
	}
	tickData.DepthSell5 = PriceQuantityOrder{
		Price:    tick.Depth.Sell[4].Price,
		Quantity: tick.Depth.Sell[4].Quantity,
		Orders:   tick.Depth.Sell[4].Orders,
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
			last_trade_time, oi, oi_day_high, oi_day_low,
			net_change, target_file, tick_stored_in_db_time
		) VALUES (
			:instrument_token, :timestamp, :is_tradable, :is_index, :mode,
			:last_price, :last_traded_quantity, :average_trade_price,
			:volume_traded, :total_buy_quantity, :total_sell_quantity,
			:total_buy, :total_sell,
			:ohlc.ohlc_open, :ohlc.ohlc_high, :ohlc.ohlc_low, :ohlc.ohlc_close,
			:depth_buy_1.price, :depth_buy_1.quantity, :depth_buy_1.orders,
			:depth_buy_2.price, :depth_buy_2.quantity, :depth_buy_2.orders,
			:depth_buy_3.price, :depth_buy_3.quantity, :depth_buy_3.orders,
			:depth_buy_4.price, :depth_buy_4.quantity, :depth_buy_4.orders,
			:depth_buy_5.price, :depth_buy_5.quantity, :depth_buy_5.orders,
			:depth_sell_1.price, :depth_sell_1.quantity, :depth_sell_1.orders,
			:depth_sell_2.price, :depth_sell_2.quantity, :depth_sell_2.orders,
			:depth_sell_3.price, :depth_sell_3.quantity, :depth_sell_3.orders,
			:depth_sell_4.price, :depth_sell_4.quantity, :depth_sell_4.orders,
			:depth_sell_5.price, :depth_sell_5.quantity, :depth_sell_5.orders,
			:last_trade_time, :oi, :oi_day_high, :oi_day_low,
			:net_change, :target_file, :tick_stored_in_db_time
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
