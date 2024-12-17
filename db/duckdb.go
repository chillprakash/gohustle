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
            last_trade_time BIGINT,
            oi BIGINT,
            oi_day_high BIGINT,
            oi_day_low BIGINT,
            net_change DOUBLE,
            
            -- Metadata
            target_file VARCHAR,
            tick_received_time BIGINT,
            tick_stored_in_db_time BIGINT,
            
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
		Ohlc:               OHLC{Open: tick.Ohlc.Open, High: tick.Ohlc.High, Low: tick.Ohlc.Low, Close: tick.Ohlc.Close},
		Depth:              MarketDepth{Buy: convertDepthItems(tick.Depth.Buy[:]), Sell: convertDepthItems(tick.Depth.Sell[:])},
		LastTradeTime:      tick.LastTradeTime,
		Oi:                 uint32(tick.Oi),
		OiDayHigh:          uint32(tick.OiDayHigh),
		OiDayLow:           uint32(tick.OiDayLow),
		NetChange:          tick.NetChange,
		TargetFile:         tick.TargetFile,
		TickStoredInDbTime: time.Now().Unix(),
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
            net_change, target_file, tick_received_time, tick_stored_in_db_time
        ) VALUES (
            :instrument_token, :timestamp, :is_tradable, :is_index, :mode,
            :last_price, :last_traded_quantity, :average_trade_price,
            :volume_traded, :total_buy_quantity, :total_sell_quantity,
            :total_buy, :total_sell,
            :ohlc.ohlc_open, :ohlc.ohlc_high, :ohlc.ohlc_low, :ohlc.ohlc_close,
            :depth.buy[0].price, :depth.buy[0].quantity, :depth.buy[0].orders,
            :depth.buy[1].price, :depth.buy[1].quantity, :depth.buy[1].orders,
            :depth.buy[2].price, :depth.buy[2].quantity, :depth.buy[2].orders,
            :depth.buy[3].price, :depth.buy[3].quantity, :depth.buy[3].orders,
            :depth.buy[4].price, :depth.buy[4].quantity, :depth.buy[4].orders,
            :depth.buy[5].price, :depth.buy[5].quantity, :depth.buy[5].orders,
            :depth.sell[0].price, :depth.sell[0].quantity, :depth.sell[0].orders,
            :depth.sell[1].price, :depth.sell[1].quantity, :depth.sell[1].orders,
            :depth.sell[2].price, :depth.sell[2].quantity, :depth.sell[2].orders,
            :depth.sell[3].price, :depth.sell[3].quantity, :depth.sell[3].orders,
            :depth.sell[4].price, :depth.sell[4].quantity, :depth.sell[4].orders,
            :depth.sell[5].price, :depth.sell[5].quantity, :depth.sell[5].orders,
            :last_trade_time, :oi, :oi_day_high, :oi_day_low,
            :net_change, :target_file, :tick_received_time, :tick_stored_in_db_time
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
	InstrumentToken    uint32      `db:"instrument_token"`
	IsTradable         bool        `db:"is_tradable"`
	IsIndex            bool        `db:"is_index"`
	Mode               string      `db:"mode"`
	Timestamp          int64       `db:"timestamp"`
	LastTradeTime      int64       `db:"last_trade_time"`
	LastPrice          float64     `db:"last_price"`
	LastTradedQuantity uint32      `db:"last_traded_quantity"`
	TotalBuyQuantity   uint32      `db:"total_buy_quantity"`
	TotalSellQuantity  uint32      `db:"total_sell_quantity"`
	VolumeTraded       uint32      `db:"volume_traded"`
	TotalBuy           uint32      `db:"total_buy"`
	TotalSell          uint32      `db:"total_sell"`
	AverageTradePrice  float64     `db:"average_trade_price"`
	Oi                 uint32      `db:"oi"`
	OiDayHigh          uint32      `db:"oi_day_high"`
	OiDayLow           uint32      `db:"oi_day_low"`
	NetChange          float64     `db:"net_change"`
	Ohlc               OHLC        `db:"ohlc"`
	Depth              MarketDepth `db:"depth"`
	TargetFile         string      `db:"target_file"`
	TickReceivedTime   int64       `db:"tick_received_time"`
	TickStoredInDbTime int64       `db:"tick_stored_in_db_time"`
}

type OHLC struct {
	Open  float64 `db:"ohlc_open"`
	High  float64 `db:"ohlc_high"`
	Low   float64 `db:"ohlc_low"`
	Close float64 `db:"ohlc_close"`
}

type DepthItem struct {
	Price    float64 `db:"price"`
	Quantity uint32  `db:"quantity"`
	Orders   uint32  `db:"orders"`
}

type MarketDepth struct {
	Buy  []DepthItem `db:"buy"`
	Sell []DepthItem `db:"sell"`
}

func convertDepthItems(depthItems []*proto.TickData_DepthItem) []DepthItem {
	items := make([]DepthItem, len(depthItems))
	for i, item := range depthItems {
		items[i] = DepthItem{Price: item.Price, Quantity: item.Quantity, Orders: item.Orders}
	}
	return items
}
