package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Match the schema we used to write
type TickParquetSchema struct {
	// Basic info
	InstrumentToken uint32 `parquet:"name=instrument_token, type=INT32"`
	IsTradable      bool   `parquet:"name=is_tradable, type=BOOLEAN"`
	IsIndex         bool   `parquet:"name=is_index, type=BOOLEAN"`
	Mode            string `parquet:"name=mode, type=BYTE_ARRAY, convertedtype=UTF8"`

	// Timestamps
	Timestamp     int64 `parquet:"name=timestamp, type=INT64"`
	LastTradeTime int64 `parquet:"name=last_trade_time, type=INT64"`

	// Price and quantity
	LastPrice          float64 `parquet:"name=last_price, type=DOUBLE"`
	LastTradedQuantity uint32  `parquet:"name=last_traded_quantity, type=INT32"`
	TotalBuyQuantity   uint32  `parquet:"name=total_buy_quantity, type=INT32"`
	TotalSellQuantity  uint32  `parquet:"name=total_sell_quantity, type=INT32"`
	VolumeTraded       uint32  `parquet:"name=volume_traded, type=INT32"`
	TotalBuy           uint32  `parquet:"name=total_buy, type=INT32"`
	TotalSell          uint32  `parquet:"name=total_sell, type=INT32"`
	AverageTradePrice  float64 `parquet:"name=average_trade_price, type=DOUBLE"`

	// ... rest of the fields matching your schema ...
}

func main() {
	fmt.Println("Starting parquet file verification...")

	// Directory containing parquet files
	ticksDir := "data/ticks"

	// Ensure directory exists
	if _, err := os.Stat(ticksDir); os.IsNotExist(err) {
		fmt.Printf("Directory %s does not exist\n", ticksDir)
		os.Exit(1)
	}

	// List all parquet files
	files, err := filepath.Glob(filepath.Join(ticksDir, "*.parquet"))
	if err != nil {
		fmt.Printf("Error finding parquet files: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d parquet files\n", len(files))

	// Check each file
	for _, file := range files {
		fileInfo, err := os.Stat(file)
		if err != nil {
			fmt.Printf("\nError getting file info for %s: %v\n", filepath.Base(file), err)
			continue
		}

		fmt.Printf("\n=================================")
		fmt.Printf("\nFile: %s", filepath.Base(file))
		fmt.Printf("\nSize: %d bytes", fileInfo.Size())
		fmt.Printf("\nModified: %s", fileInfo.ModTime().Format("2006-01-02 15:04:05"))
		fmt.Printf("\n=================================\n")

		// Try to read file content
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Printf("Error reading file: %v\n", err)
			continue
		}

		if len(data) == 0 {
			fmt.Printf("File is empty\n")
			continue
		}

		fmt.Printf("File contains %d bytes of data\n", len(data))
		analyzeParquetFile(file)
	}
}

func analyzeParquetFile(filePath string) {
	// Open file
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer fr.Close()

	// Create parquet reader
	pr, err := reader.NewParquetReader(fr, new(TickParquetSchema), 4)
	if err != nil {
		fmt.Printf("Error creating parquet reader: %v\n", err)
		return
	}
	defer pr.ReadStop()

	numRows := int(pr.GetNumRows())
	fmt.Printf("Total rows: %d\n", numRows)

	if numRows == 0 {
		return
	}

	// Read first and last records
	var firstTick, lastTick TickParquetSchema

	if err := pr.Read(&firstTick); err != nil {
		fmt.Printf("Error reading first tick: %v\n", err)
		return
	}

	// Skip to last record
	if numRows > 1 {
		pr.SkipRows(int64(numRows - 2))
		if err := pr.Read(&lastTick); err != nil {
			fmt.Printf("Error reading last tick: %v\n", err)
			return
		}
	} else {
		lastTick = firstTick
	}

	// Print summary
	fmt.Printf("\nFile Summary:")
	fmt.Printf("\nTime Range: %s to %s",
		time.Unix(firstTick.Timestamp, 0).Format("2006-01-02 15:04:05"),
		time.Unix(lastTick.Timestamp, 0).Format("2006-01-02 15:04:05"))
	fmt.Printf("\nDuration: %s",
		time.Unix(lastTick.Timestamp, 0).Sub(time.Unix(firstTick.Timestamp, 0)))

	// Print sample data
	fmt.Printf("\n\nSample Data (First Tick):")
	fmt.Printf("\nInstrument Token: %d", firstTick.InstrumentToken)
	fmt.Printf("\nIs Index: %v", firstTick.IsIndex)
	fmt.Printf("\nLast Price: %.2f", firstTick.LastPrice)
	fmt.Printf("\nVolume: %d", firstTick.VolumeTraded)

	// Get file info for size
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		return
	}

	// Print statistics using fileInfo.Size()
	fileSize := fileInfo.Size()
	fmt.Printf("\n\nFile Statistics:")
	fmt.Printf("\nFile Size: %.2f MB", float64(fileSize)/1024/1024)
	fmt.Printf("\nAverage Row Size: %.2f bytes", float64(fileSize)/float64(numRows))
	fmt.Printf("\n")
}
