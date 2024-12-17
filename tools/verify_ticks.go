package main

import (
	"fmt"
	"os"
	"path/filepath"

	"gohustle/filestore"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func main() {
	fmt.Println("Starting parquet file verification...")

	// Directory containing parquet files
	ticksDir := "data/ticks"

	// List all parquet files
	files, err := filepath.Glob(filepath.Join(ticksDir, "*.parquet"))
	if err != nil {
		fmt.Printf("Error finding parquet files: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d parquet files\n", len(files))

	// Process each file
	for _, file := range files {
		fmt.Printf("\n=================================")
		fmt.Printf("\nAnalyzing file: %s\n", filepath.Base(file))
		fmt.Printf("=================================\n")
		analyzeParquetFile(file)
	}
}

func analyzeParquetFile(filePath string) {
	// Open parquet file
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer fr.Close()

	// Create parquet reader using the same schema as writer
	pr, err := reader.NewParquetReader(fr, new(filestore.TickParquetSchema), 4)
	if err != nil {
		fmt.Printf("Error creating parquet reader: %v\n", err)
		return
	}
	defer pr.ReadStop()

	// Print basic file info
	fmt.Printf("\nFile Info:")
	fmt.Printf("\nNum Rows: %d", pr.GetNumRows())

	// Print schema info
	fmt.Printf("\n\nSchema Info:")
	fmt.Printf("\nNum Columns: %d", len(pr.SchemaHandler.SchemaElements))

	// Read first row if available
	if pr.GetNumRows() > 0 {
		tick := new(filestore.TickParquetSchema)
		if err := pr.Read(tick); err != nil {
			fmt.Printf("\nError reading first row: %v", err)
			return
		}

		fmt.Printf("\n\nSample Data:")
		fmt.Printf("\nInstrument Token: %d", tick.InstrumentToken)
		fmt.Printf("\nLast Price: %.2f", tick.LastPrice)
		fmt.Printf("\nTimestamp: %d", tick.Timestamp)
		fmt.Printf("\nIs Index: %v", tick.IsIndex)
		fmt.Printf("\nMode: %s", tick.Mode)
	}

	// Print file stats
	fileInfo, err := os.Stat(filePath)
	if err == nil {
		fmt.Printf("\n\nFile Statistics:")
		fmt.Printf("\nFile Size: %.2f MB", float64(fileInfo.Size())/1024/1024)
		if pr.GetNumRows() > 0 {
			fmt.Printf("\nAverage Row Size: %.2f bytes", float64(fileInfo.Size())/float64(pr.GetNumRows()))
		}
	}
	fmt.Printf("\n")
}
