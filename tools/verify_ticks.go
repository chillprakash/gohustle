package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	// Get file info
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Printf("Error getting file info: %v\n", err)
		return
	}

	fmt.Printf("\nFile Statistics:")
	fmt.Printf("\nFile Size: %.2f MB", float64(fileInfo.Size())/1024/1024)

	// Try using parquet-tools to inspect the file
	cmd := exec.Command("parquet-tools", "schema", filePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("\nError reading schema with parquet-tools: %v\n", err)
		fmt.Printf("Output: %s\n", string(output))
	} else {
		fmt.Printf("\n\nSchema from parquet-tools:\n%s", string(output))
	}

	// Try reading first few rows
	cmd = exec.Command("parquet-tools", "head", "-n", "1", filePath)
	output, err = cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("\nError reading data with parquet-tools: %v\n", err)
		fmt.Printf("Output: %s\n", string(output))
	} else {
		fmt.Printf("\n\nSample Data:\n%s", string(output))
	}

	fmt.Printf("\n")
}
