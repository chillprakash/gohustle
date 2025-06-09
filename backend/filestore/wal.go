package filestore

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gohustle/backend/cache"
	"gohustle/backend/logger"
	pb "gohustle/backend/proto"

	"google.golang.org/protobuf/proto"
)

const (
	TickDataDirBase = "data/ticks"
	TickFilePrefix  = "tick"
	TickFileSuffix  = ".dat"
	BufferSize      = 1024 * 1024 // 1MB buffer
)

// TickWriter handles writing to a single index's tick data file
type TickWriter struct {
	indexName  string
	currentDay string
	file       *os.File
	bufWriter  *bufio.Writer
	mu         sync.Mutex
	log        *logger.Logger
}

// TickStore manages multiple tick writers (one per index)
type TickStore struct {
	writers map[string]*TickWriter
	mu      sync.RWMutex
	log     *logger.Logger
	baseDir string
}

var (
	tickStoreInstance *TickStore
	tickStoreOnce     sync.Once
)

// GetTickStore returns the singleton tick store instance
func GetTickStore() *TickStore {
	tickStoreOnce.Do(func() {
		tickStoreInstance = &TickStore{
			writers: make(map[string]*TickWriter),
			log:     logger.L(),
			baseDir: TickDataDirBase,
		}

		// Create base directory if it doesn't exist
		if err := os.MkdirAll(tickStoreInstance.baseDir, 0755); err != nil {
			tickStoreInstance.log.Error("Failed to create tick data directory", map[string]interface{}{
				"error": err.Error(),
				"path":  tickStoreInstance.baseDir,
			})
		}
	})
	return tickStoreInstance
}

// getTickFilePath returns the path for the current tick file
func getTickFilePath(baseDir, indexName, date string) string {
	filename := fmt.Sprintf("%s_%s%s", TickFilePrefix, date, TickFileSuffix)
	return filepath.Join(baseDir, indexName, filename)
}

// newTickWriter creates a new tick writer for an index
func newTickWriter(indexName, baseDir string) (*TickWriter, error) {
	w := &TickWriter{
		indexName:  indexName,
		currentDay: time.Now().Format("2006-01-02"),
		log:        logger.L(),
	}

	// Create directory if it doesn't exist
	dir := filepath.Join(baseDir, indexName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tick directory: %w", err)
	}

	// Open file in append mode
	filePath := getTickFilePath(baseDir, indexName, w.currentDay)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open tick file: %w", err)
	}

	w.file = file
	w.bufWriter = bufio.NewWriterSize(file, BufferSize)

	w.log.Info("Opened tick file", map[string]interface{}{
		"index": indexName,
		"path":  filePath,
	})

	return w, nil
}

// Write writes a tick to the file
func (w *TickWriter) Write(tick *pb.TickData) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if we need to rotate due to day change
	currentDay := time.Now().Format("2006-01-02")
	if currentDay != w.currentDay {
		if err := w.rotate(currentDay); err != nil {
			return fmt.Errorf("failed to rotate file for new day: %w", err)
		}
	}

	// Serialize tick data
	data, err := proto.Marshal(tick)
	if err != nil {
		return fmt.Errorf("failed to marshal tick data: %w", err)
	}

	// Write length and checksum
	checksum := crc32.ChecksumIEEE(data)
	length := uint32(len(data))

	if err := binary.Write(w.bufWriter, binary.BigEndian, length); err != nil {
		return fmt.Errorf("failed to write length: %w", err)
	}
	if err := binary.Write(w.bufWriter, binary.BigEndian, checksum); err != nil {
		return fmt.Errorf("failed to write checksum: %w", err)
	}

	// Write data
	if _, err := w.bufWriter.Write(data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	// Flush periodically
	if err := w.bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}

	return nil
}

// rotate creates a new file for the new day
func (w *TickWriter) rotate(newDay string) error {
	if err := w.Close(); err != nil {
		w.log.Error("Failed to close current file during rotation", map[string]interface{}{
			"error": err.Error(),
			"index": w.indexName,
		})
	}

	w.currentDay = newDay
	filePath := getTickFilePath(filepath.Dir(filepath.Dir(w.file.Name())), w.indexName, newDay)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create new file: %w", err)
	}

	w.file = file
	w.bufWriter = bufio.NewWriterSize(file, BufferSize)

	w.log.Info("Rotated to new file", map[string]interface{}{
		"index": w.indexName,
		"path":  filePath,
	})

	return nil
}

// Close closes the tick writer
func (w *TickWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	if err := w.bufWriter.Flush(); err != nil {
		w.log.Error("Failed to flush buffer during close", map[string]interface{}{
			"error": err.Error(),
			"index": w.indexName,
		})
	}

	if err := w.file.Close(); err != nil {
		w.log.Error("Failed to close file", map[string]interface{}{
			"error": err.Error(),
			"index": w.indexName,
		})
		return err
	}

	w.file = nil
	w.bufWriter = nil
	return nil
}

// WriteTick writes a tick to the appropriate index's file
func (ts *TickStore) WriteTick(tick *pb.TickData) error {
	index, err := ts.GetIndexNameFromTokenFromStore(context.Background(), fmt.Sprintf("%d", tick.InstrumentToken))
	if err != nil {
		return fmt.Errorf("failed to get index name from token: %w", err)
	}

	// Fast path: check if writer exists
	ts.mu.RLock()
	writer, exists := ts.writers[index]
	ts.mu.RUnlock()

	if !exists {
		// Slow path: create new writer
		ts.mu.Lock()
		// Double-check after acquiring write lock
		writer, exists = ts.writers[index]
		if !exists {
			var err error
			writer, err = newTickWriter(index, ts.baseDir)
			if err != nil {
				ts.mu.Unlock()
				return fmt.Errorf("failed to create tick writer for index %s: %w", index, err)
			}
			ts.writers[index] = writer
		}
		ts.mu.Unlock()
	}

	return writer.Write(tick)
}

// GetIndexNameFromToken retrieves the index name for a given instrument token from cache
func (k *TickStore) GetIndexNameFromTokenFromStore(ctx context.Context, instrumentToken string) (string, error) {
	cache := cache.GetInMemoryCacheInstance()

	// Get index name from cache
	indexName, exists := cache.Get(instrumentToken)
	if !exists {
		return "", fmt.Errorf("no index found for instrument token: %s", instrumentToken)
	}

	// Type assert the interface{} to string
	indexNameStr, ok := indexName.(string)
	if !ok {
		return "", fmt.Errorf("invalid cache value type for instrument token: %s", instrumentToken)
	}

	return indexNameStr, nil
}

// Close closes all tick writers
func (ts *TickStore) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	var lastErr error
	for index, writer := range ts.writers {
		if err := writer.Close(); err != nil {
			ts.log.Error("Failed to close tick writer", map[string]interface{}{
				"error": err.Error(),
				"index": index,
			})
			lastErr = err
		}
		delete(ts.writers, index)
	}

	return lastErr
}

// ReadTicks reads all ticks from a file for a given index and date
func (ts *TickStore) ReadTicks(index, date string) ([]*pb.TickData, error) {
	filePath := getTickFilePath(ts.baseDir, index, date)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open tick file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, BufferSize)
	var ticks []*pb.TickData

	for {
		// Read length and checksum
		var length uint32
		var checksum uint32

		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			return nil, fmt.Errorf("failed to read checksum: %w", err)
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}

		// Verify checksum
		if actualChecksum := crc32.ChecksumIEEE(data); actualChecksum != checksum {
			return nil, fmt.Errorf("checksum mismatch: expected %d, got %d", checksum, actualChecksum)
		}

		// Unmarshal tick data
		tick := &pb.TickData{}
		if err := proto.Unmarshal(data, tick); err != nil {
			return nil, fmt.Errorf("failed to unmarshal tick data: %w", err)
		}

		ticks = append(ticks, tick)
	}

	return ticks, nil
}

// StreamTicks reads ticks from WAL and sends them to the provided channel
func (ts *TickStore) StreamTicks(indexName, date string, tickChan chan<- *pb.TickData) error {
	filePath := getTickFilePath(ts.baseDir, indexName, date)

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open tick file: %w", err)
	}
	defer file.Close()

	// Create buffered reader
	reader := bufio.NewReaderSize(file, BufferSize)

	// Read records
	for {
		// Read length
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read length: %w", err)
		}

		// Read checksum
		var checksum uint32
		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			return fmt.Errorf("failed to read checksum: %w", err)
		}

		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return fmt.Errorf("failed to read data: %w", err)
		}

		// Verify checksum
		if crc32.ChecksumIEEE(data) != checksum {
			return fmt.Errorf("checksum mismatch")
		}

		// Unmarshal tick
		tick := &pb.TickData{}
		if err := proto.Unmarshal(data, tick); err != nil {
			return fmt.Errorf("failed to unmarshal tick: %w", err)
		}

		// Send tick to channel
		tickChan <- tick
	}

	return nil
}
