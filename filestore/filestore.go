package filestore

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gohustle/logger"
)

const (
	DefaultDataPath = "data"
)

var (
	diskFileStoreInstance *DiskFileStore
	diskFileStoreOnce     sync.Once
	diskFileStoreMu       sync.RWMutex
)

type DiskFileStore struct {
	logger *logger.Logger
}

// GetDiskFileStore returns a singleton instance of DiskFileStore
func GetDiskFileStore() *DiskFileStore {
	diskFileStoreMu.RLock()
	if diskFileStoreInstance != nil {
		diskFileStoreMu.RUnlock()
		return diskFileStoreInstance
	}
	diskFileStoreMu.RUnlock()

	diskFileStoreMu.Lock()
	defer diskFileStoreMu.Unlock()

	diskFileStoreOnce.Do(func() {
		diskFileStoreInstance = &DiskFileStore{
			logger: logger.L(),
		}
	})

	return diskFileStoreInstance
}

// Legacy method for backwards compatibility
func NewDiskFileStore() *DiskFileStore {
	return GetDiskFileStore()
}

func (fs *DiskFileStore) getPath(prefix, date string) string {
	return filepath.Join(DefaultDataPath, fmt.Sprintf("%s_%s.pb.gz", prefix, date))
}

func (fs *DiskFileStore) ensureDir(filePath string) error {
	dir := filepath.Dir(filePath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		fs.logger.Error("Failed to create directory", map[string]interface{}{
			"error": err.Error(),
			"dir":   dir,
		})
		return err
	}
	return nil
}

func (fs *DiskFileStore) SaveGzippedProto(prefix, date string, data []byte) error {
	filePath := fs.getPath(prefix, date)

	if err := fs.ensureDir(filePath); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		fs.logger.Error("Failed to create file", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return err
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	if _, err := gzWriter.Write(data); err != nil {
		fs.logger.Error("Failed to write gzipped data", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return err
	}

	fs.logger.Info("Successfully saved gzipped protobuf data", map[string]interface{}{
		"filePath": filePath,
	})
	return nil
}

// ReadGzippedProto reads a gzipped protobuf file
func (fs *DiskFileStore) ReadGzippedProto(prefix, date string) ([]byte, error) {
	filePath := fs.getPath(prefix, date)
	file, err := os.Open(filePath)
	if err != nil {
		fs.logger.Error("Failed to open file", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		fs.logger.Error("Failed to create gzip reader", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}
	defer gzReader.Close()

	data, err := io.ReadAll(gzReader)
	if err != nil {
		fs.logger.Error("Failed to read data", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}

	fs.logger.Info("Successfully read gzipped protobuf data", map[string]interface{}{
		"filePath": filePath,
	})
	return data, nil
}

func (fs *DiskFileStore) FileExists(prefix, date string) bool {
	filePath := fs.getPath(prefix, date)
	_, err := os.Stat(filePath)
	exists := !os.IsNotExist(err)

	fs.logger.Info("Checked file existence", map[string]interface{}{
		"filePath": filePath,
		"exists":   exists,
	})

	return exists
}

// CreateParquetFile creates a new Parquet file at the specified path
func (fs *DiskFileStore) CreateParquetFile(filePath string) error {
	// Create directories if they don't exist
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		fs.logger.Error("Failed to create directory for Parquet file", map[string]interface{}{
			"error": err.Error(),
			"dir":   dir,
		})
		return err
	}

	// Create an empty file (will be filled by Parquet writer)
	file, err := os.Create(filePath)
	if err != nil {
		fs.logger.Error("Failed to create Parquet file", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return err
	}
	defer file.Close()

	fs.logger.Info("Created Parquet file", map[string]interface{}{
		"filePath": filePath,
	})
	return nil
}
