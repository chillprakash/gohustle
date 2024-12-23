package filestore

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gohustle/logger"
)

const (
	DefaultDataPath = "data"
)

type FileStore interface {
	SaveGzippedProto(prefix, date string, data []byte) error
	ReadGzippedProto(prefix, date string) ([]byte, error)
}

type DiskFileStore struct{}

func NewDiskFileStore() *DiskFileStore {
	return &DiskFileStore{}
}

func (fs *DiskFileStore) getPath(prefix, date string) string {
	return filepath.Join(DefaultDataPath, fmt.Sprintf("%s_%s.pb.gz", prefix, date))
}

func (fs *DiskFileStore) ensureDir(filePath string) error {
	log := logger.GetLogger()
	dir := filepath.Dir(filePath)

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		log.Error("Failed to create directory", map[string]interface{}{
			"error": err.Error(),
			"dir":   dir,
		})
		return err
	}
	return nil
}

func (fs *DiskFileStore) SaveGzippedProto(prefix, date string, data []byte) error {
	log := logger.GetLogger()
	filePath := fs.getPath(prefix, date)

	if err := fs.ensureDir(filePath); err != nil {
		return err
	}

	file, err := os.Create(filePath)
	if err != nil {
		log.Error("Failed to create file", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return err
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	if _, err := gzWriter.Write(data); err != nil {
		log.Error("Failed to write gzipped data", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return err
	}

	log.Info("Successfully saved gzipped protobuf data", map[string]interface{}{
		"filePath": filePath,
	})
	return nil
}

// ReadGzippedProto reads a gzipped protobuf file
func (fs *DiskFileStore) ReadGzippedProto(prefix, date string) ([]byte, error) {
	log := logger.GetLogger()

	filePath := fs.getPath(prefix, date)
	file, err := os.Open(filePath)
	if err != nil {
		log.Error("Failed to open file", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		log.Error("Failed to create gzip reader", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}
	defer gzReader.Close()

	data, err := io.ReadAll(gzReader)
	if err != nil {
		log.Error("Failed to read data", map[string]interface{}{
			"error":    err.Error(),
			"filePath": filePath,
		})
		return nil, err
	}

	log.Info("Successfully read gzipped protobuf data", map[string]interface{}{
		"filePath": filePath,
	})
	return data, nil
}
