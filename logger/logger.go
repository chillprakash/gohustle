package logger

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type Logger struct {
	*log.Logger
	mu sync.Mutex
}

var (
	instance    *Logger
	once        sync.Once
	istLocation *time.Location
)

func init() {
	var err error
	istLocation, err = time.LoadLocation("Asia/Kolkata")
	if err != nil {
		istLocation = time.UTC
	}
}

type LogEntry struct {
	Timestamp  string
	Level      string
	Location   string
	Package    string
	Function   string
	Message    string
	Properties map[string]interface{}
}

// GetLogger returns a singleton logger instance
func GetLogger() *Logger {
	once.Do(func() {
		instance = setupLogger()
	})
	return instance
}

func setupLogger() *Logger {
	// Create logs directory
	dir, _ := os.Getwd()
	logDir := filepath.Join(dir, "logs")
	os.MkdirAll(logDir, 0755)

	// Open log file with rotation
	logFile := filepath.Join(logDir, "application.log")
	file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("Failed to open log file:", err)
	}

	return &Logger{
		Logger: log.New(file, "", 0),
	}
}

func (l *Logger) formatMessage(level, msg string, props map[string]interface{}) string {
	// Get caller information
	pc, file, line, _ := runtime.Caller(2)
	fn := runtime.FuncForPC(pc)

	// Load IST location
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		// Fallback to UTC if IST loading fails
		ist = time.UTC
	}

	entry := LogEntry{
		Timestamp:  time.Now().In(ist).Format("02-01-06:15:04:05"),
		Level:      level,
		Location:   fmt.Sprintf("%s:%d", filepath.Base(file), line),
		Package:    filepath.Base(filepath.Dir(file)),
		Function:   filepath.Base(fn.Name()),
		Message:    msg,
		Properties: props,
	}

	logMsg := fmt.Sprintf("%s | %s | %s | %s | %s | %s",
		entry.Timestamp,
		entry.Level,
		entry.Location,
		entry.Package,
		entry.Function,
		entry.Message,
	)

	if len(props) > 0 {
		propStr := ""
		for k, v := range props {
			propStr += fmt.Sprintf(" %s=%v", k, v)
		}
		logMsg += " |" + propStr
	}

	return logMsg
}

func (l *Logger) Info(msg string, props ...map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var properties map[string]interface{}
	if len(props) > 0 {
		properties = props[0]
	}

	l.Println(l.formatMessage("INFO", msg, properties))
}

func (l *Logger) Error(msg string, props ...map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var properties map[string]interface{}
	if len(props) > 0 {
		properties = props[0]
	}

	l.Println(l.formatMessage("ERROR", msg, properties))
}

// WithLogger is a struct decorator pattern for Go
type WithLogger struct {
	Logger *Logger
}

func NewWithLogger() *WithLogger {
	return &WithLogger{
		Logger: GetLogger(),
	}
}
