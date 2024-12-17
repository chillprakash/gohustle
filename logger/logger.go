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

// ANSI color codes
const (
	colorRed   = "\033[31m"
	colorReset = "\033[0m"
)

type Logger struct {
	*log.Logger
	mu    sync.Mutex
	debug bool // Flag to enable/disable debug logging
}

var (
	instance *Logger
	once     sync.Once
)

func init() {
	_, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		// Fallback to UTC if IST loading fails
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
		debug:  false, // Default to false, no debug logs
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

	// Add color for ERROR level
	levelStr := entry.Level
	if level == "ERROR" {
		levelStr = colorRed + level + colorReset
	}

	logMsg := fmt.Sprintf("%s | %s | %s | %s | %s | %s",
		entry.Timestamp,
		levelStr,
		entry.Location,
		entry.Package,
		entry.Function,
		entry.Message,
	)

	if len(props) > 0 {
		propStr := ""
		for k, v := range props {
			if level == "ERROR" {
				propStr += fmt.Sprintf(" %s=%s%v%s", k, colorRed, v, colorReset)
			} else {
				propStr += fmt.Sprintf(" %s=%v", k, v)
			}
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

func (l *Logger) Debug(msg string, props ...map[string]interface{}) {
	if !l.debug {
		return // Do not log if debug is disabled
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	var properties map[string]interface{}
	if len(props) > 0 {
		properties = props[0]
	}

	l.Println(l.formatMessage("DEBUG", msg, properties))
}

// EnableDebug enables debug logging
func (l *Logger) EnableDebug() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debug = true
}

// DisableDebug disables debug logging
func (l *Logger) DisableDebug() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debug = false
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
