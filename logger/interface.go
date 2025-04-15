package logger

import (
	"context"
)

// LoggerInterface defines the contract for logging operations
type LoggerInterface interface {
	// Context-aware logging methods
	WithContext(ctx context.Context) LoggerInterface

	// Standard logging methods
	Info(msg string, fields map[string]interface{})
	Error(msg string, fields map[string]interface{})
	Debug(msg string, fields map[string]interface{})
	Fatal(msg string, fields map[string]interface{})
}

// ContextKey type for storing context values
type contextKey string

const (
	// Keys for context values
	RequestIDKey contextKey = "request_id"
	UserIDKey    contextKey = "user_id"
	SessionKey   contextKey = "session_id"
)

// contextLogger wraps a logger with context
type contextLogger struct {
	logger  LoggerInterface
	context context.Context
}

func (c *contextLogger) WithContext(ctx context.Context) LoggerInterface {
	return &contextLogger{
		logger:  c.logger,
		context: ctx,
	}
}

// Helper to merge context fields with provided fields
func (c *contextLogger) mergeContextFields(fields map[string]interface{}) map[string]interface{} {
	if fields == nil {
		fields = make(map[string]interface{})
	}

	// Add context values to fields if they exist
	if reqID := c.context.Value(RequestIDKey); reqID != nil {
		fields["request_id"] = reqID
	}
	if userID := c.context.Value(UserIDKey); userID != nil {
		fields["user_id"] = userID
	}
	if sessionID := c.context.Value(SessionKey); sessionID != nil {
		fields["session_id"] = sessionID
	}

	return fields
}

// Implement logging methods with context
func (c *contextLogger) Info(msg string, fields map[string]interface{}) {
	c.logger.Info(msg, c.mergeContextFields(fields))
}

func (c *contextLogger) Error(msg string, fields map[string]interface{}) {
	c.logger.Error(msg, c.mergeContextFields(fields))
}

func (c *contextLogger) Debug(msg string, fields map[string]interface{}) {
	c.logger.Debug(msg, c.mergeContextFields(fields))
}

func (c *contextLogger) Fatal(msg string, fields map[string]interface{}) {
	c.logger.Fatal(msg, c.mergeContextFields(fields))
}
