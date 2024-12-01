package main

// Import all external dependencies used in the project
import (
	// Database dependencies
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/pgxpool"

	// Configuration and environment
	_ "github.com/joho/godotenv"

	// Logging
	_ "github.com/sirupsen/logrus"
)

// Version constants for dependency management
const (
	PGXVersion      = "v5.5.1"
	GodotenvVersion = "v1.5.1"
	LogrusVersion   = "v1.9.3"
)

// DependencyConfig holds configuration for external dependencies
var DependencyConfig = struct {
	Postgres struct {
		MaxConnLifetime string
		MaxConnIdleTime string
		DefaultPoolSize int
		DefaultMaxConns int32
		DefaultMinConns int32
	}
}{
	Postgres: struct {
		MaxConnLifetime string
		MaxConnIdleTime string
		DefaultPoolSize int
		DefaultMaxConns int32
		DefaultMinConns int32
	}{
		MaxConnLifetime: "1h",
		MaxConnIdleTime: "30m",
		DefaultPoolSize: 10,
		DefaultMaxConns: 100,
		DefaultMinConns: 2,
	},
}

// init function to perform any necessary setup
func init() {
	// Any initialization code for dependencies can go here
}
