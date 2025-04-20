package deps

// Import all external dependencies used in the project
import (
	// Standard library dependencies
	_ "net/http/cookiejar"

	// Database dependencies
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/pgxpool"

	// Configuration and environment
	_ "github.com/joho/godotenv"

	// Logging
	_ "github.com/sirupsen/logrus"

	// Trading dependencies
	_ "github.com/zerodha/gokiteconnect/v4"

	// Authentication dependencies
	_ "github.com/pquerna/otp/totp"
)

// Version constants for dependency management
const (
	PGXVersion         = "v5.5.1"
	GodotenvVersion    = "v1.5.1"
	LogrusVersion      = "v1.9.3"
	KiteConnectVersion = "v4.0.0"
	TOTPVersion        = "v1.0.0"
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
	Kite struct {
		DefaultTimeout string
		MaxRetries     int
	}
	Auth struct {
		TOTPDigits    int
		TOTPInterval  int
		CookieMaxAge  int
		SessionExpiry string
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
	Kite: struct {
		DefaultTimeout string
		MaxRetries     int
	}{
		DefaultTimeout: "30s",
		MaxRetries:     3,
	},
	Auth: struct {
		TOTPDigits    int
		TOTPInterval  int
		CookieMaxAge  int
		SessionExpiry string
	}{
		TOTPDigits:    6,
		TOTPInterval:  30,
		CookieMaxAge:  86400,
		SessionExpiry: "24h",
	},
}

// init function to perform any necessary setup
func init() {
	// Any initialization code for dependencies can go here
}
