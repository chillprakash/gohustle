package deps

// Import all external dependencies used in the project
import (
	// Standard library dependencies
	_ "net/http/cookiejar"

	// Database dependencies
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
	_ "modernc.org/sqlite"

	// Redis dependencies
	_ "github.com/redis/go-redis/v9"

	// Configuration and environment
	_ "github.com/joho/godotenv"

	// Logging
	_ "github.com/sirupsen/logrus"

	// Trading dependencies
	_ "github.com/zerodha/gokiteconnect/v4"

	// Authentication dependencies
	_ "github.com/pquerna/otp/totp"

	// Web Server dependencies
	_ "github.com/gorilla/mux"
	_ "github.com/gorilla/websocket"

	// NATS dependencies
	_ "github.com/nats-io/nats.go"

	// Utility dependencies
	_ "github.com/gocarina/gocsv"
	_ "github.com/google/uuid"
)

// Version constants for dependency management
const (
	// Database versions
	PGXVersion     = "v5.5.1"
	SQLite3Version = "v1.14.28"
	ModernSQLite   = "v1.21.2"
	GoRedisVersion = "v9.7.0"

	// Configuration versions
	GodotenvVersion = "v1.5.1"

	// Logging versions
	LogrusVersion = "v1.9.3"

	// Trading versions
	KiteConnectVersion = "v4.3.5"

	// Authentication versions
	TOTPVersion = "v1.4.0"

	// Web Server versions
	MuxVersion       = "v1.8.1"
	WebsocketVersion = "v1.4.2"

	// NATS versions
	NatsVersion = "v1.41.2"

	// Utility versions
	CSVVersion  = "v0.0.0-20180809181117-b8c38cb1ba36"
	UUIDVersion = "v1.6.0"
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
	Redis struct {
		MaxRetries     int
		PoolSize       int
		MinIdleConns   int
		ConnectTimeout string
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
	NATS struct {
		MaxReconnects   int
		ReconnectWait   string
		ConnectionName  string
		MaxPingsOut     int
		PingInterval    string
		ReconnectBuffer int64
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
	Redis: struct {
		MaxRetries     int
		PoolSize       int
		MinIdleConns   int
		ConnectTimeout string
	}{
		MaxRetries:     3,
		PoolSize:       10,
		MinIdleConns:   2,
		ConnectTimeout: "5s",
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
	NATS: struct {
		MaxReconnects   int
		ReconnectWait   string
		ConnectionName  string
		MaxPingsOut     int
		PingInterval    string
		ReconnectBuffer int64
	}{
		MaxReconnects:   -1, // infinite reconnects
		ReconnectWait:   "2s",
		ConnectionName:  "gohustle",
		MaxPingsOut:     2,
		PingInterval:    "2m",
		ReconnectBuffer: 8 * 1024 * 1024, // 8MB
	},
}

// init function to perform any necessary setup
func init() {
	// Any initialization code for dependencies can go here
}
