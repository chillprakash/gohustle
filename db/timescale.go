package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/proto"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v5"
)

var (
	instance *TimescaleDB
	once     sync.Once
	mu       sync.RWMutex
)

type TimescaleDB struct {
	conn   *pgx.Conn
	config *config.TimescaleConfig
}

// NewTimescaleDB creates or returns existing TimescaleDB instance
func NewTimescaleDB() (*TimescaleDB, error) {
	mu.Lock()
	defer mu.Unlock()

	var initErr error
	once.Do(func() {
		// Get config directly
		cfg := config.GetConfig()
		instance = &TimescaleDB{}
		initErr = instance.initialize(&cfg.Timescale)
	})

	if initErr != nil {
		return nil, fmt.Errorf("failed to initialize TimescaleDB: %w", initErr)
	}

	return instance, nil
}

// Close closes the database connection
func (db *TimescaleDB) Close() {
	if db.conn != nil {
		db.conn.Close(context.Background())
		logger.GetLogger().Info("Database connection closed", nil)
	}
}

// initialize is internal initialization method
func (db *TimescaleDB) initialize(cfg *config.TimescaleConfig) error {
	log := logger.GetLogger()
	ctx := context.Background()

	// Create connection string for PgBouncer
	connString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable pool_mode=%s",
		cfg.Host, cfg.PgBouncerPort, cfg.User, cfg.Password, cfg.DBName, cfg.PoolMode,
	)

	// Establish single connection through PgBouncer
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Configure connection parameters
	if _, err := conn.Exec(ctx, "SET application_name = 'tick_writer'"); err != nil {
		return fmt.Errorf("failed to set application name: %w", err)
	}

	db.conn = conn
	db.config = cfg

	log.Info("Connected to database via PgBouncer", map[string]interface{}{
		"host":     cfg.Host,
		"pgb_port": cfg.PgBouncerPort,
		"db":       cfg.DBName,
	})

	return nil
}

// StoreTick writes a single tick to the database as protobuf
func (db *TimescaleDB) StoreTick(tick *proto.TickData) error {
	ctx := context.Background()

	// Serialize the complete proto
	protoData, err := proto.Marshal(tick)
	if err != nil {
		logger.GetLogger().Error("Failed to marshal tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
		})
		return err
	}

	// Insert the binary proto data
	_, err = db.conn.Exec(ctx, `
		INSERT INTO ticks (
			instrument_token, timestamp, proto_data
		) VALUES ($1, $2, $3)
		ON CONFLICT (instrument_token, timestamp) 
		DO UPDATE SET proto_data = EXCLUDED.proto_data`,
		tick.InstrumentToken,
		time.Unix(tick.Timestamp, 0),
		protoData,
	)

	if err != nil {
		logger.GetLogger().Error("Failed to store tick", map[string]interface{}{
			"error": err.Error(),
			"token": tick.InstrumentToken,
			"time":  time.Unix(tick.Timestamp, 0),
		})
		return fmt.Errorf("failed to store tick: %w", err)
	}

	return nil
}
