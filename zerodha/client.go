package zerodha

import (
	"context"
	"gohustle/config"
	"gohustle/db"
	"gohustle/filestore"
	"gohustle/logger"
	"os"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type KiteConnect struct {
	Kite      *kiteconnect.Client
	db        *db.TimescaleDB
	config    *config.KiteConfig
	fileStore filestore.FileStore
}

func NewKiteConnect(database *db.TimescaleDB, cfg *config.KiteConfig) *KiteConnect {
	log := logger.GetLogger()
	ctx := context.Background()

	kite := &KiteConnect{
		config:    cfg,
		db:        database,
		fileStore: filestore.NewDiskFileStore(),
	}

	log.Info("Initializing KiteConnect client", map[string]interface{}{
		"api_key": cfg.APIKey,
	})

	kite.Kite = kiteconnect.New(cfg.APIKey)

	token, err := kite.GetValidToken(ctx)
	if err != nil {
		log.Error("Failed to get valid token", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	kite.Kite.SetAccessToken(token)
	log.Info("Set access token for Kite client", map[string]interface{}{
		"token_length": len(token),
	})

	log.Info("Successfully initialized KiteConnect", nil)

	return kite
}
