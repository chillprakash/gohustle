package consumer

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gohustle/config"
	"gohustle/logger"
	proto "gohustle/proto"
	"gohustle/zerodha"
)

func StartTickConsumer(cfg *config.Config, kite *zerodha.KiteConnect) {
	log := logger.GetLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create tick writer with context and base directory
	baseDir := "data/ticks" // You might want to get this from config
	tickWriter := zerodha.NewTickWriter(ctx, 1000, baseDir)
	defer tickWriter.Shutdown()

	// Register consumer with ctx
	kite.ProcessTickTask(func(ctx context.Context, token uint32, tick *proto.TickData) error {
		// Get the token info from cache
		tokenStr := fmt.Sprintf("%d", token)
		tokenInfo, exists := kite.GetInstrumentInfo(tokenStr)
		if !exists {
			log.Error("Token not found in lookup cache", map[string]interface{}{
				"token": tokenStr,
			})
			return fmt.Errorf("token not found in lookup: %s", tokenStr)
		}

		index := tokenInfo.Index // Get the index name

		// Generate filename using expiry and current date
		currentDate := time.Now().Format("02-01-2006")
		filename := generateFileName(index, tokenInfo.Expiry, currentDate, tokenInfo.IsIndex)

		log.Info("Processing tick", map[string]interface{}{
			"token":        token,
			"symbol":       tokenInfo.Symbol,
			"index":        tokenInfo.Index,
			"expiry":       tokenInfo.Expiry.Format("2006-01-02"),
			"current_date": currentDate,
			"filename":     filename,
			"last_price":   tick.LastPrice,
			"volume":       tick.VolumeTraded,
			"oi":           tick.Oi,
			// ... other tick data ...
		})

		if err := tickWriter.Write(tick, filename); err != nil {
			log.Error("Failed to write tick", map[string]interface{}{
				"error": err.Error(),
				"token": token,
			})
			return err
		} // TODO: Implement file writing logic here
		return nil
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}

func generateFileName(index string, expiry time.Time, currentDate string, isIndex bool) string {
	formattedDate := strings.ReplaceAll(currentDate, "-", "")

	if isIndex {
		// For indices, use only index name and current date
		return fmt.Sprintf("%s_%s.pb",
			index,
			formattedDate)
	}

	// For options, use index, expiry and current date
	return fmt.Sprintf("%s_%s_%s.pb",
		index,
		expiry.Format("20060102"),
		formattedDate)
}
