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

	"github.com/hibiken/asynq"
	googleproto "google.golang.org/protobuf/proto"
)

func StartTickConsumer(cfg *config.Config, kite *zerodha.KiteConnect) {
	log := logger.GetLogger()
	log.Info("Starting File Tick Consumer", nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create tick writer with context and base directory
	baseDir := "data/ticks"
	tickWriter := zerodha.NewTickWriter(ctx, 1000, baseDir)
	defer tickWriter.Shutdown()

	// Register handler for file processing queue
	kite.GetAsynqQueue().HandleFunc("process_tick_file", func(ctx context.Context, t *asynq.Task) error {
		tick := &proto.TickData{}
		if err := googleproto.Unmarshal(t.Payload(), tick); err != nil {
			return fmt.Errorf("failed to unmarshal tick: %w", err)
		}

		// Get the token info from cache
		tokenStr := fmt.Sprintf("%d", tick.InstrumentToken)
		tokenInfo, exists := kite.GetInstrumentInfo(tokenStr)
		if !exists {
			return fmt.Errorf("token not found in lookup: %s", tokenStr)
		}

		// Generate filename
		currentDate := time.Now().Format("02-01-2006")
		filename := generateFileName(tokenInfo.Index, tokenInfo.Expiry, currentDate, tokenInfo.IsIndex)

		return tickWriter.Write(tick, filename)
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
