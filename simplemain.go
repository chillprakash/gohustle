package main

import (
	"context"
	"fmt"
	"gohustle/logger"
	"gohustle/zerodha"
)

func main() {
	ctx := context.Background()
	kc := zerodha.GetKiteConnect()
	log := logger.GetLogger()
	ltp, err := kc.GetIndexLTPFromRedis(ctx, "NIFTY")
	if err != nil {
		log.Error("Failed to get LTP", map[string]interface{}{
			"error": err.Error(),
		})
	}
	fmt.Println(ltp)
}
