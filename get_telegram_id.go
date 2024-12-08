package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"gohustle/config"
	"gohustle/logger"
	"gohustle/notify"
)

type Update struct {
	Message struct {
		Chat struct {
			ID int64 `json:"id"`
		} `json:"chat"`
	} `json:"message"`
}

type Response struct {
	Ok     bool     `json:"ok"`
	Result []Update `json:"result"`
}

func main() {
	log := logger.GetLogger()
	cfg := config.GetConfig()

	// First get chat ID
	url := fmt.Sprintf("https://api.telegram.org/bot%s/getUpdates", cfg.Telegram.BotToken)
	resp, err := http.Get(url)
	if err != nil {
		log.Fatal("Error getting updates", map[string]interface{}{
			"error": err.Error(),
		})
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response", map[string]interface{}{
			"error": err.Error(),
		})
	}

	var response Response
	if err := json.Unmarshal(body, &response); err != nil {
		log.Fatal("Error parsing JSON", map[string]interface{}{
			"error": err.Error(),
		})
	}

	if !response.Ok {
		log.Fatal("Telegram API returned not OK", nil)
	}

	if len(response.Result) == 0 {
		fmt.Println("\nNo messages found. Please:")
		fmt.Println("1. Start your bot (@your_bot_name)")
		fmt.Println("2. Send any message to your bot")
		fmt.Println("3. Run this tool again")
		return
	}

	// Print all chat IDs found
	fmt.Println("\nFound chat IDs:")
	for _, update := range response.Result {
		fmt.Printf("Chat ID: %d\n", update.Message.Chat.ID)
	}

	// Now send a test message
	telegram := notify.NewTelegramNotifier(&cfg.Telegram)
	message := fmt.Sprintf("Test message from GoHustle\n"+
		"Time: %s\n"+
		"Status: Connected\n"+
		"System: Ready",
		time.Now().Format("2006-01-02 15:04:05"))

	if err := telegram.SendMessage(message); err != nil {
		log.Fatal("Failed to send message", map[string]interface{}{
			"error": err.Error(),
		})
	}

	fmt.Println("\nTest message sent successfully!")

	// Send files from data folder
	telegram = notify.NewTelegramNotifier(&cfg.Telegram)
	dataPath := "data"

	// Walk through data directory
	err = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and files in ticks folder
		if info.IsDir() || filepath.HasPrefix(path, filepath.Join(dataPath, "ticks")) {
			return nil
		}

		fmt.Printf("Sending file: %s\n", path)
		if err := telegram.SendFile(path); err != nil {
			log.Error("Failed to send file", map[string]interface{}{
				"error": err.Error(),
				"file":  path,
			})
			return nil // continue with next file even if one fails
		}

		// Sleep to avoid hitting rate limits
		time.Sleep(1 * time.Second)
		return nil
	})

	if err != nil {
		log.Fatal("Error walking data directory", map[string]interface{}{
			"error": err.Error(),
		})
	}

	fmt.Println("\nAll files sent successfully!")
}
