package notify

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"gohustle/config"
	"gohustle/logger"
)

type TelegramNotifier struct {
	config *config.TelegramConfig
	log    *logger.Logger
}

func NewTelegramNotifier(cfg *config.TelegramConfig) *TelegramNotifier {
	return &TelegramNotifier{
		config: cfg,
		log:    logger.L(),
	}
}

func (t *TelegramNotifier) SendMessage(message string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.config.BotToken)

	resp, err := http.PostForm(apiURL, url.Values{
		"chat_id": {t.config.ChatID},
		"text":    {message},
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func (t *TelegramNotifier) SendFile(filePath string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendDocument", t.config.BotToken)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add file
	part, err := writer.CreateFormFile("document", filepath.Base(filePath))
	if err != nil {
		return err
	}
	if _, err = io.Copy(part, file); err != nil {
		return err
	}

	// Add chat_id
	writer.WriteField("chat_id", t.config.ChatID)
	writer.Close()

	// Send request
	resp, err := http.Post(apiURL, writer.FormDataContentType(), body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

// Example usage at end of day
func (t *TelegramNotifier) SendDailySummary(dataDir string) error {
	// Count files and sizes
	instrumentFiles, err := filepath.Glob(filepath.Join(dataDir, "instruments", "*.pb.gz"))
	if err != nil {
		return err
	}

	tickFiles, err := filepath.Glob(filepath.Join(dataDir, "ticks", "*.pb"))
	if err != nil {
		return err
	}

	summary := fmt.Sprintf("Daily Summary:\n"+
		"Instruments Files: %d\n"+
		"Tick Files: %d\n",
		len(instrumentFiles),
		len(tickFiles))

	if err := t.SendMessage(summary); err != nil {
		return err
	}

	// Send latest instrument file
	if len(instrumentFiles) > 0 {
		latestFile := instrumentFiles[len(instrumentFiles)-1]
		if err := t.SendFile(latestFile); err != nil {
			t.log.Error("Failed to send instrument file", map[string]interface{}{
				"error": err.Error(),
				"file":  latestFile,
			})
		}
	}

	return nil
}
