package queue

import (
	"context"
	"fmt"
	"time"

	"gohustle/config"
	"gohustle/logger"
	proto "gohustle/proto"

	googleproto "google.golang.org/protobuf/proto"

	"github.com/hibiken/asynq"
)

type AsynqQueue struct {
	client *asynq.Client
	server *asynq.Server
	config *config.AsynqConfig
	mux    *asynq.ServeMux
}

// InitAsynqQueue initializes the Asynq client and server
func InitAsynqQueue(cfg *config.AsynqConfig) *AsynqQueue {
	log := logger.GetLogger()

	log.Info("Initializing Asynq Queue", map[string]interface{}{
		"host": cfg.Host,
		"port": cfg.Port,
		"db":   cfg.DB,
	})

	redisOpt := asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.MaxConnections,
	}

	client := asynq.NewClient(redisOpt)
	server := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: cfg.Concurrency,
			Queues: map[string]int{
				"ticks_file":      cfg.Queues["ticks_file"],
				"ticks_timescale": cfg.Queues["ticks_timescale"],
			},
			RetryDelayFunc: defaultRetryFunc,
			IsFailure: func(err error) bool {
				return err != nil
			},
			ShutdownTimeout: 5 * time.Second,
			Logger:          NewAsynqLogger(),
		},
	)

	log.Info("Asynq Queue initialized", map[string]interface{}{
		"concurrency": cfg.Concurrency,
	})

	return &AsynqQueue{
		client: client,
		server: server,
		config: cfg,
		mux:    asynq.NewServeMux(),
	}
}

func defaultRetryFunc(n int, err error, task *asynq.Task) time.Duration {
	return time.Duration(n) * time.Second * 5
}

func (a *AsynqQueue) GetConfig() *config.AsynqConfig {
	return a.config
}

func (a *AsynqQueue) Close() {
	if a.client != nil {
		a.client.Close()
	}
	if a.server != nil {
		a.server.Stop()
	}
}

func (a *AsynqQueue) Enqueue(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	_, err := a.client.EnqueueContext(ctx, task, opts...)
	return err
}

func (a *AsynqQueue) HandleFunc(pattern string, handler func(context.Context, *asynq.Task) error) {
	log := logger.GetLogger()
	log.Info("Registering handler", map[string]interface{}{
		"pattern": pattern,
	})

	// Wrap the handler to add logging
	wrappedHandler := func(ctx context.Context, t *asynq.Task) error {
		log.Debug("Processing task", map[string]interface{}{
			"type": t.Type(),
			"id":   t.ResultWriter().TaskID(),
		})

		err := handler(ctx, t)
		if err != nil {
			log.Error("Task processing failed", map[string]interface{}{
				"type":  t.Type(),
				"id":    t.ResultWriter().TaskID(),
				"error": err.Error(),
			})
		}
		return err
	}

	a.mux.HandleFunc(pattern, wrappedHandler)
}

func (a *AsynqQueue) Start() error {
	log := logger.GetLogger()
	log.Info("Starting Asynq server", map[string]interface{}{
		"queues": a.config.Queues,
	})

	if err := a.server.Run(a.mux); err != nil { // Changed from Start to Run
		log.Error("Failed to start Asynq server", map[string]interface{}{
			"error": err.Error(),
		})
		return err
	}
	return nil
}

// ProcessTickTask registers a handler for tick processing
func (a *AsynqQueue) ProcessTickTask(handler func(context.Context, uint32, *proto.TickData) error) {
	a.mux.HandleFunc("process_tick", func(ctx context.Context, t *asynq.Task) error {
		log := logger.GetLogger()

		tick := &proto.TickData{}
		if err := googleproto.Unmarshal(t.Payload(), tick); err != nil {
			log.Error("Failed to unmarshal proto tick", map[string]interface{}{
				"error":   err.Error(),
				"payload": string(t.Payload()),
			})
			return err
		}

		return handler(ctx, tick.InstrumentToken, tick)
	})
}

type AsynqLogger struct {
	logger *logger.Logger
}

func NewAsynqLogger() *AsynqLogger {
	return &AsynqLogger{
		logger: logger.GetLogger(),
	}
}

func (l *AsynqLogger) Debug(args ...interface{}) {
	l.logger.Debug(fmt.Sprint(args...), map[string]interface{}{
		"source": "asynq",
	})
}

func (l *AsynqLogger) Info(args ...interface{}) {
	l.logger.Info(fmt.Sprint(args...), map[string]interface{}{
		"source": "asynq",
	})
}

func (l *AsynqLogger) Warn(args ...interface{}) {
	l.logger.Info(fmt.Sprint(args...), map[string]interface{}{
		"source": "asynq",
		"level":  "WARN",
	})
}

func (l *AsynqLogger) Error(args ...interface{}) {
	l.logger.Error(fmt.Sprint(args...), map[string]interface{}{
		"source": "asynq",
	})
}

func (l *AsynqLogger) Fatal(args ...interface{}) {
	l.logger.Error(fmt.Sprint(args...), map[string]interface{}{
		"source": "asynq",
		"level":  "FATAL",
	})
}
