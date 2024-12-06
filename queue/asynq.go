package queue

import (
	"context"
	"fmt"
	"os"
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

	redisOpt := asynq.RedisClientOpt{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
	}

	client := asynq.NewClient(redisOpt)

	if err := client.Ping(); err != nil {
		log.Error("Failed to connect to Redis", map[string]interface{}{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	server := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency:    cfg.Concurrency,
			Queues:         cfg.Queues,
			StrictPriority: true,
			RetryDelayFunc: defaultRetryFunc,
		},
	)

	log.Info("Successfully connected to Asynq", map[string]interface{}{
		"host":        cfg.Host,
		"port":        cfg.Port,
		"concurrency": cfg.Concurrency,
		"queues":      cfg.Queues,
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

func (a *AsynqQueue) HandleFunc(taskType string, handler func(context.Context, *asynq.Task) error) {
	a.mux.HandleFunc(taskType, handler)
}

func (a *AsynqQueue) Start() error {
	return a.server.Run(a.mux)
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
