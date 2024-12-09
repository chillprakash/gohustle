package main

import (
	"log"
	"net/http"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
)

func main() {
	// Create a new HTTP handler for asynqmon
	h := asynqmon.New(asynqmon.Options{
		RootPath:     "/",
		RedisConnOpt: asynq.RedisClientOpt{Addr: "localhost:6379"},
	})

	// Start HTTP server
	log.Printf("Starting asynq monitor at http://localhost:8080")
	if err := http.ListenAndServe(":8080", h); err != nil {
		log.Fatal(err)
	}
}
