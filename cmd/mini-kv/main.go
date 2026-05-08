package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"mini-kv/internal/app"
)

func main() {
	cfgPath := os.Getenv("MINIKV_CONFIG")
	application, err := app.Start(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
