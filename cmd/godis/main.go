package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"godis/internal/app"
)

func main() {
	cfgPath := os.Getenv("GODIS_CONFIG")
	application, err := app.Bootstrap(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := application.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
