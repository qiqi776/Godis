package main

import (
	"context"
	"godis/internal/app"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfgPath := os.Getenv("GODIS_CONFIG")
	str, err := app.Start(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := str.Run(ctx); err != nil {
		log.Fatal(err)
	}
}