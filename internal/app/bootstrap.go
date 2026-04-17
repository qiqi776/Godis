package app

import (
	"context"
	"godis/internal/command"
	"godis/internal/common/logger"
	"godis/internal/config"
	"godis/internal/server"
)

type App struct {
	Config 	 config.Config
	Logger   *logger.Logger
	Executor *command.Executor
	Server   *server.Server
}

func Start(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	l := logger.New(cfg.LogLevel)
	e := command.NewExecutor()
	srv := server.NewServer(cfg, l, e)

	return &App{
		Config: cfg,
		Logger: l,
		Executor: e,
		Server: srv,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	return a.Server.Run(ctx)
}