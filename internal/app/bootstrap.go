package app

import (
	"context"
	"godis/internal/command"
	"godis/internal/common/logger"
	"godis/internal/config"
	"godis/internal/engine"
	"godis/internal/server"
)

type App struct {
	Config 	 config.Config
	Logger   *logger.Logger
	Engine   *engine.Engine
	Executor *command.Executor
	Server   *server.Server
}

func Start(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	l := logger.New(cfg.LogLevel)
	eng := engine.NewEngine(cfg.Databases)
	exec := command.NewExecutor(eng)
	srv := server.NewServer(cfg, l, exec)

	return &App{
		Config: cfg,
		Logger: l,
		Engine: eng,
		Executor: exec,
		Server: srv,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	defer a.Engine.Close()
	return a.Server.Run(ctx)
}