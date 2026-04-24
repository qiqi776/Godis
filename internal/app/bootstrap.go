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
	Config   config.Config
	Logger   *logger.Logger
	Engine   *engine.Engine
	Executor *command.Executor
	AOF      *command.AOFLog
	RDB      *command.RDBFile
	Server   *server.Server
}

func Bootstrap(cfgPath string) (*App, error) {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return nil, err
	}

	l := logger.New(cfg.LogLevel)
	eng := engine.New(cfg.Databases)
	exec := command.NewExecutor(eng)

	var rdbFile *command.RDBFile
	if cfg.RDBEnabled {
		rdbFile, err = command.NewRDBFile(cfg.RDBPath)
		if err != nil {
			return nil, err
		}
		if err := rdbFile.Load(exec); err != nil {
			return nil, err
		}
		exec.SetDumper(rdbFile)
	}

	var aofLog *command.AOFLog
	if cfg.AOFEnabled {
		fsyncPolicy, err := command.ParseFsyncPolicy(cfg.AOFFsync)
		if err != nil {
			return nil, err
		}

		aofLog, err = command.OpenAOFWithPreamble(cfg.AOFPath, cfg.AOFUseRDBPreamble, fsyncPolicy)
		if err != nil {
			return nil, err
		}
		if err := aofLog.Replay(exec); err != nil {
			_ = aofLog.Close()
			return nil, err
		}
		exec.SetAppender(aofLog)
		exec.SetRewriter(aofLog)
	}

	srv := server.New(cfg, l, exec)

	return &App{
		Config:   cfg,
		Logger:   l,
		Engine:   eng,
		Executor: exec,
		AOF:      aofLog,
		RDB:      rdbFile,
		Server:   srv,
	}, nil
}

func (a *App) Run(ctx context.Context) error {
	if a.AOF != nil {
		defer a.AOF.Close()
	}
	defer a.Engine.Close()
	return a.Server.Run(ctx)
}
