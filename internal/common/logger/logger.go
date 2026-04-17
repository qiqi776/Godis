package logger

import (
	"io"
	"log"
	"os"
	"strings"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelError
)

type Logger struct {
	base  *log.Logger
	level Level
}

func New(raw string) *Logger {
    return &Logger{
        base:  log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lmicroseconds),
        level: parseLevel(raw),
    }
}

func NewDiscard() *Logger {
    return &Logger{
        base:  log.New(io.Discard, "", 0),
        level: LevelError,
    }
}

func (l *Logger) Debugf(format string, args ...any) {
    if l.level <= LevelDebug {
        l.base.Printf("[DEBUG] "+format, args...)
    }
}

func (l *Logger) Infof(format string, args ...any) {
    if l.level <= LevelInfo {
        l.base.Printf("[INFO] "+format, args...)
    }
}

func (l *Logger) Errorf(format string, args ...any) {
    if l.level <= LevelError {
        l.base.Printf("[ERROR] "+format, args...)
    }
}

func parseLevel(raw string) Level {
    switch strings.ToLower(raw) {
    case "debug":
        return LevelDebug
    case "error":
        return LevelError
    default:
        return LevelInfo
    }
}