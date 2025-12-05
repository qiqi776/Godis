package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"io"
)

// 定义日志级别
const (
	LevelDebug = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

var (
	logger 		*log.Logger
	currentLevel int
)

// 级别转换
func parseLevel(lv string) int {
	switch strings.ToLower(lv) {
	case "debug": return LevelDebug
	case "warn":  return LevelWarn
	case "error": return LevelError
	case "fatal": return LevelFatal
	default:      return LevelInfo
	}
}



func Init(level string, logFile string) {
	currentLevel = parseLevel(level)
	var out io.Writer = os.Stdout

	// 如果配置了日志文件，就写入文件
	if logFile != "" {
		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err == nil {
			out = f
		} else {
			fmt.Printf("Failed to open log file: %v, using stdout\n", err)
		}
	}
	logger = log.New(out, "[GODIS] ", log.LstdFlags|log.Lmicroseconds)
}

func Info(format string, v ...interface{}) {
	if currentLevel <= LevelInfo {
        logger.Printf("[INFO] "+format, v...)
    }
}

func Debug(format string, v ...interface{}) {
	if currentLevel <= LevelDebug {
        logger.Printf("[DEBUG] "+format, v...)
    }
}

func Error(format string, v ...interface{}) {
	if currentLevel <= LevelError {
		logger.Printf("[ERROR] "+format, v...)
	}
}

func Fatal(format string, v ...interface{}) {
	if currentLevel <= LevelFatal {
		logger.Printf("[FATAL] "+format, v...)
		os.Exit(1)
	}
}