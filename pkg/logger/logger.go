package logger

import (
	"log"
	"os"
)

var logger *log.Logger

func Init(level string) {
	logger = log.New(os.Stdout, "[GODIS] ", log.LstdFlags|log.Lmicroseconds)
}

func Info(format string, v ...interface{}) {
	logger.Printf("[INFO] "+format, v...)
}

func Debug(format string, v ...interface{}) {
	logger.Printf("[DEBUG] "+format, v...)
}

func Error(format string, v ...interface{}) {
	logger.Printf("[ERROR] "+format, v...)
}

func Fatal(format string, v ...interface{}) {
	logger.Printf("[FATAL] "+format, v...)
	os.Exit(1)
}