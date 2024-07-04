package log

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger
var loggerCfg zap.Config

func Logger() *zap.Logger {
	if logger == nil {
		if conf := os.Getenv("LOGGER_ENV"); conf == "prod" {
			loggerCfg = zap.NewProductionConfig()
		} else {
			loggerCfg = zap.NewDevelopmentConfig()
		}
		if conf := os.Getenv("LOGGER_LEVEL"); len(conf) > 0 {
			l, e := zapcore.ParseLevel(conf)
			if e != nil {
				l = zapcore.InfoLevel
			}
			loggerCfg.Level.SetLevel(l)
		}
		loggerCfg.DisableStacktrace = true
		if conf := os.Getenv("LOGGER_STACKTRACE"); conf == "true" {
			loggerCfg.DisableStacktrace = false
		}
		loggerCfg.DisableCaller = true
		var err error
		if logger, err = loggerCfg.Build(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	return logger
}

func SetLoggerLevelInfo() {
	loggerCfg.Level.SetLevel(zapcore.InfoLevel)
}

func SetLoggerLevelDebug() {
	loggerCfg.Level.SetLevel(zapcore.DebugLevel)
}

func SetLoggerLevelFatal() {
	loggerCfg.Level.SetLevel(zapcore.FatalLevel)
}

func SetLoggerLevel(level string) {
	l, e := zapcore.ParseLevel(level)
	if e != nil {
		l = zapcore.FatalLevel
	}
	loggerCfg.Level.SetLevel(l)
}

func ResetLoggerLevel(log *zap.Logger, level string) *zap.Logger {
	zlevel, err := zapcore.ParseLevel(level)
	if err != nil {
		logger.Error(err.Error())
		return log
	}
	return log.WithOptions(zap.WrapCore(
		func(zapcore.Core) zapcore.Core {
			sink, _, err := zap.Open(loggerCfg.OutputPaths...)
			if err != nil {
				logger.Error(err.Error())
			}
			return zapcore.NewCore(
				zapcore.NewConsoleEncoder(loggerCfg.EncoderConfig),
				sink,
				zlevel)
		}))
}
