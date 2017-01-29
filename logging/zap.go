package logging

import (
	"github.com/uber-go/zap"
)

type zapLogger struct {
	log zap.Logger
}


func (l *zapLogger) Debug(message string, fields ...interface{}) {
	l.log.Log(zap.DebugLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) Info(message string, fields ...interface{}) {
	l.log.Log(zap.InfoLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) Warn(message string, fields ...interface{}) {
	l.log.Log(zap.WarnLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) Error(message string, fields ...interface{}) {
	l.log.Log(zap.ErrorLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) Panic(message string, fields ...interface{}) {
	l.log.Log(zap.PanicLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) Fatal(message string, fields ...interface{}) {
	l.log.Log(zap.FatalLevel, message, l.convertToFields(fields)...)
}

func (l *zapLogger) convertToFields(originalFields []interface{}) []zap.Field {
	converted := make([]zap.Field, 0, len(originalFields))

	for _, original := range originalFields {
		switch v := original.(type) {
			case zap.Field:
				converted = append(converted, v)

			default:
				panic("Logger methods only accept zap.Field")
		}

	}

	return converted
}

func CreateLogger(debug bool) Logger {
	l := &zapLogger{}

	if debug {
		l.log = zap.New(zap.NewTextEncoder(zap.TextNoTime()))
	} else {
		l.log = zap.New(zap.NewJSONEncoder())
	}

	return l
}

func String(key string, val string) zap.Field {
	return zap.String(key, val)
}

func Int(key string, val int) zap.Field {
	return zap.Int(key, val)
}

func Int64(key string, val int64) zap.Field {
	return zap.Int64(key, val)
}