package common

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var Logger = func(name string) *ZerologLogger {
	return NewZerologLogger("gostore::", "name", name)
}

type ZerologLogger struct {
	zerolog.Logger
	traceID          string
	optionalKeyPairs []interface{}
}

func NewZerologLogger(traceID string, optional ...interface{}) *ZerologLogger {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	l := log.With().Logger()
	return &ZerologLogger{Logger: l, traceID: traceID, optionalKeyPairs: optional}
}

func (l *ZerologLogger) fieldsFromArgs(args ...interface{}) map[string]interface{} {
	fields := map[string]interface{}{"traceID": l.traceID}
	if len(l.optionalKeyPairs) >= 2 {
		for i := 0; i < len(l.optionalKeyPairs); i += 2 {
			key, ok := l.optionalKeyPairs[i].(string)
			if !ok {
				key = "<unknown>"
			}
			fields[key] = l.optionalKeyPairs[i+1]
		}
	}
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			key = "<unknown>"
		}
		fields[key] = args[i+1]
	}
	return fields
}
func (l *ZerologLogger) Trace(msg string, args ...interface{}) {
	l.WithLevel(zerolog.TraceLevel).Fields(l.fieldsFromArgs(args...)).Msg(msg)
}

func (l *ZerologLogger) Debug(msg string, args ...interface{}) {
	l.WithLevel(zerolog.DebugLevel).Fields(l.fieldsFromArgs(args...)).Msg(msg)
}

func (l *ZerologLogger) Info(msg string, args ...interface{}) {
	l.WithLevel(zerolog.InfoLevel).Fields(l.fieldsFromArgs(args...)).Msg(msg)
}

func (l *ZerologLogger) Warn(msg string, args ...interface{}) error {
	l.WithLevel(zerolog.WarnLevel).Fields(l.fieldsFromArgs(args...)).Msg(msg)
	return nil
}

func (l *ZerologLogger) Error(msg string, args ...interface{}) error {
	l.WithLevel(zerolog.ErrorLevel).Fields(l.fieldsFromArgs(args...)).Msg(msg)
	return nil
}

func (l *ZerologLogger) Fatal(msg string, args ...interface{}) {
	l.WithLevel(zerolog.FatalLevel).Msgf(msg, args...)
}

func (l *ZerologLogger) Log(level int, msg string, args []interface{}) {
	switch level {
	case 0:
		l.Trace(msg, args...)
	case 1:
		l.Debug(msg, args...)
	case 2:
		l.Info(msg, args...)
	case 3:
		l.Warn(msg, args...)
	case 4:
		l.Error(msg, args...)
	case 5:
		l.Fatal(msg, args...)
	default:
		l.WithLevel(zerolog.InfoLevel).Msgf(msg, args...)
	}
}

func (l *ZerologLogger) SetLevel(level int) {
	l.Logger = l.Logger.Level(zerolog.Level(level))
}

func (l *ZerologLogger) IsTrace() bool {
	return l.GetLevel() == zerolog.TraceLevel
}

func (l *ZerologLogger) IsDebug() bool {
	return l.GetLevel() <= zerolog.DebugLevel
}

func (l *ZerologLogger) IsInfo() bool {
	return l.GetLevel() <= zerolog.InfoLevel
}

func (l *ZerologLogger) IsWarn() bool {
	return l.GetLevel() <= zerolog.WarnLevel
}

type requestIDKEY string

const (
	requestIDKey = "reqID"
)

// WithRqID returns a context which knows its request ID
func WithRqID(ctx context.Context, rqID string) context.Context {
	return context.WithValue(ctx, requestIDKEY(requestIDKey), rqID)
}
