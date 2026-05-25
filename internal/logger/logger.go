// Package logger wraps zerolog with structured, levelled logging.
// Every log line automatically includes service, correlation_id, order_id,
// and attempt_count — making distributed tracing trivial without a full
// tracing stack.
package logger

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type contextKey string

const (
	correlationKey contextKey = "correlation_id"
	orderKey       contextKey = "order_id"
	serviceKey     contextKey = "service"
	attemptKey     contextKey = "attempt"
)

// Init configures the global zerolog logger for a given service.
// Pretty-prints in development; JSON in production (LOG_FORMAT=json).
func Init(serviceName string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339

	var base zerolog.Logger
	if os.Getenv("LOG_FORMAT") == "json" {
		base = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		base = log.Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05"})
	}

	return base.With().Str("service", serviceName).Logger()
}

// WithCorrelation injects correlation_id + order_id into the context
// so every downstream log call carries them automatically.
func WithCorrelation(ctx context.Context, correlationID, orderID string) context.Context {
	ctx = context.WithValue(ctx, correlationKey, correlationID)
	ctx = context.WithValue(ctx, orderKey, orderID)
	return ctx
}

// WithAttempt injects the retry attempt number into the context.
func WithAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, attemptKey, attempt)
}

// FromContext returns a zerolog.Logger enriched with all values in ctx.
func FromContext(ctx context.Context, base zerolog.Logger) zerolog.Logger {
	l := base

	if v, ok := ctx.Value(correlationKey).(string); ok && v != "" {
		l = l.With().Str("correlation_id", v).Logger()
	}
	if v, ok := ctx.Value(orderKey).(string); ok && v != "" {
		l = l.With().Str("order_id", v).Logger()
	}
	if v, ok := ctx.Value(attemptKey).(int); ok {
		l = l.With().Int("attempt", v).Logger()
	}

	return l
}
