package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New() (*zap.Logger, func()) { l, _ := zap.NewProduction(); return l, func() { _ = l.Sync() } }
func Err(err error) zap.Field    { return zap.Error(err) }
func State(s string) zap.Field   { return zap.String("state", s) }
func Latency(ms int64) zap.Field { return zap.Int64("latency_ms", ms) }

var _ = zapcore.InfoLevel
