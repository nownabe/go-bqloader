package bqloader

import (
	"context"
	"time"
)

type contextKey string

const (
	startedTimeKey contextKey = "startedTime"
)

func withStartedTime(ctx context.Context) context.Context {
	return context.WithValue(ctx, startedTimeKey, time.Now())
}

func startedTimeFrom(ctx context.Context) (time.Time, bool) {
	t, ok := ctx.Value(startedTimeKey).(time.Time)
	return t, ok
}
