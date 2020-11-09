package bqloader

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/rs/zerolog"
	"golang.org/x/xerrors"
)

// BQLoader loads data from Cloud Storage to BigQuery table.
type BQLoader interface {
	AddHandler(context.Context, *Handler) error
	Handle(context.Context, Event) error
	MustAddHandler(context.Context, *Handler)
}

// New build a new Loader.
// TODO: Use zerolog.ConsoleWriter for development.
func New() BQLoader {
	return &bqloader{
		handlers: []*Handler{},
		mu:       sync.RWMutex{},
		logger:   zerolog.New(os.Stdout).With().Timestamp().Logger().Hook(severityHook{}),
	}
}

type bqloader struct {
	handlers []*Handler
	mu       sync.RWMutex
	logger   zerolog.Logger
}

func (l *bqloader) AddHandler(ctx context.Context, h *Handler) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if h.extractor == nil {
		ex, err := newDefaultExtractor(ctx, h.Project)
		if err != nil {
			return xerrors.Errorf("failed to build default extractor for %s: %w", h.Project, err)
		}
		h.extractor = ex
	}

	if h.loader == nil {
		loader, err := newDefaultLoader(ctx, h.Project, h.Dataset, h.Table)
		if err != nil {
			return xerrors.Errorf("failed to build default loader for %s.%s.%s: %w",
				h.Project, h.Dataset, h.Table, err)
		}
		h.loader = loader
	}

	l.handlers = append(l.handlers, h)

	return nil
}

func (l *bqloader) MustAddHandler(ctx context.Context, h *Handler) {
	if err := l.AddHandler(ctx, h); err != nil {
		panic(err)
	}
}

/*
	TODO: Use context logger with fields.
	TODO: Use Cloud Functions Metadata https://godoc.org/cloud.google.com/go/functions/metadata
*/
func (l *bqloader) Handle(ctx context.Context, e Event) error {
	l.logger.Info().Msg("BQLoader started to handle an event")
	defer l.logger.Info().Msg("BQLoader finished to handle an envent")

	l.logger.Info().Msg(fmt.Sprintf("file name = %s", e.Name))

	for _, h := range l.handlers {
		l.logger.Info().Msg(fmt.Sprintf("handler = %+v", h))
		if h.match(e.Name) {
			l.logger.Info().Msg("handler matches")
			ctx := l.logger.WithContext(ctx)
			if err := h.handle(ctx, e); err != nil {
				// TODO: Use l.logger.Err(err)
				l.logger.Error().Msg(fmt.Sprintf("error: %v", err))
				return xerrors.Errorf("failed to handle: %w", err)
			}
		}
	}

	return nil
}

/*
	severity log field is used as Cloud Logging severity
	See https://cloud.google.com/functions/docs/monitoring/logging#processing_special_json_fields_in_messages
*/
type severityHook struct{}

func (h severityHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level != zerolog.NoLevel {
		e.Str("severity", level.String())
	}
}
