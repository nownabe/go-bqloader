package bqloader

import (
	"context"
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
	l := zerolog.New(os.Stdout).With().Timestamp().Logger().Hook(severityHook{})
	return &bqloader{
		handlers: []*Handler{},
		mu:       sync.RWMutex{},
		logger:   &l,
	}
}

type bqloader struct {
	handlers []*Handler
	mu       sync.RWMutex
	logger   *zerolog.Logger
}

func (l *bqloader) AddHandler(ctx context.Context, h *Handler) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if h.extractor == nil {
		ex, err := newDefaultExtractor(ctx, h.Project)
		if err != nil {
			err = xerrors.Errorf("failed to build default extractor for project '%s': %w", h.Project, err)
			h.logger(l.logger).Err(err).Msg(err.Error())
			return err
		}
		h.extractor = ex
	}

	if h.loader == nil {
		loader, err := newDefaultLoader(ctx, h.Project, h.Dataset, h.Table)
		if err != nil {
			err = xerrors.Errorf("failed to build default loader for table '%s.%s.%s': %w",
				h.Project, h.Dataset, h.Table, err)
			h.logger(l.logger).Err(err).Msg(err.Error())
			return err
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
	TODO: Use Cloud Functions Metadata https://godoc.org/cloud.google.com/go/functions/metadata
*/
func (l *bqloader) Handle(ctx context.Context, e Event) error {
	logger := e.logger(l.logger)

	logger.Info().Msg("bqloader started to handle an event")
	defer logger.Info().Msg("bqloader finished to handle an envent")

	for _, h := range l.handlers {
		if h.match(e.Name) {
			l := h.logger(logger)
			if err := h.handle(l.WithContext(ctx), e); err != nil {
				err = xerrors.Errorf("failed to handle: %w", err)
				l.Err(err).Msg(err.Error())
				return err
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
