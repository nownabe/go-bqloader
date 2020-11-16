package bqloader

import (
	"context"
	"io"
	"os"
	"sync"

	"cloud.google.com/go/functions/metadata"
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
func New(opts ...Option) (BQLoader, error) {
	bq := &bqloader{
		handlers:      []*Handler{},
		mu:            sync.RWMutex{},
		prettyLogging: false,
		logLevel:      zerolog.ErrorLevel,
	}

	for _, o := range opts {
		if err := o.apply(bq); err != nil {
			return nil, err
		}
	}

	var w io.Writer
	if bq.prettyLogging {
		w = zerolog.ConsoleWriter{Out: os.Stdout}
	} else {
		w = os.Stdout
	}
	l := zerolog.New(w).Level(bq.logLevel).With().Timestamp().Logger().Hook(severityHook{})
	bq.logger = &l

	return bq, nil
}

type bqloader struct {
	handlers      []*Handler
	mu            sync.RWMutex
	logger        *zerolog.Logger
	prettyLogging bool
	logLevel      zerolog.Level
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

func (l *bqloader) Handle(ctx context.Context, e Event) error {
	logger := contextualLogger(ctx, e, l.logger)

	logger.Info().Msg("bqloader started to handle an event")
	defer logger.Info().Msg("bqloader finished to handle an envent")

	// TODO: Make this parallel.
	for _, h := range l.handlers {
		if h.match(e.Name) {
			l := h.logger(logger)
			ctx := l.WithContext(ctx)
			err := h.handle(ctx, e)
			if err != nil {
				err = xerrors.Errorf("failed to handle: %w", err)
				l.Err(err).Msg(err.Error())
			}

			res := &Result{Event: e, Handler: h, Error: err}
			nerr := h.Notifier.Notify(ctx, res)
			if nerr != nil {
				nerr = xerrors.Errorf("failed to notify: %w", nerr)
				l.Err(nerr).Msg(nerr.Error())
			}

			// TODO: Avoid earlier return
			if err != nil {
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

func contextualLogger(ctx context.Context, e Event, l *zerolog.Logger) *zerolog.Logger {
	logger := e.logger(l)

	md, err := metadata.FromContext(ctx)
	if err == nil {
		rd := zerolog.Dict().
			Str("service", md.Resource.Service).
			Str("name", md.Resource.Name).
			Str("type", md.Resource.Type).
			Str("rawPath", md.Resource.RawPath)

		d := zerolog.Dict().
			Str("eventId", md.EventID).
			Time("timestamp", md.Timestamp).
			Str("eventType", md.EventType).
			Dict("resource", rd)

		ml := logger.With().Dict("metadata", d).Logger()
		logger = &ml
	} else {
		logger.Warn().Msg(err.Error())
	}

	return logger
}
