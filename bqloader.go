package bqloader

import (
	"context"
	"io"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/functions/metadata"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
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
			h.logger(ctx, l.logger).Err(err).Msg(err.Error())
			return err
		}
		h.extractor = ex
	}

	if h.loader == nil {
		loader, err := newDefaultLoader(ctx, h.Project, h.Dataset, h.Table)
		if err != nil {
			err = xerrors.Errorf("failed to build default loader for table '%s.%s.%s': %w",
				h.Project, h.Dataset, h.Table, err)
			h.logger(ctx, l.logger).Err(err).Msg(err.Error())
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
	ctx = withStartedTime(ctx)
	logger := contextualLogger(ctx, e, l.logger)

	logger.Info().Msg("bqloader started to handle an event")
	defer func() {
		now := time.Now()
		e := logger.Info().Time("finished", now)
		if t, ok := startedTimeFrom(ctx); ok {
			e.TimeDiff("elapsed", now, t)
		}
		e.Msgf("bqloader finished to handle an envent")
	}()

	g, ctx := errgroup.WithContext(logger.WithContext(ctx))

	for _, h := range l.handlers {
		if h.match(e.Name) {
			h := h
			g.Go(func() error {
				return h.handle(ctx, e)
			})
		}
	}

	if err := g.Wait(); err != nil {
		err = xerrors.Errorf("imcompleted with error: %w", err)
		logger.Err(err).Msg(err.Error())
		return err
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
	lctx := e.logger(l).With()

	t, ok := startedTimeFrom(ctx)
	if ok {
		lctx = lctx.Time("started", t)
	}

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

		lctx = lctx.Dict("metadata", d)
	} else {
		l.Warn().Msg(err.Error())
	}

	rl := lctx.Logger()
	return &rl
}
