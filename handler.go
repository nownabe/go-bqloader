package bqloader

import (
	"context"
	"regexp"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
	"golang.org/x/xerrors"
)

const defaultBatchSize = 10000

// Handler defines how to handle events which match specified pattern.
type Handler struct {
	// Name is the handler's name.
	Name string

	Pattern         *regexp.Regexp
	Encoding        encoding.Encoding
	Parser          Parser
	Notifier        Notifier
	Projector       Projector
	SkipLeadingRows uint

	// BatchSize specifies how much records are processed in a groutine.
	// Default is 10000.
	BatchSize int

	// Project specifies GCP project name of destination BigQuery table.
	Project string

	// Dataset specifies BigQuery dataset ID of destination table
	Dataset string

	// Table specifies BigQuery table ID as destination.
	Table string

	extractor extractor
	loader    loader
	semaphore chan struct{}
}

// Projector transforms source records into records for destination.
type Projector func(int, []string) ([]string, error)

func (h *Handler) match(name string) bool {
	return h.Pattern != nil && h.Pattern.MatchString(name)
}

func (h *Handler) handle(ctx context.Context, e Event) error {
	ctx = withHandlerStartedTime(ctx)
	l := log.Ctx(ctx)
	l = h.logger(ctx, l)
	ctx = l.WithContext(ctx)

	l.Info().Msgf("handler %s started to handle an event", h.Name)
	defer func() {
		now := time.Now()
		e := l.Info().Time("handlerFinished", now)
		if t, ok := handlerStartedTimeFrom(ctx); ok {
			e.TimeDiff("handlerElapsed", now, t)
		}
		e.Msgf("handler %s finished to handle an event", h.Name)
	}()

	err := h.process(ctx, e)
	if err != nil {
		err = xerrors.Errorf("failed to handle: %w", err)
		l.Err(err).Msg(err.Error())
	}

	if h.Notifier != nil {
		res := &Result{Event: e, Handler: h, Error: err}
		if nerr := h.Notifier.Notify(ctx, res); nerr != nil {
			nerr = xerrors.Errorf("failed to notify: %w", nerr)
			l.Err(nerr).Msg(nerr.Error())
		}
	}

	return err
}

func (h *Handler) process(ctx context.Context, e Event) error {
	r, closer, err := h.extractor.extract(ctx, e)
	if err != nil {
		return xerrors.Errorf("failed to extract: %w", err)
	}
	defer closer()

	if h.Encoding != nil {
		r = transform.NewReader(r, h.Encoding.NewDecoder())
	}

	source, err := h.Parser(ctx, r)
	if err != nil {
		return xerrors.Errorf("failed to parse: %w", err)
	}
	source = source[h.SkipLeadingRows:]

	records := [][]string{}
	mu := sync.Mutex{}
	eg := errgroup.Group{}
	numBatches := h.calcBatches(len(source))

	for i := 0; i < numBatches; i++ {
		i := i
		h.semaphore <- struct{}{}

		eg.Go(func() error {
			defer func() { <-h.semaphore }()

			batchRecords := [][]string{}

			startLine := h.BatchSize * i
			endLine := h.BatchSize * (i + 1)
			if endLine > len(source) {
				endLine = len(source)
			}

			for j := startLine; j < endLine; j++ {
				record, err := h.Projector(j, source[j])
				if err != nil {
					return xerrors.Errorf("failed to project row %d (line %d): %w", j, uint(j)+h.SkipLeadingRows, err)
				}

				if record != nil {
					batchRecords = append(batchRecords, record)
				}
			}

			mu.Lock()
			defer mu.Unlock()

			records = append(records, batchRecords...)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return xerrors.Errorf("failed to project: %w", err)
	}

	if err := h.loader.load(ctx, records); err != nil {
		return xerrors.Errorf("failed to load: %w", err)
	}

	return nil
}

func (h *Handler) logger(ctx context.Context, l *zerolog.Logger) *zerolog.Logger {
	lctx := l.With()

	if t, ok := handlerStartedTimeFrom(ctx); ok {
		lctx = lctx.Time("handlerStarted", t)
	}

	d := zerolog.Dict().
		Str("name", h.Name).
		Uint("skipLeadingRows", h.SkipLeadingRows).
		Str("project", h.Project).
		Str("dataset", h.Dataset).
		Str("table", h.Table)

	if h.Pattern != nil {
		d = d.Str("pattern", h.Pattern.String())
	}

	logger := lctx.Dict("handler", d).Logger()

	return &logger
}

func (h *Handler) calcBatches(length int) int {
	r := length / h.BatchSize
	if length%h.BatchSize != 0 {
		r++
	}
	return r
}
