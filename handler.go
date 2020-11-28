package bqloader

import (
	"context"
	"regexp"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
	"golang.org/x/xerrors"
)

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

	// Project specifies GCP project name of destination BigQuery table.
	Project string

	// Dataset specifies BigQuery dataset ID of destination table
	Dataset string

	// Table specifies BigQuery table ID as destination.
	Table string

	extractor extractor
	loader    loader
}

// Projector transforms source records into records for destination.
type Projector func(int, []string) ([]string, error)

func (h *Handler) match(name string) bool {
	return h.Pattern != nil && h.Pattern.MatchString(name)
}

func (h *Handler) handle(ctx context.Context, e Event) error {
	started := time.Now()
	l := log.Ctx(ctx)
	l = h.logger(l)
	ctx = l.WithContext(ctx)

	l.Info().Msgf("handler %s started to handle an event", h.Name)
	defer func() {
		elapsed := time.Since(started)
		l.Info().Msgf("handler %s finished to handle an event. elapsed = %v", h.Name, elapsed)
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

	records := make([][]string, len(source))

	// TODO: Make this loop parallel.
	for i, r := range source {
		record, err := h.Projector(i, r)
		if err != nil {
			return xerrors.Errorf("failed to project row %d (line %d): %w", i, uint(i)+h.SkipLeadingRows, err)
		}

		records[i] = record
	}

	if err := h.loader.load(ctx, records); err != nil {
		return xerrors.Errorf("failed to load: %w", err)
	}

	return nil
}

func (h *Handler) logger(l *zerolog.Logger) *zerolog.Logger {
	d := zerolog.Dict().
		Str("name", h.Name).
		Str("pattern", h.Pattern.String()).
		Uint("skipLeadingRows", h.SkipLeadingRows).
		Str("project", h.Project).
		Str("dataset", h.Dataset).
		Str("table", h.Table)
	logger := l.With().Dict("handler", d).Logger()
	return &logger
}
