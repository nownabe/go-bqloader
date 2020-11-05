package bqloader

import (
	"context"
	"log"
	"sync"

	"golang.org/x/xerrors"
)

// BQLoader loads data from Cloud Storage to BigQuery table.
type BQLoader interface {
	AddHandler(context.Context, *Handler) error
	Handle(context.Context, Event) error
	MustAddHandler(context.Context, *Handler)
}

// New build a new Loader.
func New() BQLoader {
	return &bqloader{
		handlers: []*Handler{},
		mu:       sync.RWMutex{},
	}
}

type bqloader struct {
	handlers []*Handler
	mu       sync.RWMutex
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

func (l *bqloader) Handle(ctx context.Context, e Event) error {
	log.Printf("loader started")
	defer log.Printf("loader finished")

	log.Printf("file name = %s", e.Name)

	for _, h := range l.handlers {
		log.Printf("handler = %+v", h)
		if h.match(e.Name) {
			log.Printf("handler matches")
			if err := h.handle(ctx, e); err != nil {
				log.Printf("error: %v", err)
				return xerrors.Errorf("failed to handle: %w", err)
			}
		}
	}

	return nil
}
