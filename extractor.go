package bqloader

import (
	"context"
	"io"
	"log"

	"cloud.google.com/go/storage"
	"golang.org/x/xerrors"
)

// extractor extracts data from source such as Cloud Storage.
type extractor interface {
	extract(context.Context, Event) (io.Reader, func(), error)
}

type defaultExtractor struct {
	storage *storage.Client
}

func newDefaultExtractor(ctx context.Context, project string) (extractor, error) {
	s, err := storage.NewClient(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to build storage client for %s: %w", project, err)
	}

	return &defaultExtractor{storage: s}, nil
}

// TODO: Summarize error log (use xerrors).
func (e *defaultExtractor) extract(ctx context.Context, ev Event) (io.Reader, func(), error) {
	obj := e.storage.Bucket(ev.Bucket).Object(ev.Name)
	r, err := obj.NewReader(ctx)
	if err != nil {
		log.Printf("failed to initialize object reader: %v", err)
		return nil, nil, xerrors.Errorf("failed to get reader of %s: %w", ev.FullPath(), err)
	}
	log.Printf("DEBUG r = %+v", r)

	return r, func() { r.Close() }, nil
}
