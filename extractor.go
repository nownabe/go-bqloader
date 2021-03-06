package bqloader

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/xerrors"
)

// Extractor extracts data from source such as Cloud Storage.
type Extractor interface {
	Extract(context.Context, Event) (io.Reader, func(), error)
}

type defaultExtractor struct {
	storage *storage.Client
}

func newDefaultExtractor(ctx context.Context, project string) (Extractor, error) {
	s, err := storage.NewClient(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to build storage client for %s: %w", project, err)
	}

	return &defaultExtractor{storage: s}, nil
}

func (e *defaultExtractor) Extract(ctx context.Context, ev Event) (io.Reader, func(), error) {
	obj := e.storage.Bucket(ev.Bucket).Object(ev.Name)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, nil, xerrors.Errorf("failed to get reader of %s: %w", ev.FullPath(), err)
	}

	return r, func() { r.Close() }, nil
}
