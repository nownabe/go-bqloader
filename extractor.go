package bqloader

import (
	"context"
	"io"
	"log"

	"cloud.google.com/go/storage"
)

// extractor extracts data from source such as Cloud Storage.
type extractor interface {
	extract(context.Context, Event) (io.Reader, error)
}

type defaultExtractor struct {
	storage *storage.Client
}

func newDefaultExtractor(ctx context.Context, project string) (extractor, error) {
	s, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &defaultExtractor{storage: s}, nil
}

// TODO: Summarize error log (use xerrors).
func (e *defaultExtractor) extract(ctx context.Context, ev Event) (io.Reader, error) {
	obj := e.storage.Bucket(ev.Bucket).Object(ev.Name)
	r, err := obj.NewReader(ctx)
	if err != nil {
		log.Printf("failed to initialize object reader: %v", err)
		return nil, err
	}
	defer r.Close()
	log.Printf("DEBUG r = %+v", r)

	return r, nil
}
