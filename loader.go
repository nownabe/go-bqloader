package bqloader

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"golang.org/x/text/transform"
)

// BQLoader loads data from Cloud Storage to BigQuery table.
type BQLoader interface {
	AddHandler(*Handler)
	Handle(context.Context, Event) error
}

// Event is an event from Cloud Storage.
type Event struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`

	// for test
	source io.Reader
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

func (l *bqloader) AddHandler(h *Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.handlers = append(l.handlers, h)
}

func (l *bqloader) Handle(ctx context.Context, e Event) error {
	log.Printf("loader started")
	defer log.Printf("loader finished")

	log.Printf("file name = %s", e.Name)

	for _, h := range l.handlers {
		log.Printf("handler = %+v", h)
		if h.match(e.Name) {
			log.Printf("handler matches")
			if err := l.handle(ctx, e, h); err != nil {
				log.Printf("error: %v", err)
				return err
			}
		}
	}

	return nil
}

func (l *bqloader) handle(ctx context.Context, e Event, h *Handler) error {
	var r io.Reader
	if h.extractor != nil {
		// If extractor is specified, prefer to use it.
		er, err := h.extractor.extract(ctx, e)
		if err != nil {
			return err
		}
		r = er
	} else {
		// If extractor is not specified, use the default extractor.
		// TODO: Make following process to get data from cloud storage an extractor.
		sc, err := storage.NewClient(ctx)
		if err != nil {
			return err
		}

		obj := sc.Bucket(e.Bucket).Object(e.Name)
		objr, err := obj.NewReader(ctx)
		if err != nil {
			log.Printf("[%s] failed to initialize object reader: %v", h.Name, err)
			return err
		}
		defer objr.Close()
		log.Printf("[%s] DEBUG objr = %+v", h.Name, objr)

		r = objr
	}

	if h.Encoding != nil {
		r = transform.NewReader(r, h.Encoding.NewDecoder())
	}

	source, err := h.Parser(ctx, r)
	if err != nil {
		log.Printf("[%s] failed to parse object: %v", h.Name, err)
		return err
	}
	source = source[h.SkipLeadingRows:]

	records := make([][]string, len(source))

	// TODO: Make this loop parallel.
	for i, r := range source {
		record, err := h.Projector(r)
		if err != nil {
			log.Printf("[%s] failed to project row %d: %v", h.Name, i+h.SkipLeadingRows, err)
			return err
		}

		records[i] = record
	}

	log.Printf("[%s] DEBUG records = %+v", h.Name, records)

	// If loader is specified, prefer to use Loader.
	if h.loader != nil {
		return h.loader.load(ctx, records)
	}

	// TODO: Make following process to load a Loader.

	// TODO: Make output format more efficient. e.g. gzip.
	buf := &bytes.Buffer{}
	if err := csv.NewWriter(buf).WriteAll(records); err != nil {
		log.Printf("[%s] failed to write csv: %v", h.Name, err)
		return err
	}

	bq, err := bigquery.NewClient(ctx, h.Project)
	if err != nil {
		return err
	}

	table := bq.Dataset(h.Dataset).Table(h.Table)
	rs := bigquery.NewReaderSource(buf)
	loader := table.LoaderFrom(rs)

	job, err := loader.Run(ctx)
	if err != nil {
		log.Printf("[%s] failed to run bigquery load job: %v", h.Name, err)
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("[%s] failed to wait job: %v", h.Name, err)
		return err
	}

	if status.Err() != nil {
		log.Printf("[%s] failed to load csv: %v", h.Name, status.Errors)
		return status.Err()
	}

	return nil
}