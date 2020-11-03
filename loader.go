package bqloader

import (
	"bytes"
	"context"
	"encoding/csv"
	"log"

	"cloud.google.com/go/bigquery"
)

// loader loads projected data into a destination such as BigQuery.
type loader interface {
	load(context.Context, [][]string) error
}

type defaultLoader struct {
	table *bigquery.Table
}

func newDefaultLoader(ctx context.Context, project, dataset, table string) (loader, error) {
	bq, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}

	t := bq.Dataset(dataset).Table(table)

	return &defaultLoader{table: t}, nil
}

// TODO: Log with handler name (use context).
// TODO: Summarize log (use xerrors)
func (l *defaultLoader) load(ctx context.Context, records [][]string) error {
	// TODO: Make output format more efficient. e.g. gzip.
	buf := &bytes.Buffer{}
	if err := csv.NewWriter(buf).WriteAll(records); err != nil {
		log.Printf("failed to write csv: %v", err)
		return err
	}
	rs := bigquery.NewReaderSource(buf)
	loader := l.table.LoaderFrom(rs)

	job, err := loader.Run(ctx)
	if err != nil {
		log.Printf("failed to run bigquery load job: %v", err)
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("failed to wait job: %v", err)
		return err
	}

	if status.Err() != nil {
		log.Printf("failed to load csv: %v", status.Errors)
		return status.Err()
	}

	return nil
}
