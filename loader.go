package bqloader

import (
	"bytes"
	"context"
	"encoding/csv"

	"cloud.google.com/go/bigquery"
	"golang.org/x/xerrors"
)

// Loader loads projected data into a destination such as BigQuery.
type Loader interface {
	Load(context.Context, [][]string) error
}

type defaultLoader struct {
	table *bigquery.Table
}

func newDefaultLoader(ctx context.Context, project, dataset, table string) (Loader, error) {
	bq, err := bigquery.NewClient(ctx, project)
	if err != nil {
		return nil, xerrors.Errorf("failed to build bigquery client for %s.%s.%s: %w",
			project, dataset, table, err)
	}

	t := bq.Dataset(dataset).Table(table)

	return &defaultLoader{table: t}, nil
}

func (l *defaultLoader) Load(ctx context.Context, records [][]string) error {
	buf := &bytes.Buffer{}
	if err := csv.NewWriter(buf).WriteAll(records); err != nil {
		return xerrors.Errorf("failed to write csv into buffer: %w", err)
	}

	rs := bigquery.NewReaderSource(buf)
	rs.AllowQuotedNewlines = true

	loader := l.table.LoaderFrom(rs)
	loader.LoadConfig.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		return xerrors.Errorf("failed to run bigquery load job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return xerrors.Errorf("failed to wait bigquery job: %w", err)
	}

	if status.Err() != nil {
		return xerrors.Errorf("bigquery load job failed: %w", status.Err())
	}

	return nil
}
