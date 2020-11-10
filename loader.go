package bqloader

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
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
		return nil, xerrors.Errorf("failed to build bigquery client for %s.%s.%s: %w",
			project, dataset, table, err)
	}

	t := bq.Dataset(dataset).Table(table)

	return &defaultLoader{table: t}, nil
}

// TODO: Summarize log (use xerrors)
func (l *defaultLoader) load(ctx context.Context, records [][]string) error {
	logger := log.Ctx(ctx)

	// TODO: Make output format more efficient. e.g. gzip.
	buf := &bytes.Buffer{}
	if err := csv.NewWriter(buf).WriteAll(records); err != nil {
		logger.Error().Msg(fmt.Sprintf("failed to write csv: %v", err))
		return xerrors.Errorf("failed to write csv into buffer: %w", err)
	}
	rs := bigquery.NewReaderSource(buf)
	loader := l.table.LoaderFrom(rs)
	loader.LoadConfig.CreateDisposition = bigquery.CreateNever

	job, err := loader.Run(ctx)
	if err != nil {
		logger.Error().Msg(fmt.Sprintf("failed to run bigquery load job: %v", err))
		return xerrors.Errorf("failed to run bigquery load job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		logger.Error().Msg(fmt.Sprintf("failed to wait job: %v", err))
		return xerrors.Errorf("failed to wait bigquery job: %w", err)
	}

	if status.Err() != nil {
		logger.Error().Msg(fmt.Sprintf("failed to load csv: %v", status.Errors))
		return xerrors.Errorf("bigquery load job failed: %w", status.Err())
	}

	return nil
}
