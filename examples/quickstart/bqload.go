package bqload

import (
	"context"
	"os"
	"regexp"
	"runtime"
	"strings"
	"time"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"

	"go.nownabe.dev/bqloader"
)

var loader bqloader.BQLoader

func init() {
	c := runtime.NumCPU()
	runtime.GOMAXPROCS(c)

	var err error
	loader, err = bqloader.New(bqloader.WithLogLevel("debug"), bqloader.WithConcurrency(c))
	if err != nil {
		panic(err)
	}
	loader.MustAddHandler(context.Background(), newHandler())
}

func newHandler() *bqloader.Handler {
	/*
		this projector converts date fields formatted as "2006/01/02"
		at the first column into strings like "2006-01-02" that satisfies
		BigQuery date type, and removes commas in numeric fields.
	*/
	projector := func(_ context.Context, r []string) ([]string, error) {
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("Column 0 cannot parse as a date: %w", err)
		}

		r[0] = t.Format("2006-01-02")
		r[2] = strings.ReplaceAll(r[2], ",", "")
		r[3] = strings.ReplaceAll(r[3], ",", "")
		r[4] = strings.ReplaceAll(r[4], ",", "")

		return r, nil
	}

	return &bqloader.Handler{
		Name:     "quickstart",                         // Handler name used in logs and notifications.
		Pattern:  regexp.MustCompile("^example_bank/"), // This handler processes files matched to this pattern.
		Encoding: japanese.ShiftJIS,                    // Source file encoding.
		Parser:   bqloader.CSVParser(),                 // Parser parses source file into records.
		Notifier: &bqloader.SlackNotifier{
			Token:   os.Getenv("SLACK_TOKEN"),
			Channel: os.Getenv("SLACK_CHANNEL"),
		},
		Projector:       projector, // Projector transforms each row.
		SkipLeadingRows: 1,         // Skip header row.

		// Destination.
		Project: os.Getenv("BIGQUERY_PROJECT_ID"),
		Dataset: os.Getenv("BIGQUERY_DATASET_ID"),
		Table:   os.Getenv("BIGQUERY_TABLE_ID"),
	}
}

// BQLoad is the entrypoint for Cloud Functions.
func BQLoad(ctx context.Context, e bqloader.Event) error {
	return loader.Handle(ctx, e)
}
