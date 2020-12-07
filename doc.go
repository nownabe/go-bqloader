/*

Package bqloader is a simple ETL framework running on Cloud Functions
to load data from Cloud Storage into BigQuery.

Getting started

See Quickstart example to get a full instruction.
https://github.com/nownabe/go-bqloader/tree/main/examples/quickstart

For simple transforming and loading CSV, import the package `go.nownabe.dev/bqloader` and write your handler.

	package myfunc

	import (
		"context"
		"os"
		"regexp"
		"strings"
		"time"

		"golang.org/x/text/encoding/japanese"
		"golang.org/x/xerrors"

		"go.nownabe.dev/bqloader"
	)

	var loader bqloader.BQLoader

	func init() {
		var err error
		loader, err = bqloader.New()
		if err != nil {
			panic(err)
		}
		loader.MustAddHandler(context.Background(), newHandler())
	}

	func newHandler() *bqloader.Handler {
		// This projector converts date fields formatted as "2006/01/02"
		// at the first column into strings like "2006-01-02" that satisfies
		// BigQuery date type.
		projector := func(_ context.Context, r []string) ([]string, error) {
			t, err := time.Parse("2006/01/02", r[0])
			if err != nil {
				return nil, xerrors.Errorf("Column 0 cannot parse as a date: %w", err)
			}

			r[0] = t.Format("2006-01-02")

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

*/
package bqloader
