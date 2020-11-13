package bqload

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
	loader, err = bqloader.New(bqloader.WithLogLevel("debug"))
	if err != nil {
		panic(err)
	}
	loader.MustAddHandler(context.Background(), newHandler())
}

func newHandler() *bqloader.Handler {
	projector := func(l int, r []string) ([]string, error) {
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("Line %d column 0 cannot parse as a date: %w", l, err)
		}

		r[0] = t.Format("2006-01-02")
		r[2] = strings.ReplaceAll(r[2], ",", "")
		r[3] = strings.ReplaceAll(r[3], ",", "")
		r[4] = strings.ReplaceAll(r[4], ",", "")

		return r, nil
	}

	return &bqloader.Handler{
		Name:            "quickstart",
		Pattern:         regexp.MustCompile("^example_bank/"),
		Encoding:        japanese.ShiftJIS,
		Parser:          bqloader.CSVParser,
		Projector:       projector,
		SkipLeadingRows: 1,

		Project: os.Getenv("BIGQUERY_PROJECT_ID"),
		Dataset: os.Getenv("BIGQUERY_DATASET_ID"),
		Table:   os.Getenv("BIGQUERY_TABLE_ID"),
	}
}

// BQLoad is the entrypoint for Cloud Functions.
func BQLoad(ctx context.Context, e bqloader.Event) error {
	return loader.Handle(ctx, e)
}