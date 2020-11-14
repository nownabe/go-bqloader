bqloader
========

![Main Branch Workflow](https://github.com/nownabe/go-bqloader/workflows/Main%20Branch%20Workflow/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/nownabe/go-bqloader)](https://goreportcard.com/report/github.com/nownabe/go-bqloader)
[![codecov](https://codecov.io/gh/nownabe/go-bqloader/branch/main/graph/badge.svg)](https://codecov.io/gh/nownabe/go-bqloader)

bqloader is a simple ETL framework running on Cloud Functions to load data from Cloud Storage into BigQuery.

## Installation

```bash
go get -u go.nownabe.dev/bqloader
```

## Getting Started

(*See [Quickstart example](https://github.com/nownabe/go-bqloader/tree/main/examples/quickstart) to get a full instruction.)

For simple transforming and loading CSV, import the package `go.nownabe.dev/bqloader` and write your handler.

```go
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
  /*
    Projectors transform each row.
    This projector transforms date columns formatted as "2006/01/02" at the first column
    into BigQuery date format like "2006-01-02".
  */
	projector := func(l int, r []string) ([]string, error) {
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("Line %d column 0 cannot parse as a date: %w", l, err)
		}

		r[0] = t.Format("2006-01-02")

		return r, nil
	}

	return &bqloader.Handler{
		Name:            "mybank",                        // Handler name used in logging.
		Pattern:         regexp.MustCompile("^mybank/"),  // Files matching this pattern are processed with this handler.
    Encoding:        japanese.ShiftJIS,               // Encoding field specifies the encoding of input files.
		Parser:          bqloader.CSVParser,
		Projector:       projector,
		SkipLeadingRows: 1,

		Project: os.Getenv("BIGQUERY_PROJECT_ID"),
		Dataset: os.Getenv("BIGQUERY_DATASET_ID"),
		Table:   os.Getenv("BIGQUERY_TABLE_ID"),
	}
}

// Func is the entrypoint for Cloud Functions.
func Func(ctx context.Context, e bqloader.Event) error {
	return loader.Handle(ctx, e)
}
```
