bqloader
========

[![PkgGoDev](https://pkg.go.dev/badge/go.nownabe.dev/bqloader)](https://pkg.go.dev/go.nownabe.dev/bqloader)
![Main Branch Workflow](https://github.com/nownabe/go-bqloader/workflows/Main%20Branch%20Workflow/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/nownabe/go-bqloader)](https://goreportcard.com/report/github.com/nownabe/go-bqloader)
[![codecov](https://codecov.io/gh/nownabe/go-bqloader/branch/main/graph/badge.svg)](https://codecov.io/gh/nownabe/go-bqloader)
![GitHub](https://img.shields.io/github/license/nownabe/go-bqloader)
![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/nownabe/go-bqloader?sort=semver)

bqloader is a simple ETL framework running on Cloud Functions to load data from Cloud Storage into BigQuery.

## Installation

```bash
go get -u go.nownabe.dev/bqloader
```

## Getting Started with Pre-configured Handlers

See the [example](https://github.com/nownabe/go-bqloader/tree/main/examples/pre_configured_handlers) to get a full instruction.

To load some types of CSV formats, you can use pre-configured handlers.
See [full list](https://github.com/nownabe/go-bqloader/tree/doc/contrib/handlers).


```go
package myfunc

import (
	"context"
	"os"
	"runtime"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

var loader bqloader.BQLoader

func init() {
	loader, _ = bqloader.New()

	t := handlers.TableGenerator(os.Getenv("BIGQUERY_PROJECT_ID"), os.Getenv("BIGQUERY_DATASET_ID"))
	n := &bqloader.SlackNotifier{
		Token:   os.Getenv("SLACK_TOKEN"),
		Channel: os.Getenv("SLACK_CHANNEL"),
	}

	handlers.MustAddHandlers(context.Background(), loader,
		/*
			These build handlers to load CSVs, given four arguments:
			handler name, a pattern to file path on Cloud Storage, a BigQuery table and a notifier.
		*/
		handlers.SBISumishinNetBankStatement("SBI Bank", `^sbi_bank/`, t("sbi_bank"), n),
		handlers.SMBCCardStatement("SMBC Card", `^smbc_card/`, t("smbc_card"), n),
	)
}

// BQLoad is the entrypoint for Cloud Functions.
func BQLoad(ctx context.Context, e bqloader.Event) error {
	return loader.Handle(ctx, e)
}
```


## Getting Started with Custom Handlers

(See [Quickstart example](https://github.com/nownabe/go-bqloader/tree/main/examples/quickstart) to get a full instruction.)

To load other CSVs, import the package `go.nownabe.dev/bqloader` and write your custom handler.

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
		This projector converts date fields formatted as "2006/01/02"
		at the first column into strings like "2006-01-02" that satisfies
		BigQuery date type.
	*/
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
```

## Diagram

![diagram](https://raw.githubusercontent.com/nownabe/go-bqloader/main/diagram.png)
