package bqload

import (
	"context"
	"os"
	"regexp"
	"runtime"
	"time"

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
	// Extract payment month from object path in this preprocessor.
	type contextKey string
	const monthKey contextKey = "month"

	re := regexp.MustCompile(`source_(\d+)\.csv`)
	preprocessor := func(ctx context.Context, e bqloader.Event) (context.Context, error) {
		match := re.FindStringSubmatch(e.Name)
		if len(match) < 2 {
			return ctx, xerrors.Errorf("wrong object path: %s", e.Name)
		}

		paymentMonth, err := time.Parse("200601", match[1])
		if err != nil {
			return ctx, xerrors.Errorf("failed to parse payment month from object path: %s: %w", match[1], err)
		}

		return context.WithValue(ctx, monthKey, paymentMonth.Format("2006-01-02")), nil
	}

	projector := func(ctx context.Context, r []string) ([]string, error) {
		// Skip noisy row.
		if r[0] == "" {
			return nil, nil
		}

		// Get payment month from context.
		paymentMonth, ok := ctx.Value(monthKey).(string)
		if !ok {
			return nil, xerrors.Errorf("failed to get payment month from context: %v", paymentMonth)
		}

		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("Column 0 cannot parse as a date: %w", err)
		}

		r[0] = t.Format("2006-01-02")
		r = append(r, paymentMonth)

		return r, nil
	}

	return &bqloader.Handler{
		Name:            "preprocessor",
		Pattern:         regexp.MustCompile("^preprocessor/"),
		Parser:          bqloader.CSVParser(),
		Projector:       projector,
		Preprocessor:    preprocessor,
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
