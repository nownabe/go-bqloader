package handlers

import (
	"context"
	"encoding/csv"
	"io"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/xerrors"
)

// RakutenCardStatement build a handler for statements of Rakuten Card (楽天カード 明細).
func RakutenCardStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	var monthKey contextKey = "month"

	re := regexp.MustCompile(`enavi(\d+)\(\d+\)`)
	preprocessor := func(ctx context.Context, e bqloader.Event) (context.Context, error) {
		match := re.FindStringSubmatch(e.Name)
		if len(match) < 2 {
			return ctx, xerrors.Errorf("wrong object path: %s", e.Name)
		}

		month, err := time.Parse("200601", match[1])
		if err != nil {
			return ctx, xerrors.Errorf("failed to parse payment month from object path: %s: %w", match[1], err)
		}

		return context.WithValue(ctx, monthKey, month.Format("2006-01-02")), nil
	}

	projector := func(ctx context.Context, r []string) ([]string, error) {
		if r[0] == "" {
			return nil, nil
		}

		paymentMonth, ok := ctx.Value(monthKey).(string)
		if !ok {
			return nil, xerrors.Errorf("failed to get payment month from context: %v", paymentMonth)
		}

		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %w", err)
		}

		r[0] = t.Format("2006-01-02")
		r = append(r, paymentMonth)

		return r, nil
	}

	parser := bqloader.Parser(func(_ context.Context, r io.Reader) ([][]string, error) {
		reader := csv.NewReader(r)
		reader.LazyQuotes = true

		records, err := reader.ReadAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to read content as a CSV: %w", err)
		}

		return records, nil
	})

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 1,

		Parser:       parser,
		Projector:    projector,
		Preprocessor: preprocessor,
		Notifier:     notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
