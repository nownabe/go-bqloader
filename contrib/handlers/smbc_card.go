package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

// SMBCCardStatement build a *bqloader.Handler for statements of SMBC card (三井住友VISAカード).
func SMBCCardStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	type contextKey string
	var monthKey contextKey = "month"

	re := regexp.MustCompile(`/(\d+)\.csv`)
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

		// 0: date (ご利用日)
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, err
		}
		r[0] = t.Format("2006-01-02")

		// 7: payment_month (支払い月)
		r = append(r, paymentMonth)

		return r, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 0,

		Encoding:     japanese.ShiftJIS,
		Parser:       PartialCSVParser(1, 0, "\r\n"),
		Projector:    projector,
		Preprocessor: preprocessor,
		Notifier:     notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
