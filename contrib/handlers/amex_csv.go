package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

func AMEXStatementCSV(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	var monthKey contextKey = "month"

	filenameRE := regexp.MustCompile(`/(\d\d\d\d-\d\d)\.csv$`)

	preprocessor := func(ctx context.Context, e bqloader.Event) (context.Context, error) {
		match := filenameRE.FindStringSubmatch(e.Name)
		if len(match) < 2 {
			return ctx, xerrors.Errorf("wrong object path: %s", e.Name)
		}

		month, err := time.Parse("2006-01", match[1])
		if err != nil {
			return ctx, xerrors.Errorf("failed to parse payment month from object path: %s: %w", match[1], err)
		}

		return context.WithValue(ctx, monthKey, month.Format("2006-01-02")), nil
	}

	projector := func(ctx context.Context, r []string) ([]string, error) {
		paymentMonth, ok := ctx.Value(monthKey).(string)
		if !ok {
			return nil, xerrors.Errorf("failed to get payment month from context")
		}

		// 0: date (ご利用日)
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %v", err)
		}
		r[0] = t.Format("2006-01-02")

		// 1: データ処理日
		t, err = time.Parse("2006/01/02", r[1])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %v", err)
		}
		r[1] = t.Format("2006-01-02")

		// 4: 金額
		r[4] = CleanNumber(r[5])

		// 5: 海外通貨利用金額
		r[5] = r[6]

		// 6: 換算レート
		r[6] = r[7]

		// 7: 追加情報
		r[7] = ""

		// 8: payment_month (支払い月)
		r = append(r, paymentMonth)

		return r, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 1,

		Encoding:     japanese.ShiftJIS,
		Parser:       bqloader.CSVParser(),
		Projector:    projector,
		Preprocessor: preprocessor,
		Notifier:     notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
