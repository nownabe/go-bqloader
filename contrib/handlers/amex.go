package handlers

import (
	"context"
	"errors"
	"io"
	"regexp"
	"time"

	"github.com/extrame/xls"
	"gitlab.com/osaki-lab/iowrapper"
	"go.nownabe.dev/bqloader"
	"golang.org/x/xerrors"
)

var (
	errAMEXStatementNoSheet = errors.New("no sheet found")
)

// AMEXStatement build a *bqloader.Handler for statements of AMEX (American Express).
// To add column of payment month, keep the file name as the payment month like '2022-07.xls'.
func AMEXStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	var monthKey contextKey = "month"

	getRow := func(sheet *xls.WorkSheet, row int) (r *xls.Row, ok bool) {
		defer func() { recover() }()

		r = nil
		ok = false

		return sheet.Row(row), true
	}

	dateRE := regexp.MustCompile(`^\d\d\d\d/\d\d/\d\d$`)

	parser := func(_ context.Context, r io.Reader) ([][]string, error) {
		wb, err := xls.OpenReader(iowrapper.NewSeeker(r), "utf-8")
		if err != nil {
			return nil, xerrors.Errorf("failed to open xls file: %w", err)
		}

		sheet := wb.GetSheet(0)
		if sheet == nil {
			return nil, errAMEXStatementNoSheet
		}

		records := [][]string{}

		for i := 0; i <= int(sheet.MaxRow); i++ {
			row, ok := getRow(sheet, i)
			if !ok {
				continue
			}

			if val := row.Col(row.FirstCol()); !dateRE.Match([]byte(val)) {
				continue
			}

			record := []string{}

			for colNum := row.FirstCol(); colNum < row.LastCol(); colNum++ {
				record = append(record, row.Col(colNum))
			}

			records = append(records, record)
		}

		return records, nil
	}

	filenameRE := regexp.MustCompile(`/(\d\d\d\d-\d\d)\.xls$`)

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
		r[4] = CleanNumber(r[4])

		// 8: payment_month (支払い月)
		r = append(r, paymentMonth)

		return r, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 0,

		// Parser:       PartialCSVParser(1, 0, "\r\n"),
		Parser:       parser,
		Projector:    projector,
		Preprocessor: preprocessor,
		Notifier:     notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
