package handlers

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

// SMBCCardStatement build a *bqloader.Handler for statements of SMBC card (三井住友VISAカード).
// To add column of payment month, keep the file name when you downloaded it.
func SMBCCardStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	var monthKey contextKey = "month"

	parser := func(_ context.Context, r io.Reader) ([][]string, error) {
		body, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, xerrors.Errorf("failed to read: %w", err)
		}

		csvBody := ""

		lines := strings.Split(string(body), "\r\n")
		for _, line := range lines {
			if len(line) > 4 && line[4] == '/' {
				csvBody += line + "\r\n"
			}
		}

		records, err := csv.NewReader(bytes.NewReader([]byte(csvBody))).ReadAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to read as CSV: %w", err)
		}

		return records, nil
	}

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
			return nil, xerrors.Errorf("failed to parse date: %v", err)
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

		Encoding: japanese.ShiftJIS,
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
