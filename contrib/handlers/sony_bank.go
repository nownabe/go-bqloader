package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

// SonyBankStatement build a handler for statements of Sony Bank (ソニー銀行).
func SonyBankStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	projector := func(_ context.Context, r []string) ([]string, error) {
		t, err := time.Parse("2006年01月02日", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %w", err)
		}

		r[0] = t.Format("2006-01-02")

		// Remove commas
		r[3] = CleanNumber(r[3])
		r[4] = CleanNumber(r[4])
		r[5] = CleanNumber(r[5])

		return r, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 1,

		Encoding:  japanese.ShiftJIS,
		Parser:    bqloader.CSVParser(),
		Projector: projector,
		Notifier:  notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
