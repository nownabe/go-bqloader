package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

// SBISumishinNetBankStatement build a handler for statements of SBI bank (住信SBIネット銀行).
func SBISumishinNetBankStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	projector := func(_ context.Context, r []string) ([]string, error) {
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %w", err)
		}

		r[0] = t.Format("2006-01-02")

		// Remove commas
		r[2] = CleanNumber(r[2])
		r[3] = CleanNumber(r[3])
		r[4] = CleanNumber(r[4])

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
