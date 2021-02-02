package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
)

// RakutenBankStatement build a handler for statements for Rakuten Bank (楽天銀行 入出金明細).
func RakutenBankStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	projector := func(ctx context.Context, r []string) ([]string, error) {
		// 0: date (取引日)
		t, err := time.Parse("20060102", r[0])
		if err != nil {
			return nil, err
		}
		r[0] = t.Format("2006-01-02")

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
