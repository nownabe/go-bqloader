package handlers

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

func parseSMBCDate(w string) (time.Time, error) {
	if len(w) != 9 {
		return time.Time{}, xerrors.Errorf("invalid date format: %s", w)
	}

	var rekiBase int
	switch w[0] {
	case 'H':
		rekiBase = 1988
	case 'R':
		rekiBase = 2018
	default:
		return time.Time{}, xerrors.Errorf("%s is not supported", w)
	}

	wareki, err := strconv.Atoi(w[1:3])
	if err != nil {
		return time.Time{}, xerrors.Errorf("failed to parse wareki as int: %w", err)
	}

	return time.Parse("2006.01.02", fmt.Sprintf("%d%s", wareki+rekiBase, w[3:9]))
}

// SMBCStatement builds a handler for statements for SMBC (三井住友銀行 入出金明細).
func SMBCStatement(name, pattern string, t Table, n bqloader.Notifier) *bqloader.Handler {
	projector := func(ctx context.Context, r []string) ([]string, error) {
		// 0: date (年月日)
		t, err := parseSMBCDate(r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse date: %w", err)
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
		Notifier:  n,

		Project: t.Project,
		Dataset: t.Dataset,
		Table:   t.Table,
	}
}
