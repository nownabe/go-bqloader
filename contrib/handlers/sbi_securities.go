package handlers

import (
	"context"
	"regexp"
	"time"

	"go.nownabe.dev/bqloader"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/xerrors"
)

// SBISecuritiesGlobalBankingStatement build a handler for banking statement of SBI Securities Global (SBI証券 外国株式 入出金明細).
func SBISecuritiesGlobalBankingStatement(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	projector := func(_ context.Context, r []string) ([]string, error) {
		projected := make([]string, 6)

		// 0 -> 0: date (入出金日)
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse data: %w", err)
		}
		projected[0] = t.Format("2006-01-02")

		// 1 -> 1: 取引
		projected[1] = r[1]

		// 5 -> 2: 通貨
		projected[2] = r[5]

		// 2 -> 3: 摘要
		projected[3] = r[2]

		// 3 -> 4: 出金額
		projected[4] = CleanNumber(r[3])

		// 4 -> 5: 入金額
		projected[5] = CleanNumber(r[4])

		return projected, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 1,

		Encoding:  japanese.ShiftJIS,
		Parser:    PartialCSVParser(6, 0, "\n"),
		Projector: projector,
		Notifier:  notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}

// SBISecuritiesGlobalExecutionHistory build a handler for execution history of SBI Securities Global (SBI証券 外国株式 約定履歴).
func SBISecuritiesGlobalExecutionHistory(name, pattern string, table Table, notifier bqloader.Notifier) *bqloader.Handler {
	projector := func(_ context.Context, r []string) ([]string, error) {
		// 0: contract_date (国内約定日)
		contractDate, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse data at column 0: %w", err)
		}
		r[0] = contractDate.Format("2006-01-02")

		// 8: quantity (約定数量)
		r[8] = CleanNumber(r[8])

		// 9: unit_price (約定単価)
		r[9] = CleanNumber(r[9])

		// 10: delivery_date (国内受渡日)
		deliveryDate, err := time.Parse("2006/01/02", r[10])
		if err != nil {
			return nil, xerrors.Errorf("failed to parse data at column 10: %w", err)
		}
		r[10] = deliveryDate.Format("2006-01-02")

		// 11: delivery_amount (受渡金額)
		r[11] = CleanNumber(r[11])

		return r, nil
	}

	return &bqloader.Handler{
		Name:            name,
		Pattern:         regexp.MustCompile(pattern),
		SkipLeadingRows: 1,

		Encoding:  japanese.ShiftJIS,
		Parser:    PartialCSVParser(6, 0, "\r\n"),
		Projector: projector,
		Notifier:  notifier,

		Project: table.Project,
		Dataset: table.Dataset,
		Table:   table.Table,
	}
}
