package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_SBISecuritiesGlobalBankingStatement(t *testing.T) {
	t.Parallel()

	const csv = "testdata/sbi_securities_global_banking_statement.csv"

	expected := [][]string{
		{"2022-12-30", "分配金", "米ドル", "BND 銘柄名:VG TBM", "", "0.64"},
		{"2022-12-28", "出金", "米ドル", "米国株式TECL外国源泉税の過日徴収", "3.58", ""},
		{"2022-11-18", "入金", "米ドル", "米国BND211229配当税還付 税減額日:220311", "", "0.07"},
		{"2022-10-03", "入金", "米ドル", "住信SBIネット銀行から外貨入金", "", "100.00"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SBISecuritiesGlobalBankingStatement)

	name := "path_to/sbi_securities_global_banking_statement.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}

func Test_SBISecuritiesGlobalExecutionHistory(t *testing.T) {
	t.Parallel()

	const csv = "testdata/sbi_securities_global_execution_history.csv"

	expected := [][]string{
		{"2020-09-18", "バンガード S&P 500 ETF", "VOO", "NYSEArca", "米国株式", "成行", "買付", "特定預り", "8", "306.4800", "2020-09-24", "257345"},
		{"2020-09-29", "iシェアーズ コア　米国高配当株 ETF", "HDV", "NYSEArca", "米国株式", "成行", "買付", "特定預り", "22", "80.2400", "2020-10-01", "187423"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SBISecuritiesGlobalExecutionHistory)

	name := "path_to/sbi_securities_global_execution_history.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
