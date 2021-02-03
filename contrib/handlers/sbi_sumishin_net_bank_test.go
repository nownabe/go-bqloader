package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_SBISumishinNetBankStatement(t *testing.T) {
	t.Parallel()

	const csv = "csv/sbi_sumishin_net_bank_statement.csv"

	expected := [][]string{
		{"2020-12-25", "普通　円　フィンビーエゴマ", "220", "", "29447", "-"},
		{"2020-12-25", "振込＊キュウヨ．フリコム．ジヤパン（ド", "", "29667", "29667", "-"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SBISumishinNetBankStatement)

	name := "path_to/sbi_sumishin_net_bank_statement.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
