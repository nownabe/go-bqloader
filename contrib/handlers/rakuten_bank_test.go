package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_RakutenBankStatement(t *testing.T) {
	t.Parallel()

	const csv = "csv/rakuten_bank_statement.csv"

	expected := [][]string{
		{"2020-02-25", "-754", "184655", "ラクテンショウケンカブシキガイシャ （投資信託買付代金）"},
		{"2020-02-28", "8363", "193018", "ラクテンショウケンカブシキガイシャ （自動スイ－プ）"},
		{"2020-03-03", "12033", "205051", "ラクテンショウケンカブシキガイシャ （自動スイ－プ）"},
	}

	h, tl := buildTestHandler(t, csv, handlers.RakutenBankStatement)

	name := "path_to/rakuten_bank_statement.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
