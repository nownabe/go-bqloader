package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_SMBCCardStatement(t *testing.T) {
	t.Parallel()

	const csv = "testdata/smbc_card_statement.csv"

	expected := [][]string{
		{"2020-11-29", "Ａｍａｚｏｎ　Ｄｏｗｎｌｏａｄｓ", "288", "", "", "", "", "2020-12-01"},
		{"2020-11-14", "UBER *EATS (HELP.UBER.COM)", "1650", "", "", "", "1650.00　JPY　1.0000　11 16", "2020-12-01"},
		{"2020-11-30", "ゴールドカード年会費", "", "", "", "5500", "（うち消費税等５００円）", "2020-12-01"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SMBCCardStatement)

	name := "path_to/202012.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
