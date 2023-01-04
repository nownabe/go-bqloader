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

func Test_SMBCCardStatement2(t *testing.T) {
	t.Parallel()

	const csv = "testdata/smbc_card_statement2.csv"

	expected := [][]string{
		{"2022-10-31", "インターネットイニシアティブ", "5989", "", "", "", "", "2022-12-01"},
		{"2022-11-01", "ＡＭＡＺＯＮ．ＣＯ．ＪＰ", "17673", "", "", "", "", "2022-12-01"},
		{"2022-11-05", "ＡＭＡＺＯＮ．ＣＯ．ＪＰ", "2490", "", "", "", "", "2022-12-01"},
		{"2022-11-05", "Ａｍａｚｏｎ　Ｄｏｗｎｌｏａｄｓ", "594", "", "", "", "", "2022-12-01"},
		{"2022-11-13", "ＡＭＡＺＯＮ．ＣＯ．ＪＰ", "-500", "", "", "", "返品", "2022-12-01"},
		{"2022-11-13", "ＡＭＡＺＯＮ．ＣＯ．ＪＰ", "-17673", "", "", "", "返品", "2022-12-01"},
		{"2022-11-30", "Ａｍａｚｏｎ　Ｄｏｗｎｌｏａｄｓ", "673", "", "", "", "", "2022-12-01"},
		{"2022-11-29", "ABC-COMPANY (SERVICE )", "3507", "", "", "", "24.90　USD　140.873　11 30", "2022-12-01"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SMBCCardStatement)

	name := "path_to/202212.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
