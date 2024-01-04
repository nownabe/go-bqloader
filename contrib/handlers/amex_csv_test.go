package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_AMEXStatementCSV(t *testing.T) {
	t.Parallel()

	const csv = "testdata/amex_statement.csv"

	expected := [][]string{
		{"2023-07-10", "2023-07-10", "前回分口座振替金額", "TARO AMEX", "-4048", "", "", "", "2023-08-01"},
		{"2023-07-08", "2023-07-09", "UBER EATS", "TARO AMEX", "1408", "", "", "", "2023-08-01"},
		{"2023-07-03", "2023-07-04", "GITHUB, INC.", "TARO AMEX", "-1431", "9.68 USD", "147.831", "", "2023-08-01"},
		{"2023-07-02", "2023-07-03", "GITHUB, INC.", "TARO AMEX", "1479", "10.00 USD", "147.9", "", "2023-08-01"},
	}

	h, tl := buildTestHandler(t, csv, handlers.AMEXStatementCSV)

	name := "path_to/2023-08.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
