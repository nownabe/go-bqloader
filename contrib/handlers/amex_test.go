package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_AMEXStatement(t *testing.T) {
	t.Parallel()

	const xls = "testdata/amex_statement.xls"

	expected := [][]string{
		{"2022-06-19", "2022-06-20", "GOOGLE *DOMAINS", "TARO AMEX", "1760", "", "", "", "2022-07-01"},
		{"2022-05-29", "2022-05-29", "IWANTMYNAME IWANTMYNAME", "TARO AMEX", "129", "1.00 USD", "129", "", "2022-07-01"},
		{"2022-05-29", "2022-05-29", "IWANTMYNAME IWANTMYNAME", "", "-129", "1.00 USD", "129", "", "2022-07-01"},
	}

	h, tl := buildTestHandler(t, xls, handlers.AMEXStatement)

	name := "path_to/2022-07.xls"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
