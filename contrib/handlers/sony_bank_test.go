package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_SonyBankStatement(t *testing.T) {
	t.Parallel()

	const csv = "testdata/sony_bank_statement.csv"

	expected := [][]string{
		{"2020-12-12", "積み立て定期預金へ振替", "", "", "10000", "661450"},
		{"2020-12-15", "振込 ソニー　タロウ", "", "220000", "", "881450"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SonyBankStatement)

	name := "path_to/sony_bank_statement.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
