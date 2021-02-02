package handlers_test

import (
	"context"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_RakutenCardStatement(t *testing.T) {
	const csv = "csv/rakuten_card_statement.csv"

	expected := [][]string{
		{"2020-12-03", "foo", "本人", "1回払い", "2750", "0", "2750", "2750", "0", "*", "2020-12-01"},
		{"2020-11-20", "bar", "家族", "1回払い", "9968", "0", "9968", "9968", "0", "*", "2020-12-01"},
		{"2020-11-20", "baz", "本人", "1回払い", "1570", "0", "1570", "1570", "0", "*", "2020-12-01"},
	}

	h, tl := buildTestHandler(t, csv, handlers.RakutenCardStatement)

	name := "path_to/enavi202012(1234).csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
