package handlers_test

import (
	"context"
	"testing"
	"time"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

func Test_parseSMBCDate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		input  string
		expect time.Time
		e      bool
	}{
		{input: "S64.01.07", expect: time.Time{}, e: true},
		{input: "H01.01.08", expect: time.Date(1989, 1, 8, 0, 0, 0, 0, time.UTC), e: false},
		{input: "H31.04.30", expect: time.Date(2019, 4, 30, 0, 0, 0, 0, time.UTC), e: false},
		{input: "R01.05.01", expect: time.Date(2019, 5, 1, 0, 0, 0, 0, time.UTC), e: false},
		{input: "R1.5.2", expect: time.Time{}, e: true},
		{input: "2021/12/17", expect: time.Date(2021, 12, 17, 0, 0, 0, 0, time.UTC), e: false},
		{input: "2021/1/1", expect: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC), e: false},
	}

	for _, c := range cases {
		c := c
		t.Run(c.input, func(t *testing.T) {
			t.Parallel()

			actual, err := handlers.ParseSMBCDate(c.input)

			if c.e {
				if err == nil {
					t.Errorf("Expected error didn't occur")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if !actual.Equal(c.expect) {
					t.Errorf("Expected %s but %s", c.expect, actual)
				}
			}
		})
	}
}

func Test_SMBCStatement(t *testing.T) {
	t.Parallel()

	const csv = "csv/smbc_statement.csv"

	expected := [][]string{
		{"2019-12-04", "10389", "", "カ)ビユ-カ-ド", "124001"},
		{"2019-12-21", "", "160000", "振込　スミトモ タロウ", "284001"},
		{"2019-12-26", "80980", "", "ミツイスミトモカ-ド (カ", "203021"},
	}

	h, tl := buildTestHandler(t, csv, handlers.SMBCStatement)

	name := "path_to/smbc_statement.csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	if err := h.Handle(context.Background(), e); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
