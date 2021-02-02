package handlers_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	"go.nownabe.dev/bqloader"
	"go.nownabe.dev/bqloader/contrib/handlers"
)

type testLoader struct {
	result [][]string
}

func (l *testLoader) Load(ctx context.Context, rs [][]string) error {
	l.result = rs
	return nil
}

type testExtractor struct {
	source io.Reader
}

func (e *testExtractor) Extract(_ context.Context, ev bqloader.Event) (io.Reader, func(), error) {
	return e.source, func() {}, nil
}

func assertEqual(t *testing.T, expected [][]string, actual [][]string) {
	if len(expected) != len(actual) {
		t.Errorf("expected %d length, but %d", len(expected), len(actual))
	}

	for i := range expected {
		if len(expected[i]) != len(actual[i]) {
			t.Errorf("expected length of actual[%d] is %d, but %d", i, len(expected[i]), len(actual[i]))
		}

		for j := range expected[i] {
			if expected[i][j] != actual[i][j] {
				t.Errorf("expected actual[%d][%d] is %s, but %s", i, j, expected[i][j], actual[i][j])
			}
		}
	}
}

func Test_RakutenCardStatement(t *testing.T) {
	const csv = "csv/rakuten_card_statement.csv"

	expected := [][]string{
		{"2020-12-03", "foo", "本人", "1回払い", "2750", "0", "2750", "2750", "0", "*", "2020-12-01"},
		{"2020-11-20", "bar", "家族", "1回払い", "9968", "0", "9968", "9968", "0", "*", "2020-12-01"},
		{"2020-11-20", "baz", "本人", "1回払い", "1570", "0", "1570", "1570", "0", "*", "2020-12-01"},
	}

	body, err := ioutil.ReadFile(csv)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	tl := &testLoader{}

	table := handlers.Table{Project: "p", Dataset: "d", Table: "t"}
	h := handlers.RakutenCardStatement("name", "^path_to/", table, nil)
	h.SetConcurrency(1)
	h.BatchSize = 100
	h.Loader = tl
	h.Extractor = &testExtractor{source: bytes.NewBuffer(body)}

	name := "path_to/enavi202012(1234).csv"
	e := bqloader.Event{Name: name, Bucket: "bucket"}

	err = h.Handle(context.Background(), e)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	assertEqual(t, expected, tl.result)
}
