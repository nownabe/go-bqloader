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

func buildTestHandler(
	t *testing.T,
	csv string,
	f func(string, string, handlers.Table, bqloader.Notifier) *bqloader.Handler,
) (*bqloader.Handler, *testLoader) {
	body, err := ioutil.ReadFile(csv)
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}

	tl := &testLoader{}
	table := handlers.Table{Project: "p", Dataset: "d", Table: "t"}

	h := f("name", "^path_to/", table, nil)
	h.SetConcurrency(1)
	h.BatchSize = 100
	h.Loader = tl
	h.Extractor = &testExtractor{source: bytes.NewBuffer(body)}

	return h, tl
}
