package handlers_test

import (
	"bytes"
	"context"
	"fmt"
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
	t.Helper()

	if len(expected) != len(actual) {
		t.Errorf("expected %d length, but %d", len(expected), len(actual))
	}

	for i := range expected {
		if len(expected[i]) != len(actual[i]) {
			t.Errorf("expected length of actual[%d] is %d, but %d", i, len(expected[i]), len(actual[i]))
		}

		for j := range expected[i] {
			if expected[i][j] != actual[i][j] {
				t.Errorf("expected actual[%d][%d] is '%s', but '%s'", i, j, expected[i][j], actual[i][j])
			}
		}
	}
}

func buildTestHandler(
	t *testing.T,
	csv string,
	f func(string, string, handlers.Table, bqloader.Notifier) *bqloader.Handler,
) (*bqloader.Handler, *testLoader) {
	t.Helper()

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

func Test_PartialCSVParser(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		skipHeadRows uint
		skipTailRows uint
		sep          string
		body         string
		expect       [][]string
	}{
		{
			skipHeadRows: 3,
			skipTailRows: 3,
			sep:          "\n",
			body:         "foo\n\nbar\n1,2,3\n4,5,6\n\nbaz\nqux",
			expect:       [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
		},
		{
			skipHeadRows: 0,
			skipTailRows: 3,
			sep:          "\n",
			body:         "1,2,3\n4,5,6\n\nbaz\nqux",
			expect:       [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
		},
		{
			skipHeadRows: 3,
			skipTailRows: 0,
			sep:          "\n",
			body:         "foo\n\nbar\n1,2,3\n4,5,6",
			expect:       [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
		},
		{
			skipHeadRows: 3,
			skipTailRows: 3,
			sep:          "\r\n",
			body:         "foo\r\n\r\nbar\r\n1,2,3\r\n4,5,6\r\n\r\nbaz\r\nqux",
			expect:       [][]string{{"1", "2", "3"}, {"4", "5", "6"}},
		},
	}

	ctx := context.Background()
	for _, c := range cases {
		c := c
		t.Run(
			fmt.Sprintf("head=%d,tail=%d,sep=%q", c.skipHeadRows, c.skipTailRows, c.sep),
			func(t *testing.T) {
				t.Parallel()

				f := handlers.PartialCSVParser(c.skipHeadRows, c.skipTailRows, c.sep)
				actual, err := f(ctx, bytes.NewReader([]byte(c.body)))

				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				assertEqual(t, c.expect, actual)
			},
		)
	}
}
