package bqloader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"
)

func TestLoader(t *testing.T) {
	projector := func(_ int, r []string) ([]string, error) {
		t, err := time.Parse("2006/01/02", r[0])
		if err != nil {
			return nil, fmt.Errorf("Failed to parse date: %v", err)
		}

		r[0] = t.Format("2006-01-02")

		return r, nil
	}

	te := newTestExtractor()
	tl := newTestLoader()
	tn := newTestNotifier()

	handler := &Handler{
		Name:      "test-handler",
		Pattern:   regexp.MustCompile("^test/"),
		Parser:    CSVParser(),
		Notifier:  tn,
		Projector: projector,
		extractor: te,
		loader:    tl,
	}

	ctx := context.Background()

	loader, err := New(WithPrettyLogging(), WithLogLevel("debug"), WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	loader.MustAddHandler(ctx, handler)

	src := bytes.NewBufferString("2020/11/21,foo,123")
	e := Event{Name: "test/name", Bucket: "bucket", source: src}

	if err := loader.Handle(ctx, e); err != nil {
		t.Fatal(err)
	}

	res := tl.(*testLoader)

	if len(res.result) != 1 {
		t.Fatalf("Size of result records should be 1, but %d.", len(res.result))
	}

	if len(res.result[0]) != 3 {
		t.Fatalf("Size of each record be 3, but %d", len(res.result[0]))
	}

	if res.result[0][0] != "2020-11-21" {
		t.Errorf(`results[0][0] should be "2020-11-21", but "%s"`, res.result[0][0])
	}

	if res.result[0][1] != "foo" {
		t.Errorf(`results[0][1] should be "foo", but "%s"`, res.result[0][1])
	}

	if res.result[0][2] != "123" {
		t.Errorf(`results[0][2] should be "123", but "%s"`, res.result[0][2])
	}
}

func TestBQLoader_error(t *testing.T) {
	projector := func(_ int, r []string) ([]string, error) {
		return nil, fmt.Errorf("projector error")
	}

	te := newTestExtractor()
	tl := newTestLoader()
	tn := newTestNotifier()

	handler := &Handler{
		Name:      "test-handler",
		Pattern:   regexp.MustCompile("^test/"),
		Parser:    CSVParser(),
		Notifier:  tn,
		Projector: projector,
		extractor: te,
		loader:    tl,
	}

	ctx := context.Background()

	loader, err := New(WithPrettyLogging(), WithLogLevel("debug"))
	if err != nil {
		t.Fatal(err)
	}
	loader.MustAddHandler(ctx, handler)

	src := bytes.NewBufferString("2020/11/21,foo,123")
	e := Event{Name: "test/name", Bucket: "bucket", source: src}

	if err := loader.Handle(ctx, e); err == nil {
		t.Error("expected error but no error occurred")
	}
}

type testExtractor struct{}

func newTestExtractor() extractor {
	return &testExtractor{}
}

func (e *testExtractor) extract(_ context.Context, ev Event) (io.Reader, func(), error) {
	return ev.source, func() {}, nil
}

type testLoader struct {
	result [][]string
}

func newTestLoader() loader {
	return &testLoader{}
}

func (l *testLoader) load(ctx context.Context, rs [][]string) error {
	l.result = rs
	return nil
}

type testNotifier struct{}

func newTestNotifier() Notifier {
	return &testNotifier{}
}

func (n *testNotifier) Notify(ctx context.Context, r *Result) error {
	return nil
}
