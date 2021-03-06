package bqloader

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

func Test_Handler_WithSkipping(t *testing.T) {
	t.Parallel()

	projector := func(_ context.Context, r []string) ([]string, error) {
		if r[0] == "" {
			return nil, nil
		}

		return r, nil
	}

	rawCSV := `123,456,789
,foo bar,123
234,567,890`
	src := bytes.NewBufferString(rawCSV)

	tl := newTestLoader()

	handler := &Handler{
		Name:      "test-handler",
		Parser:    CSVParser(),
		Projector: projector,
		BatchSize: defaultBatchSize,
		Extractor: newTestExtractor(),
		Loader:    tl,
		semaphore: make(chan struct{}, 1),
	}
	e := Event{Name: "test/name", Bucket: "bucket", source: src}

	err := handler.Handle(context.Background(), e)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	res := tl.(*testLoader)

	if len(res.result) != 2 {
		t.Fatalf("Size of result records should be 2, but %d", len(res.result))
	}

	if len(res.result[0]) != 3 {
		t.Fatalf("Size of each record be 3, but %d", len(res.result[0]))
	}

	if res.result[0][0] != "123" {
		t.Errorf(`results[0][0] should be "123", but "%s"`, res.result[0][0])
	}

	if res.result[0][1] != "456" {
		t.Errorf(`results[0][1] should be "456", but "%s"`, res.result[0][1])
	}

	if res.result[0][2] != "789" {
		t.Errorf(`results[0][2] should be "789", but "%s"`, res.result[0][2])
	}

	if len(res.result[1]) != 3 {
		t.Fatalf("Size of each record be 3, but %d", len(res.result[1]))
	}

	if res.result[1][0] != "234" {
		t.Errorf(`results[1][0] should be "234", but "%s"`, res.result[1][0])
	}

	if res.result[1][1] != "567" {
		t.Errorf(`results[1][1] should be "567", but "%s"`, res.result[1][1])
	}

	if res.result[1][2] != "890" {
		t.Errorf(`results[1][2] should be "890", but "%s"`, res.result[1][2])
	}
}

func Test_Handler_WithPreprocessor(t *testing.T) {
	t.Parallel()

	projector := func(ctx context.Context, r []string) ([]string, error) {
		prefix, ok := ctx.Value("prefix").(string)
		if !ok {
			return nil, fmt.Errorf("prefix is not found")
		}

		row := make([]string, 4)
		row[0] = prefix
		row[1] = r[0]
		row[2] = r[1]
		row[3] = r[2]

		return row, nil
	}

	preprocessor := func(ctx context.Context, e Event) (context.Context, error) {
		prefix := strings.Split(e.Name, "/")[0]
		return context.WithValue(ctx, "prefix", prefix), nil
	}

	src := bytes.NewBufferString(`123,456,789`)

	tl := newTestLoader()

	handler := &Handler{
		Name:         "test-handler",
		Parser:       CSVParser(),
		Projector:    projector,
		Preprocessor: preprocessor,
		BatchSize:    defaultBatchSize,
		Extractor:    newTestExtractor(),
		Loader:       tl,
		semaphore:    make(chan struct{}, 1),
	}
	e := Event{Name: "test/name", Bucket: "bucket", source: src}

	err := handler.Handle(context.Background(), e)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	res := tl.(*testLoader)

	if len(res.result) != 1 {
		t.Fatalf("Size of result records should be 1, but %d", len(res.result))
	}

	if len(res.result[0]) != 4 {
		t.Fatalf("Size of each record be 4, but %d", len(res.result[0]))
	}

	if res.result[0][0] != "test" {
		t.Errorf(`results[0][0] should be "test", but "%s"`, res.result[0][0])
	}

	if res.result[0][1] != "123" {
		t.Errorf(`results[0][0] should be "123", but "%s"`, res.result[0][1])
	}

	if res.result[0][2] != "456" {
		t.Errorf(`results[0][1] should be "456", but "%s"`, res.result[0][2])
	}

	if res.result[0][3] != "789" {
		t.Errorf(`results[0][2] should be "789", but "%s"`, res.result[0][3])
	}
}
