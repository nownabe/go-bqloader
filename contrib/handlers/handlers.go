package handlers

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"io/ioutil"
	"strings"

	"go.nownabe.dev/bqloader"
	"golang.org/x/xerrors"
)

type contextKey string

// Table identifies BigQuery table.
type Table struct {
	Project string
	Dataset string
	Table   string
}

// PartialCSVParser builds a parser for CSV with invalid head and tail lines.
func PartialCSVParser(skipHeadRows uint, skipTailRows uint, sep string) bqloader.Parser {
	return func(_ context.Context, r io.Reader) ([][]string, error) {
		body, err := ioutil.ReadAll(r)
		if err != nil {
			return nil, xerrors.Errorf("failed to read: %w", err)
		}

		lines := strings.Split(string(body), sep)
		csvBody := strings.Join(lines[skipHeadRows:uint(len(lines))-skipTailRows], sep)
		records, err := csv.NewReader(bytes.NewReader([]byte(csvBody))).ReadAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to read as CSV: %w", err)
		}

		return records, nil
	}
}
