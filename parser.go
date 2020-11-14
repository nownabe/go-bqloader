package bqloader

import (
	"context"
	"encoding/csv"
	"io"

	"golang.org/x/xerrors"
)

// Parser parses files from storage.
type Parser func(context.Context, io.Reader) ([][]string, error)

// CSVParser provides a parser to parse CSV files.
func CSVParser() Parser {
	return func(_ context.Context, r io.Reader) ([][]string, error) {
		records, err := csv.NewReader(r).ReadAll()
		if err != nil {
			return nil, xerrors.Errorf("failed to parse as CSV: %w", err)
		}
		return records, nil
	}
}
