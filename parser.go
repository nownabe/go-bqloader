package bqloader

import (
	"context"
	"encoding/csv"
	"io"
)

// Parser parses files from storage.
type Parser func(context.Context, io.Reader) ([][]string, error)

// CSVParser provides a parser to parse CSV files.
var CSVParser Parser

func init() {
	CSVParser = func(_ context.Context, r io.Reader) ([][]string, error) {
		return csv.NewReader(r).ReadAll()
	}
}
