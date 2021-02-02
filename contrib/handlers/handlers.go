package handlers

type contextKey string

// Table identifies BigQuery table.
type Table struct {
	Project string
	Dataset string
	Table   string
}
