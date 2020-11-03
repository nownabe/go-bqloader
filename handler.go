package bqloader

import (
	"context"
	"io"
	"regexp"

	"golang.org/x/text/encoding"
)

// Handler defines how to handle events which match specified pattern.
type Handler struct {
	// Name is the handler's name.
	Name string

	Pattern         *regexp.Regexp
	Encoding        encoding.Encoding
	Parser          Parser
	Projector       Projector
	SkipLeadingRows int

	// Project specifies GCP project name of destination BigQuery table.
	Project string

	// Dataset specifies BigQuery dataset ID of destination table
	Dataset string

	// Table specifies BigQuery table ID as destination.
	Table string

	extractor extractor
	loader    loader
}

// Projector transforms source records into records for destination.
type Projector func([]string) ([]string, error)

func (h *Handler) match(name string) bool {
	return h.Pattern != nil && h.Pattern.MatchString(name)
}

// extractor extracts data from source such as Cloud Storage.
type extractor interface {
	extract(context.Context, Event) (io.Reader, error)
}

// loader loads projected data into a destination such as BigQuery.
type loader interface {
	load(context.Context, [][]string) error
}
