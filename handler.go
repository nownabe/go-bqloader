package bqloader

import (
	"context"
	"log"
	"regexp"

	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
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
type Projector func(int, []string) ([]string, error)

func (h *Handler) match(name string) bool {
	return h.Pattern != nil && h.Pattern.MatchString(name)
}

func (h *Handler) handle(ctx context.Context, e Event) error {
	r, closer, err := h.extractor.extract(ctx, e)
	if err != nil {
		return err
	}
	defer closer()

	if h.Encoding != nil {
		r = transform.NewReader(r, h.Encoding.NewDecoder())
	}

	source, err := h.Parser(ctx, r)
	if err != nil {
		log.Printf("[%s] failed to parse object: %v", h.Name, err)
		return err
	}
	source = source[h.SkipLeadingRows:]

	records := make([][]string, len(source))

	// TODO: Make this loop parallel.
	for i, r := range source {
		record, err := h.Projector(i, r)
		if err != nil {
			log.Printf("[%s] failed to project row %d: %v", h.Name, i+h.SkipLeadingRows, err)
			return err
		}

		records[i] = record
	}

	log.Printf("[%s] DEBUG records = %+v", h.Name, records)

	return h.loader.load(ctx, records)
}
