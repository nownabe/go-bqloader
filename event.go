package bqloader

import (
	"fmt"
	"io"

	"github.com/rs/zerolog"
)

// Event is an event from Cloud Storage.
type Event struct {
	Name   string `json:"name"`
	Bucket string `json:"bucket"`

	// for test
	source io.Reader
}

// FullPath returns full path of storage object beginning with gs://.
func (e *Event) FullPath() string {
	return fmt.Sprintf("gs://%s/%s", e.Bucket, e.Name)
}

func (e *Event) logger(l zerolog.Logger) zerolog.Logger {
	d := zerolog.Dict().
		Str("name", e.Name).
		Str("bucket", e.Bucket)

	return l.With().Dict("event", d).Logger()
}
