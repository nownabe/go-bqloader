package bqloader

import (
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"
)

// Event is an event from Cloud Storage.
type Event struct {
	Name        string    `json:"name"`
	Bucket      string    `json:"bucket"`
	TimeCreated time.Time `json:"timeCreated"`

	// for test
	source io.Reader
}

// FullPath returns full path of storage object beginning with gs://.
func (e *Event) FullPath() string {
	return fmt.Sprintf("gs://%s/%s", e.Bucket, e.Name)
}

// TODO: Add metadata context here.
func (e *Event) logger(l *zerolog.Logger) *zerolog.Logger {
	d := zerolog.Dict().
		Str("name", e.Name).
		Str("bucket", e.Bucket).
		Time("timeCreated", e.TimeCreated)

	logger := l.With().Dict("event", d).Logger()
	return &logger
}
