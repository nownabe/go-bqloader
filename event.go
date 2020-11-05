package bqloader

import (
	"fmt"
	"io"
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
