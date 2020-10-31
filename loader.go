package bqloader

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"golang.org/x/text/transform"
)

// Loader loads data from Cloud Storage to BigQuery table.
type Loader interface {
	AddHandler(*Handler)
	Handle(context.Context, Event) error
}

// Event is an event from Cloud Storage.
type Event struct {
	Name string `json:"name"`

	Kind                    string                 `json:"kind"`
	ID                      string                 `json:"id"`
	SelfLink                string                 `json:"selfLink"`
	Bucket                  string                 `json:"bucket"`
	Generation              string                 `json:"generation"`
	Metageneration          string                 `json:"metageneration"`
	ContentType             string                 `json:"contentType"`
	TimeCreated             time.Time              `json:"timeCreated"`
	Updated                 time.Time              `json:"updated"`
	TemporaryHold           bool                   `json:"temporaryHold"`
	EventBasedHold          bool                   `json:"eventBasedHold"`
	RetentionExpirationTime time.Time              `json:"retentionExpirationTime"`
	StorageClass            string                 `json:"storageClass"`
	TimeStorageClassUpdated time.Time              `json:"timeStorageClassUpdated"`
	Size                    string                 `json:"size"`
	MD5Hash                 string                 `json:"md5Hash"`
	MediaLink               string                 `json:"mediaLink"`
	ContentEncoding         string                 `json:"contentEncoding"`
	ContentDisposition      string                 `json:"contentDisposition"`
	CacheControl            string                 `json:"cacheControl"`
	Metadata                map[string]interface{} `json:"metadata"`
	CRC32C                  string                 `json:"crc32c"`
	ComponentCount          int                    `json:"componentCount"`
	Etag                    string                 `json:"etag"`
	CustomerEncryption      struct {
		EncryptionAlgorithm string `json:"encryptionAlgorithm"`
		KeySha256           string `json:"keySha256"`
	} `json:"customerEncryption"`
	KMSKeyName    string `json:"kmsKeyName"`
	ResourceState string `json:"resourceState"`
}

// New build a new Loader.
func New() Loader {
	return &loader{
		handlers: []*Handler{},
		mu:       sync.RWMutex{},
	}
}

type loader struct {
	handlers []*Handler
	mu       sync.RWMutex
}

func (l *loader) AddHandler(h *Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.handlers = append(l.handlers, h)
}

func (l *loader) Handle(ctx context.Context, e Event) error {
	log.Printf("loader started")
	defer log.Printf("loader finished")

	log.Printf("file name = %s", e.Name)

	for _, h := range l.handlers {
		log.Printf("handler = %+v", h)
		if h.match(e.Name) {
			log.Printf("handler matches")
			if err := l.handle(ctx, e, h); err != nil {
				log.Printf("error: %v", err)
				return err
			}
		}
	}

	return nil
}

func (l *loader) handle(ctx context.Context, e Event, h *Handler) error {
	sc, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bq, err := bigquery.NewClient(ctx, h.Project)
	if err != nil {
		return err
	}

	obj := sc.Bucket(e.Bucket).Object(e.Name)
	objr, err := obj.NewReader(ctx)
	if err != nil {
		log.Printf("[%s] failed to initialize object reader: %v", h.Name, err)
		return err
	}
	defer objr.Close()
	log.Printf("[%s] DEBUG objr = %+v", h.Name, objr)

	var r io.Reader
	if h.Encoding != nil {
		r = transform.NewReader(objr, h.Encoding.NewDecoder())
	} else {
		r = objr
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
		record, err := h.Projector(r)
		if err != nil {
			log.Printf("[%s] failed to project row %d: %v", h.Name, i+h.SkipLeadingRows, err)
			return err
		}

		records[i] = record
	}

	log.Printf("[%s] DEBUG records = %+v", h.Name, records)

	// TODO: Make output format more efficient. e.g. gzip.
	buf := &bytes.Buffer{}
	if err := csv.NewWriter(buf).WriteAll(records); err != nil {
		log.Printf("[%s] failed to write csv: %v", h.Name, err)
		return err
	}

	table := bq.Dataset(h.Dataset).Table(h.Table)
	rs := bigquery.NewReaderSource(buf)
	loader := table.LoaderFrom(rs)

	job, err := loader.Run(ctx)
	if err != nil {
		log.Printf("[%s] failed to run bigquery load job: %v", h.Name, err)
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("[%s] failed to wait job: %v", h.Name, err)
		return err
	}

	if status.Err() != nil {
		log.Printf("[%s] failed to load csv: %v", h.Name, status.Errors)
		return status.Err()
	}

	return nil
}
