package bqloader

import "github.com/rs/zerolog"

// Option configures BQLoader.
type Option interface {
	apply(*bqloader) error
}

type optionFunc func(*bqloader) error

func (f optionFunc) apply(l *bqloader) error {
	return f(l)
}

// WithPrettyLogging configures BQLoader to print human friendly logs.
func WithPrettyLogging() Option {
	return optionFunc(func(l *bqloader) error {
		l.prettyLogging = true
		return nil
	})
}

// WithLogLevel configures log level to print logs.
// Allowed values are trace, debug, info, warn, error, fatal or panic.
func WithLogLevel(l string) Option {
	return optionFunc(func(bq *bqloader) error {
		l, err := zerolog.ParseLevel(l)
		if err != nil {
			return err
		}
		bq.logLevel = l
		return nil
	})
}
