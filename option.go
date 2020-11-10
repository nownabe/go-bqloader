package bqloader

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
