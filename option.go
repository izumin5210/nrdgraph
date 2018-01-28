package nrdgraph

type Option func(*options)

type options struct {
	host         string
	id           string
	databaseName string
}

// WithHost sets host name to newrelic segment.
func WithHost(host string) Option {
	return func(o *options) {
		o.host = host
	}
}

// WithHost sets ID to newrelic segment.
func WithID(id string) Option {
	return func(o *options) {
		o.id = id
	}
}

// WithHost sets database name to newrelic segment.
func WithDatabaseName(name string) Option {
	return func(o *options) {
		o.databaseName = name
	}
}
