package idgen

import (
	"context"
	"time"
)

// Generator is the abstraction for all ID strategies.
type Generator interface {
	// Generate creates a new unique ID.
	Generate(ctx context.Context) (ID, error)

	// Name returns the strategy identifier (e.g., "objectid", "ulid").
	Name() string
}

// ID is the generated identifier with metadata.
type ID struct {
	// Raw is the canonical binary/bytes representation.
	Raw []byte

	// String is the human-readable encoding (hex, base32, etc.).
	String string

	// Time returns the embedded timestamp, if any.
	Time time.Time

	// Sortable indicates whether lexicographic sort equals chronological sort.
	Sortable bool
}

// Extractor extracts metadata from existing IDs.
type Extractor interface {
	// Parse converts a string representation back to ID metadata.
	Parse(string) (ID, error)

	// Time extracts the embedded timestamp from an ID string.
	Time(string) (time.Time, error)
}
