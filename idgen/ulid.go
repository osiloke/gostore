package idgen

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// ULIDStrategy generates sortable, server-independent IDs.
type ULIDStrategy struct {
	mu      sync.Mutex
	entropy *ulid.MonotonicEntropy
}

// NewULIDStrategy creates a ULID generator with monotonic entropy.
func NewULIDStrategy() *ULIDStrategy {
	return &ULIDStrategy{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

func (s *ULIDStrategy) Name() string { return "ulid" }

func (s *ULIDStrategy) Generate(_ context.Context) (ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ms := ulid.Timestamp(time.Now())
	id, err := ulid.New(ms, s.entropy)
	if err != nil {
		return ID{}, err
	}

	return ID{
		Raw:      id[:],
		String:   id.String(),
		Time:     ulid.Time(id.Time()),
		Sortable: true, // Guaranteed lexicographic == chronological
	}, nil
}

// Parse converts a ULID string back to ID metadata.
func (s *ULIDStrategy) Parse(idStr string) (ID, error) {
	return ParseULID(idStr)
}

// Time extracts the embedded timestamp from an ID string.
func (s *ULIDStrategy) Time(idStr string) (time.Time, error) {
	parsed, err := ParseULID(idStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.Time, nil
}

// ParseULID converts a ULID string to metadata.
func ParseULID(s string) (ID, error) {
	id, err := ulid.Parse(s)
	if err != nil {
		return ID{}, err
	}
	return ID{
		Raw:      id[:],
		String:   s,
		Time:     ulid.Time(id.Time()),
		Sortable: true,
	}, nil
}
