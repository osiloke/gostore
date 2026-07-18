package idgen

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"
)

// ObjectIdStrategy implements the MongoDB ObjectId spec with stable machine ID.
type ObjectIdStrategy struct {
	machineID [3]byte
	pid       uint16
	counter   uint32
}

// NewObjectIdStrategy creates a generator with configurable machine ID source.
func NewObjectIdStrategy(opts ObjectIdOptions) (*ObjectIdStrategy, error) {
	machineID, err := resolveMachineID(opts.MachineID)
	if err != nil {
		return nil, fmt.Errorf("resolve machine id: %w", err)
	}

	return &ObjectIdStrategy{
		machineID: machineID,
		pid:       uint16(os.Getpid()),
		counter:   0,
	}, nil
}

func (s *ObjectIdStrategy) Name() string { return "objectid" }

func (s *ObjectIdStrategy) Generate(_ context.Context) (ID, error) {
	var b [12]byte

	// Timestamp, 4 bytes, big endian
	binary.BigEndian.PutUint32(b[:4], uint32(time.Now().Unix()))

	// Machine ID (stable)
	b[4] = s.machineID[0]
	b[5] = s.machineID[1]
	b[6] = s.machineID[2]

	// PID, 2 bytes, big endian
	b[7] = byte(s.pid >> 8)
	b[8] = byte(s.pid)

	// Counter, 3 bytes, big endian
	i := atomic.AddUint32(&s.counter, 1)
	b[9] = byte(i >> 16)
	b[10] = byte(i >> 8)
	b[11] = byte(i)

	return ID{
		Raw:      b[:],
		String:   hex.EncodeToString(b[:]),
		Time:     time.Unix(int64(binary.BigEndian.Uint32(b[:4])), 0),
		Sortable: true, // Roughly sortable by time, with machine ID caveats
	}, nil
}

// Parse converts a hex string representation back to ID metadata.
func (s *ObjectIdStrategy) Parse(idStr string) (ID, error) {
	return ParseObjectId(idStr)
}

// Time extracts the embedded timestamp from an ID string.
func (s *ObjectIdStrategy) Time(idStr string) (time.Time, error) {
	parsed, err := ParseObjectId(idStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.Time, nil
}

// ObjectIdOptions configures the ObjectId generator.
type ObjectIdOptions struct {
	// MachineID overrides the machine identifier source.
	// If empty, falls back to GATEWAY_MACHINE_ID env var, then hostname.
	MachineID string
}

func resolveMachineID(override string) ([3]byte, error) {
	var id [3]byte

	// Priority 1: explicit override
	if override != "" {
		sum := md5.Sum([]byte(override))
		copy(id[:], sum[:3])
		return id, nil
	}

	// Priority 2: environment variable
	if envID := os.Getenv("GATEWAY_MACHINE_ID"); envID != "" {
		sum := md5.Sum([]byte(envID))
		copy(id[:], sum[:3])
		return id, nil
	}

	// Priority 3: hostname (legacy behavior)
	hostname, err := os.Hostname()
	if err != nil {
		_, err2 := io.ReadFull(rand.Reader, id[:])
		if err2 != nil {
			return id, fmt.Errorf("cannot get hostname: %v; %v", err, err2)
		}
		return id, nil
	}

	sum := md5.Sum([]byte(hostname))
	copy(id[:], sum[:3])
	return id, nil
}

// ParseObjectId converts a hex string to ID metadata.
func ParseObjectId(s string) (ID, error) {
	if len(s) != 24 {
		return ID{}, errors.New("invalid objectid length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, err
	}
	if len(b) != 12 {
		return ID{}, errors.New("invalid objectid bytes")
	}

	secs := int64(binary.BigEndian.Uint32(b[:4]))
	return ID{
		Raw:      b,
		String:   s,
		Time:     time.Unix(secs, 0),
		Sortable: true,
	}, nil
}
