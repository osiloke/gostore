package idgen

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// SnowflakeOptions configures the 64-bit Snowflake generator.
type SnowflakeOptions struct {
	// Epoch is the custom epoch in milliseconds (default: 2021-01-01).
	Epoch int64
	// DatacenterID is the datacenter identifier (0-31).
	DatacenterID int64
	// MachineID is the machine identifier within datacenter (0-31).
	MachineID int64
}

// SnowflakeStrategy implements the Snowflake ID strategy.
type SnowflakeStrategy struct {
	mu           sync.Mutex
	epoch        int64
	datacenterID int64
	machineID    int64
	sequence     int64
	lastTime     int64
}

// NewSnowflakeStrategy creates a new SnowflakeStrategy.
func NewSnowflakeStrategy(opts SnowflakeOptions) (*SnowflakeStrategy, error) {
	epoch := opts.Epoch
	if epoch == 0 {
		epoch = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	}

	if opts.DatacenterID < 0 || opts.DatacenterID > 31 {
		return nil, fmt.Errorf("datacenter ID must be between 0 and 31")
	}

	if opts.MachineID < 0 || opts.MachineID > 31 {
		return nil, fmt.Errorf("machine ID must be between 0 and 31")
	}

	return &SnowflakeStrategy{
		epoch:        epoch,
		datacenterID: opts.DatacenterID,
		machineID:    opts.MachineID,
		lastTime:     -1,
	}, nil
}

func (s *SnowflakeStrategy) Name() string { return "snowflake" }

func (s *SnowflakeStrategy) Generate(ctx context.Context) (ID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()

	if now < s.lastTime {
		return ID{}, fmt.Errorf("clock moved backwards: refuse to generate id for %d milliseconds", s.lastTime-now)
	}

	if now == s.lastTime {
		s.sequence = (s.sequence + 1) & 4095
		if s.sequence == 0 {
			// Sequence overflow, spin wait for next millisecond
			for now <= s.lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTime = now

	// Calculate bits
	// 41 bits for timestamp
	timeShift := uint64(22)
	datacenterShift := uint64(17)
	machineShift := uint64(12)

	timestampPart := uint64(now - s.epoch)
	idVal := (timestampPart << timeShift) |
		(uint64(s.datacenterID) << datacenterShift) |
		(uint64(s.machineID) << machineShift) |
		uint64(s.sequence)

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], idVal)

	idStr := strconv.FormatUint(idVal, 10)
	embeddedTime := time.UnixMilli(s.epoch + int64(timestampPart))

	return ID{
		Raw:      b[:],
		String:   idStr,
		Time:     embeddedTime,
		Sortable: true,
	}, nil
}

func (s *SnowflakeStrategy) Parse(idStr string) (ID, error) {
	return ParseSnowflake(idStr, s.epoch)
}

func (s *SnowflakeStrategy) Time(idStr string) (time.Time, error) {
	parsed, err := ParseSnowflake(idStr, s.epoch)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.Time, nil
}

// ParseSnowflake converts a Snowflake decimal string back to ID metadata.
func ParseSnowflake(s string, epoch int64) (ID, error) {
	idVal, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return ID{}, fmt.Errorf("invalid snowflake ID format: %w", err)
	}

	timestampPart := int64(idVal >> 22)
	embeddedTime := time.UnixMilli(epoch + timestampPart)

	var b [8]byte
	binary.BigEndian.PutUint64(b[:], idVal)

	return ID{
		Raw:      b[:],
		String:   s,
		Time:     embeddedTime,
		Sortable: true,
	}, nil
}
