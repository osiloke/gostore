package idgen

import (
	"context"
	"fmt"
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

// SnowflakeStrategy will implement Generator when needed.
type SnowflakeStrategy struct {
	opts SnowflakeOptions
}

// NewSnowflakeStrategy returns a Snowflake strategy stub.
func NewSnowflakeStrategy(opts SnowflakeOptions) (*SnowflakeStrategy, error) {
	return nil, fmt.Errorf("snowflake not yet implemented")
}

func (s *SnowflakeStrategy) Name() string { return "snowflake" }

func (s *SnowflakeStrategy) Generate(ctx context.Context) (ID, error) {
	return ID{}, fmt.Errorf("snowflake not yet implemented")
}

func (s *SnowflakeStrategy) Parse(idStr string) (ID, error) {
	return ID{}, fmt.Errorf("snowflake not yet implemented")
}

func (s *SnowflakeStrategy) Time(idStr string) (time.Time, error) {
	return time.Time{}, fmt.Errorf("snowflake not yet implemented")
}
