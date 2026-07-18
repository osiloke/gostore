package idgen

import (
	"fmt"
)

// Strategy names.
const (
	StrategyObjectId  = "objectid"
	StrategyULID      = "ulid"
	StrategySnowflake = "snowflake"
)

// Config selects the active strategy.
type Config struct {
	Strategy string
	// ObjectId-specific
	ObjectId ObjectIdOptions
	// Snowflake-specific (future)
	Snowflake SnowflakeOptions
}

// Default returns the default configuration (ObjectId with env-based machine ID).
func Default() Config {
	return Config{
		Strategy: StrategyObjectId,
		ObjectId: ObjectIdOptions{},
	}
}

// New creates a Generator from configuration.
func New(cfg Config) (Generator, error) {
	switch cfg.Strategy {
	case StrategyObjectId:
		return NewObjectIdStrategy(cfg.ObjectId)
	case StrategyULID:
		return NewULIDStrategy(), nil
	case StrategySnowflake:
		return NewSnowflakeStrategy(cfg.Snowflake)
	default:
		return nil, fmt.Errorf("unknown strategy: %s", cfg.Strategy)
	}
}

// Must panics on error. Use for init-time setup.
func Must(g Generator, err error) Generator {
	if err != nil {
		panic(err)
	}
	return g
}
