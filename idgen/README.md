# ID Generator Abstraction (`idgen`)

This package provides a unified, highly concurrent, and extensible unique ID generation abstraction layer supporting multiple strategies like MongoDB-compatible ObjectIds, ULIDs, and Snowflake IDs.

## Features

1. **Strategy Pattern (`Generator` interface)**: Swap ID generation strategies easily with zero modifications to your business logic call sites.
2. **Robust ObjectId Generation**: Resolves machine identifier stability issues by resolving via optional overrides, the `GATEWAY_MACHINE_ID` environment variable, or falling back to the standard hostname md5 sum.
3. **Monotonic thread-safe ULIDs**: Leveraging monotonic entropy source with mutex synchronization to guarantee thread safety and chronological sort order under heavy concurrency.
4. **Customizable Snowflake Generator**: Full standard Snowflake generator with a 41-bit millisecond timestamp, 5-bit datacenter ID, 5-bit machine ID, and 12-bit sequence structure, featuring customized epoch and strict clock validation.
5. **ID Extraction (`Extractor` interface)**: Full parsing and metadata extraction to retrieve embedded timestamp, sortability characteristics, and raw representations easily across all formats.

---

## Core Types and Interfaces

```go
// Generator is the abstraction for all ID strategies.
type Generator interface {
	Generate(ctx context.Context) (ID, error)
	Name() string
}

// ID represents a generated unique identifier with metadata.
type ID struct {
	Raw      []byte    // Canonical binary/bytes representation.
	String   string    // Human-readable encoding (hex, base32, decimal).
	Time     time.Time // Embedded timestamp.
	Sortable bool      // Whether lexicographical sort matches chronological order.
}

// Extractor extracts metadata from existing ID strings.
type Extractor interface {
	Parse(string) (ID, error)
	Time(string) (time.Time, error)
}
```

---

## Usage Examples

### 1. Initialization and Configuration

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/osiloke/gostore/idgen"
)

func main() {
	// Configure active strategy (e.g. ULID)
	cfg := idgen.Config{
		Strategy: idgen.StrategyULID,
	}

	// Create generator using the factory pattern
	gen, err := idgen.New(cfg)
	if err != nil {
		log.Fatalf("failed to create generator: %v", err)
	}

	// Generate a unique ID
	id, err := gen.Generate(context.Background())
	if err != nil {
		log.Fatalf("failed to generate ID: %v", err)
	}

	fmt.Printf("Active Strategy: %s\n", gen.Name())
	fmt.Printf("Generated ID:    %s\n", id.String)
	fmt.Printf("Timestamp:       %v\n", id.Time)
}
```

### 2. Supported Strategies

#### ObjectId Strategy (`"objectid"`)
Generates 12-byte (24-hex-char) MongoDB-compatible IDs. Supports stable machine ID resolution.
```go
cfg := idgen.Config{
	Strategy: idgen.StrategyObjectId,
	ObjectId: idgen.ObjectIdOptions{
		MachineID: "stable-prod-gw-01", // Optional explicit override
	},
}
```

#### ULID Strategy (`"ulid"`)
Generates 16-byte (26-character Base32) monotonic, server-independent, lexicographically sortable IDs.
```go
cfg := idgen.Config{
	Strategy: idgen.StrategyULID,
}
```

#### Snowflake Strategy (`"snowflake"`)
Generates 64-bit integer IDs (serialized as decimal string) with custom epoch and worker coordinates.
```go
cfg := idgen.Config{
	Strategy: idgen.StrategySnowflake,
	Snowflake: idgen.SnowflakeOptions{
		Epoch:        time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		DatacenterID: 2,
		MachineID:    15,
	},
}
```

---

## Thread Safety

All strategies are designed with strict synchronization mechanisms (`atomic` and `sync.Mutex`) to be entirely safe for concurrent use across multiple goroutines.
