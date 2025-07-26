# Progressive Migration Store

The `ProgressiveMigrationStore` is a Go package that provides a store that progressively migrates data from a primary to a secondary store. It is designed to be used in a live, zero-downtime migration scenario.

## Architecture

The `ProgressiveMigrationStore` has three main components:

*   **Primary Store**: The existing, legacy data store. This is the initial source of truth.
*   **Secondary Store**: The new destination data store, which may have a different architecture (e.g., sharded or pooled).
*   **Metadata Store**: A separate, reliable key-value store used to track the migration status of data partitions.

### Partitioning

The entire keyspace is divided into logical `partitions` or `shards`. The store must be configured with a `PartitionFunc`, which is a function that determines the partition ID for a given key. This allows for flexible partitioning strategies based on the needs of the application.

### Migration State

The `MetadataStore` tracks the state for each `partitionID`. The possible states are:

*   `NotMigrated`: The default state. No data has been migrated for this partition.
*   `MigrationInProgress`: A background task is actively copying this partition from the primary to the secondary store.
*   `Migrated`: The partition has been fully copied and validated.

### Read Path (On-Demand Migration)

When a `Get(key)` request is received, the following steps are taken:

1.  The `partitionID` for the key is determined using the `PartitionFunc`.
2.  The `MetadataStore` is checked for the partition's status.
3.  If the status is `Migrated`, the request is served directly from the `SecondaryStore`.
4.  If the status is `NotMigrated` or `MigrationInProgress`, the request is served from the `PrimaryStore` to minimize latency. If the status was `NotMigrated`, a non-blocking goroutine is started to perform a full migration of all keys within that partition.

This on-demand migration strategy ensures that data is migrated only when it is needed, which can significantly reduce the initial migration time and resource consumption.

### Write Path (Dual-Write)

All write operations (`Save`, `Update`, `Delete`, and their batch equivalents) are performed on both the `PrimaryStore` and the `SecondaryStore`. This dual-write strategy ensures that new and updated data is immediately consistent across both stores.

The operation is considered successful only if the write to the `PrimaryStore` succeeds. A failure to write to the `SecondaryStore` is logged and can be handled by a separate reconciliation process, but it will not fail the client request.

### Background Backfill

In addition to on-demand migration, the `ProgressiveMigrationStore` provides a background backfill process. This process systematically iterates through all `NotMigrated` partitions and triggers the migration process for them, independent of read traffic. This ensures that data that is not actively read is also eventually migrated.

The backfill process can be started by calling the `StartBackfill` method.

## Usage

The following examples demonstrate how to create and use a `ProgressiveMigrationStore`.

### Basic Usage

This example shows a simple migration from one memory store to another.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gostore/gostore/common"
	"github.com/gostore/gostore/stores/memory"
	"github.com/gostore/gostore/stores/progressive"
)

func main() {
	// Create the primary, secondary, and metadata stores.
	primary := memory.NewMemoryStore()
	secondary := memory.NewMemoryStore()
	metadata := memory.NewMemoryStore()

	// Create the progressive migration store.
	store := progressive.New(
		primary,
		secondary,
		metadata,
		func(key string) string {
			// A simple partitioner that uses the first character of the key.
			if len(key) > 0 {
				return string(key[0])
			}
			return ""
		},
		common.Logger("test"),
	)

	// Start the background backfill process.
	go store.StartBackfill(context.Background())

	// Write some data to the store.
	store.Save("a-key", "store1", map[string]interface{}{"id": "a-key", "data": "somedata"})
	store.Save("b-key", "store1", map[string]interface{}{"id": "b-key", "data": "somedata"})

	// Read the data back. The first read for each partition will trigger a migration.
	var dst1, dst2 any
	store.Get("a-key", "store1", &dst1)
	store.Get("b-key", "store1", &dst2)

	fmt.Println(dst1)
	fmt.Println(dst2)

	// Wait for the migrations to complete.
	time.Sleep(100 * time.Millisecond)
}
```

### Migrating to a PoolStore

This example demonstrates how to migrate from a single store to a `PoolStore`, where each partition is a separate store in the pool.

```go
package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gostore/gostore/common"
	"github.com/gostore/gostore/pool"
	"github.com/gostore/gostore/stores/memory"
	"github.com/gostore/gostore/stores/progressive"
)

func main() {
	primary := memory.NewMemoryStore()
	poolStore := pool.NewObjectPool(10) // Pool of 10 stores
	metadata := memory.NewMemoryStore()

	// The secondary store is a wrapper around the pool that creates new stores on demand.
	secondary := &PoolStoreAdapter{pool: poolStore}

	store := progressive.New(
		primary,
		secondary,
		metadata,
		func(key string) string {
			// Partition by customer ID (e.g., "customer-123:profile")
			parts := strings.Split(key, ":")
			if len(parts) > 0 {
				return parts[0]
			}
			return "default"
		},
		common.Logger("test"),
	)

	go store.StartBackfill(context.Background())

	store.Save("customer-123:profile", "users", map[string]interface{}{"id": "customer-123:profile", "data": "somedata"})
	store.Save("customer-456:profile", "users", map[string]interface{}{"id": "customer-456:profile", "data": "somedata"})

	var dst1, dst2 any
	store.Get("customer-123:profile", "users", &dst1)
	store.Get("customer-456:profile", "users", &dst2)

	fmt.Println(dst1)
	fmt.Println(dst2)

	time.Sleep(100 * time.Millisecond)
}

// PoolStoreAdapter is a simple adapter to make the PoolStore compatible with the ObjectStore interface.
type PoolStoreAdapter struct {
	pool *pool.ObjectPool
}

func (a *PoolStoreAdapter) getStore(partition string) (common.ObjectStore, error) {
	return a.pool.GetOrCreate(partition, func() (common.ObjectStore, error) {
		return memory.NewMemoryStore(), nil
	})
}

// Implement the common.ObjectStore interface by delegating to the correct store in the pool.
// (Implementation of all ObjectStore methods is omitted for brevity)
```

### Migrating to a ShardedStore

This example shows how to migrate to a `ShardedStore`, which automatically distributes data across a set of shards.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gostore/gostore/common"
	"github.com/gostore/gostore/stores/memory"
	"github.com/gostore/gostore/stores/progressive"
	"github.com/gostore/gostore/stores/sharded"
)

func main() {
	primary := memory.NewMemoryStore()
	metadata := memory.NewMemoryStore()

	// Create a sharded store with 10 shards.
	shardedStore := sharded.NewGenericShardedStore(10, "memory", nil, nil, memory.NewMemoryStore())

	store := progressive.New(
		primary,
		shardedStore,
		metadata,
		func(key string) string {
			// The partitioner for the progressive store should align with the
			// sharding logic of the sharded store.
			shardName, _ := sharded.DefaultShardLocator(10)("", key)
			return shardName
		},
		common.Logger("test"),
	)

	go store.StartBackfill(context.Background())

	store.Save("a-key", "store1", map[string]interface{}{"id": "a-key", "data": "somedata"})
	store.Save("b-key", "store1", map[string]interface{}{"id": "b-key", "data": "somedata"})

	var dst1, dst2 any
	store.Get("a-key", "store1", &dst1)
	store.Get("b-key", "store1", &dst2)

	fmt.Println(dst1)
	fmt.Println(dst2)

	time.Sleep(100 * time.Millisecond)
}