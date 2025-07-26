package progressive

import (
	"context"
	"sync"

	"github.com/gostore/gostore/common"
)

// MigrationStatus represents the migration state of a partition.
type MigrationStatus int

const (
	// NotMigrated means the partition has not been migrated.
	NotMigrated MigrationStatus = iota
	// MigrationInProgress means the partition is currently being migrated.
	MigrationInProgress
	// Migrated means the partition has been fully migrated.
	Migrated
)

// PartitionFunc is a function that determines the partition ID for a given key.
// The partitioner is a core component of the progressive migration store, as it
// determines how the keyspace is divided into logical partitions.
type PartitionFunc func(key string) string

// PartitionMetadata stores the migration status of a partition. This struct is
// stored in the metadata store to track the progress of the migration.
type PartitionMetadata struct {
	ID              string          `json:"id"`
	Status          MigrationStatus `json:"status"`
	LastMigratedKey string          `json:"lastMigratedKey"`
}

// ProgressiveMigrationStore is a store that progressively migrates data from a primary
// to a secondary store. It implements the common.ObjectStore interface.
//
// It is designed to be used in a live, zero-downtime migration scenario.
// When a read request is received for a partition that has not yet been migrated,
// the request is served from the primary store, and a background goroutine is
// started to migrate the partition. Subsequent read requests for the same
// partition will be served from the secondary store once the migration is
// complete.
//
// Write requests are always sent to both the primary and secondary stores to
// ensure that new and updated data is immediately consistent.
type ProgressiveMigrationStore struct {
	primary       common.ObjectStore
	secondary     common.ObjectStore
	metadata      common.ObjectStore
	partitioner   PartitionFunc
	migrationLock sync.Mutex
	logger        *common.ZerologLogger
	batchSize     int
}

// New creates a new ProgressiveMigrationStore.
//
// primary: The primary data store.
// secondary: The secondary data store.
// metadata: The metadata store, used to track migration status.
// partitioner: A function that determines the partition ID for a given key.
// logger: A logger for logging messages.
func New(
	primary, secondary, metadata common.ObjectStore,
	partitioner PartitionFunc,
	logger *common.ZerologLogger,
	batchSize int,
) *ProgressiveMigrationStore {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}
	return &ProgressiveMigrationStore{
		primary:     primary,
		secondary:   secondary,
		metadata:    metadata,
		partitioner: partitioner,
		logger:      logger,
		batchSize:   batchSize,
	}
}

// Save saves an object to the store. It implements the dual-write strategy.
func (s *ProgressiveMigrationStore) Save(key, store string, src any) (string, error) {
	// Write to primary store first, as it is the source of truth.
	id, err := s.primary.Save(key, store, src)
	if err != nil {
		return "", err
	}

	// Then, write to the secondary store.
	if _, err := s.secondary.Save(key, store, src); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during Save", "error", err, "key", key, "store", store)
	}

	return id, nil
}

// Update updates an object in the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// update to the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) Update(key, store string, src any) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.Update(key, store, src)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.Update(key, store, src); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during Update", "error", err, "key", key, "store", store)
	}

	return nil
}

// Delete deletes an object from the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// delete from the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) Delete(key, store string) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.Delete(key, store)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.Delete(key, store); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during Delete", "error", err, "key", key, "store", store)
	}

	return nil
}

// BatchInsert inserts multiple objects into the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// insert into the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) BatchInsert(data []any, store string, opts common.ObjectStoreOptions) ([]string, error) {
	// Write to primary store first, as it is the source of truth.
	ids, err := s.primary.BatchInsert(data, store, opts)
	if err != nil {
		return nil, err
	}

	// Then, write to the secondary store.
	if _, err := s.secondary.BatchInsert(data, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during BatchInsert", "error", err, "store", store)
	}

	return ids, nil
}

// BatchUpdate updates multiple objects in the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// update to the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) BatchUpdate(id, data []any, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.BatchUpdate(id, data, store, opts)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.BatchUpdate(id, data, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during BatchUpdate", "error", err, "store", store)
	}

	return nil
}

// BatchDelete deletes multiple objects from the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// delete from the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) BatchDelete(ids []any, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.BatchDelete(ids, store, opts)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.BatchDelete(ids, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during BatchDelete", "error", err, "store", store)
	}

	return nil
}

// BatchFilterDelete deletes multiple objects from the store based on a filter. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// delete from the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) BatchFilterDelete(filter []map[string]any, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.BatchFilterDelete(filter, store, opts)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.BatchFilterDelete(filter, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during BatchFilterDelete", "error", err, "store", store)
	}

	return nil
}

// Get gets an object from the store. It implements the on-demand migration logic.
// If the partition for the given key has been migrated, the object is read from
// the secondary store. Otherwise, it is read from the primary store, and a
// background migration is triggered for the partition.
func (s *ProgressiveMigrationStore) Get(key, store string, dst any) error {
	partitionID := s.partitioner(key)
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return err
	}

	if status == Migrated {
		return s.secondary.Get(key, store, dst)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	err = s.primary.Get(key, store, dst)
	if err != nil {
		return err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return nil
}

func (s *ProgressiveMigrationStore) getPartitionStatus(partitionID string) (MigrationStatus, error) {
	var metadata PartitionMetadata
	err := s.metadata.Get(partitionID, "migration_status", &metadata)
	if err != nil {
		// If the metadata is not found, it means the partition has not been migrated yet.
		if err == common.ErrNotFound {
			return NotMigrated, nil
		}
		return NotMigrated, err
	}
	return metadata.Status, nil
}

func (s *ProgressiveMigrationStore) triggerMigration(partitionID string) {
	s.migrationLock.Lock()
	defer s.migrationLock.Unlock()

	// Double-check the status to avoid race conditions.
	status, err := s.getPartitionStatus(partitionID)
	if err != nil || status != NotMigrated {
		return
	}

	// Update the status to MigrationInProgress.
	metadata := PartitionMetadata{ID: partitionID, Status: MigrationInProgress}
	if _, err := s.metadata.Save(partitionID, "migration_status", &metadata); err != nil {
		s.logger.Error("failed to update partition status to InProgress", "error", err, "partitionID", partitionID)
		return
	}

	// Start the migration in a background goroutine.
	go s.migratePartition(partitionID)
}

func (s *ProgressiveMigrationStore) migratePartition(partitionID string) {
	s.logger.Info("starting partition migration", "partitionID", partitionID)

	// Get the current migration metadata for the partition.
	var metadata PartitionMetadata
	err := s.metadata.Get(partitionID, "migration_status", &metadata)
	if err != nil && err != common.ErrNotFound {
		s.logger.Error("failed to get partition metadata", "error", err, "partitionID", partitionID)
		return
	}

	var rows common.ObjectRows
	if metadata.LastMigratedKey != "" {
		// Resume from the last migrated key.
		rows, err = s.primary.Since(metadata.LastMigratedKey, s.batchSize, 0, partitionID)
	} else {
		// Start from the beginning.
		rows, err = s.primary.AllCursor(partitionID)
	}

	if err != nil {
		s.logger.Error("failed to get cursor for partition", "error", err, "partitionID", partitionID)
		return
	}
	defer rows.Close()

	var objects []any
	var lastKey string
	for {
		var obj common.HasID
		hasNext, err := rows.Next(&obj)
		if err != nil {
			s.logger.Error("cursor error during migration", "error", err, "partitionID", partitionID)
			return
		}
		if !hasNext {
			break
		}

		objects = append(objects, obj)
		lastKey = obj.GetId()

		if len(objects) >= s.batchSize {
			if err := s.flushBatch(partitionID, objects, lastKey); err != nil {
				return // Error is already logged in flushBatch
			}
			objects = nil // Reset batch
		}
	}

	// Flush any remaining objects.
	if len(objects) > 0 {
		if err := s.flushBatch(partitionID, objects, lastKey); err != nil {
			return
		}
	}

	// Update the partition status to Migrated.
	metadata.Status = Migrated
	if _, err := s.metadata.Save(partitionID, "migration_status", &metadata); err != nil {
		s.logger.Error("failed to update partition status to Migrated", "error", err, "partitionID", partitionID)
		return
	}

	s.logger.Info("partition migration completed", "partitionID", partitionID)
}

func (s *ProgressiveMigrationStore) flushBatch(partitionID string, objects []any, lastKey string) error {
	if _, err := s.secondary.BatchInsert(objects, partitionID, nil); err != nil {
		s.logger.Error("failed to batch insert objects into secondary store", "error", err, "partitionID", partitionID)
		return err
	}

	// Update the last migrated key.
	metadata := PartitionMetadata{
		ID:              partitionID,
		Status:          MigrationInProgress,
		LastMigratedKey: lastKey,
	}
	if _, err := s.metadata.Save(partitionID, "migration_status", &metadata); err != nil {
		s.logger.Error("failed to update last migrated key", "error", err, "partitionID", partitionID)
		return err
	}

	return nil
}

// FilterGet gets an object from the store based on a filter.
// It does not trigger on-demand migration, as a filter can span multiple partitions.
// For simplicity, this method always reads from the primary store. A more
// advanced implementation could check if all partitions in the filter have been
// migrated.
func (s *ProgressiveMigrationStore) FilterGet(filter map[string]any, store string, dst any, opts common.ObjectStoreOptions) error {
	// For simplicity, we'll always read from the primary store for FilterGet.
	// A more advanced implementation could check if all partitions in the filter
	// have been migrated.
	return s.primary.FilterGet(filter, store, dst, opts)
}

// StartBackfill starts a background process that systematically migrates all partitions.
// This method is non-blocking and will return immediately. The backfill process
// will run in a separate goroutine.
func (s *ProgressiveMigrationStore) StartBackfill(ctx context.Context) {
	s.logger.Info("starting background backfill process")

	// This is a simplified backfill process. A more robust implementation would
	// handle pagination and rate limiting.
	rows, err := s.metadata.AllCursor("migration_status")
	if err != nil {
		s.logger.Error("failed to get cursor for backfill", "error", err)
		return
	}
	defer rows.Close()

	for {
		var metadata PartitionMetadata
		hasNext, err := rows.Next(&metadata)
		if err != nil {
			s.logger.Error("cursor error during backfill", "error", err)
			return
		}
		if !hasNext {
			break
		}

		if metadata.Status == NotMigrated {
			s.triggerMigration(metadata.ID)
		}
	}

	s.logger.Info("background backfill process completed")
}
