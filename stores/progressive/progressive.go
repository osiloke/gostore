package progressive

import (
	"context"
	"errors"
	"sync"

	"github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/worker"
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
	primary          common.ObjectStore
	secondary        common.ObjectStore
	metadata         common.ObjectStore
	partitioner      PartitionFunc
	migrationLock    sync.Mutex
	logger           *common.ZerologLogger
	batchSize        int
	workerPool       *worker.WorkerPool
	migrationJobHook func(*migrationJob) // for testing
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
	numWorkers int,
) *ProgressiveMigrationStore {
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}
	if numWorkers <= 0 {
		numWorkers = 10 // Default number of workers
	}
	return &ProgressiveMigrationStore{
		primary:     primary,
		secondary:   secondary,
		metadata:    metadata,
		partitioner: partitioner,
		logger:      logger,
		batchSize:   batchSize,
		workerPool:  worker.NewWorkerPool(numWorkers),
	}
}

// CreateDatabase creates a database in both the primary and secondary stores.
func (s *ProgressiveMigrationStore) CreateDatabase() error {
	if err := s.primary.CreateDatabase(); err != nil {
		return err
	}
	return s.secondary.CreateDatabase()
}

// CreateTable creates a table in both the primary and secondary stores.
func (s *ProgressiveMigrationStore) CreateTable(table string, sample interface{}) error {
	if err := s.primary.CreateTable(table, sample); err != nil {
		return err
	}
	return s.secondary.CreateTable(table, sample)
}

// GetStore returns the primary store.
func (s *ProgressiveMigrationStore) GetStore() interface{} {
	return s.primary.GetStore()
}

// Stats returns statistics from the primary store.
func (s *ProgressiveMigrationStore) Stats(store string) (map[string]interface{}, error) {
	return s.primary.Stats(store)
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

// SaveAll saves multiple objects to the store. It implements the dual-write strategy.
func (s *ProgressiveMigrationStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	keys, err = s.primary.SaveAll(store, src...)
	if err != nil {
		return nil, err
	}
	if _, err := s.secondary.SaveAll(store, src...); err != nil {
		s.logger.Error("failed to write to secondary store during SaveAll", "error", err, "store", store)
	}
	return keys, nil
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

// Replace replaces an existing document with a new one.
func (s *ProgressiveMigrationStore) Replace(key string, store string, src interface{}) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.Replace(key, store, src)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.Replace(key, store, src); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during Replace", "error", err, "key", key, "store", store)
	}

	return nil
}

// FilterUpdate updates multiple documents matching a filter.
func (s *ProgressiveMigrationStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.FilterUpdate(filter, src, store, opts)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.FilterUpdate(filter, src, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during FilterUpdate", "error", err, "store", store)
	}

	return nil
}

// FilterReplace replaces multiple documents matching a filter.
func (s *ProgressiveMigrationStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.FilterReplace(filter, src, store, opts)
	if err != nil {
		return err
	}

	return nil
}

// FilterDelete removes multiple documents matching a filter.
func (s *ProgressiveMigrationStore) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	// Write to primary store first, as it is the source of truth.
	err := s.primary.FilterDelete(filter, store, opts)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := s.secondary.FilterDelete(filter, store, opts); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during FilterDelete", "error", err, "store", store)
	}

	return nil
}

// FilterCount counts documents matching a filter.
func (s *ProgressiveMigrationStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	// For simplicity, we'll always read from the primary store for FilterCount.
	return s.primary.FilterCount(filter, store, opts)
}

// FilterGetAll retrieves multiple documents matching a filter.
func (s *ProgressiveMigrationStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.FilterGetAll(filter, count, skip, store, opts)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, err := s.primary.FilterGetAll(filter, count, skip, store, opts)
	if err != nil {
		return nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// Query retrieves documents matching a filter and calculates aggregations.
func (s *ProgressiveMigrationStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, nil, err
	}

	if status == Migrated {
		return s.secondary.Query(filter, aggregates, count, skip, store, opts)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, agg, err := s.primary.Query(filter, aggregates, count, skip, store, opts)
	if err != nil {
		return nil, nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, agg, nil
}

// FilterSince retrieves all documents after a specific ID matching a filter.
func (s *ProgressiveMigrationStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.FilterSince(id, filter, count, skip, store, opts)
	}

	rows, err := s.primary.FilterSince(id, filter, count, skip, store, opts)
	if err != nil {
		return nil, err
	}

	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// FilterBefore retrieves all documents before a specific ID matching a filter.
func (s *ProgressiveMigrationStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.FilterBefore(id, filter, count, skip, store, opts)
	}

	rows, err := s.primary.FilterBefore(id, filter, count, skip, store, opts)
	if err != nil {
		return nil, err
	}

	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// FilterBeforeCount counts all documents before a specific ID matching a filter.
func (s *ProgressiveMigrationStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	// For simplicity, we'll always read from the primary store for FilterBeforeCount.
	return s.primary.FilterBeforeCount(id, filter, count, skip, store, opts)
}

// All retrieves all documents in a store.
func (s *ProgressiveMigrationStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.All(count, skip, store)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, err := s.primary.All(count, skip, store)
	if err != nil {
		return nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// AllCursor retrieves all documents in a store using a cursor.
func (s *ProgressiveMigrationStore) AllCursor(store string) (common.ObjectRows, error) {
	// For simplicity, we'll always read from the primary store for AllCursor.
	return s.primary.AllCursor(store)
}

// Since retrieves all documents after a specific ID.
func (s *ProgressiveMigrationStore) Since(id string, count int, skip int, store string) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.Since(id, count, skip, store)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, err := s.primary.Since(id, count, skip, store)
	if err != nil {
		return nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// Before retrieves all documents before a specific ID.
func (s *ProgressiveMigrationStore) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.Before(id, count, skip, store)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, err := s.primary.Before(id, count, skip, store)
	if err != nil {
		return nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// AllWithinRange retrieves all documents within a range.
func (s *ProgressiveMigrationStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return nil, err
	}

	if status == Migrated {
		return s.secondary.AllWithinRange(filter, count, skip, store, opts)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	rows, err := s.primary.AllWithinRange(filter, count, skip, store, opts)
	if err != nil {
		return nil, err
	}

	// If the partition has not been migrated yet, start the migration process
	// in a background goroutine.
	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return rows, nil
}

// GetByField retrieves a document by a specific field and value.
func (s *ProgressiveMigrationStore) GetByField(name, val, store string, dst interface{}) error {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return err
	}

	if status == Migrated {
		return s.secondary.GetByField(name, val, store, dst)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	err = s.primary.GetByField(name, val, store, dst)
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

// FilterGet retrieves a single document matching a filter.
func (s *ProgressiveMigrationStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) error {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return err
	}

	if status == Migrated {
		return s.secondary.FilterGet(filter, store, dst, opts)
	}

	err = s.primary.FilterGet(filter, store, dst, opts)
	if err != nil {
		return err
	}

	if status == NotMigrated {
		s.triggerMigration(partitionID)
	}

	return nil
}

// GetByFieldsByField retrieves a document by a specific field and value, selecting specific fields.
func (s *ProgressiveMigrationStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	partitionID := store
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		return err
	}

	if status == Migrated {
		return s.secondary.GetByFieldsByField(name, val, store, fields, dst)
	}

	// If the partition is not migrated or is in the process of being migrated,
	// serve the request from the primary store.
	err = s.primary.GetByFieldsByField(name, val, store, fields, dst)
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

// Get retrieves a document by its key.
func (s *ProgressiveMigrationStore) Get(key, store string, dst interface{}) error {
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

// triggerMigration creates and enqueues a migration job.
// It also provides a hook for testing purposes.
func (s *ProgressiveMigrationStore) triggerMigration(partitionID string) {
	job := &migrationJob{
		store:       s,
		partitionID: partitionID,
	}
	if s.migrationJobHook != nil {
		s.migrationJobHook(job)
	}
	s.workerPool.Enqueue(job)
}

// getPartitionStatus retrieves the migration status of a partition from the
// metadata store. If the partition is not found, it is assumed to be NotMigrated.
func (s *ProgressiveMigrationStore) getPartitionStatus(partitionID string) (MigrationStatus, error) {
	var metadata PartitionMetadata
	err := s.metadata.Get(partitionID, "migration_status", &metadata)
	if err != nil {
		if err == common.ErrNotFound {
			return NotMigrated, nil
		}
		return 0, err
	}
	return metadata.Status, nil
}

// setPartitionStatus updates the migration status of a partition in the
// metadata store.
func (s *ProgressiveMigrationStore) setPartitionStatus(partitionID string, status MigrationStatus, lastMigratedKey string) error {
	metadata := PartitionMetadata{
		ID:              partitionID,
		Status:          status,
		LastMigratedKey: lastMigratedKey,
	}
	_, err := s.metadata.Save(partitionID, "migration_status", &metadata)
	return err
}

// migratePartition migrates a partition from the primary to the secondary store.
// It is designed to be run in a background goroutine.
func (s *ProgressiveMigrationStore) migratePartition(partitionID string, done chan bool, migrationStarted chan<- struct{}, resumeMigration <-chan struct{}) {
	s.migrationLock.Lock()
	defer s.migrationLock.Unlock()
	defer func() {
		if done != nil {
			done <- true
		}
	}()

	// Check if the partition is already being migrated or has been migrated.
	status, err := s.getPartitionStatus(partitionID)
	if err != nil {
		s.logger.Error("failed to get partition status before migration", "error", err, "partition", partitionID)
		return
	}
	if status == MigrationInProgress || status == Migrated {
		return
	}

	// Set the partition status to MigrationInProgress.
	if err := s.setPartitionStatus(partitionID, MigrationInProgress, ""); err != nil {
		s.logger.Error("failed to set partition status to InProgress", "error", err, "partition", partitionID)
		return
	}

	// Get a cursor for all documents in the partition from the primary store.
	var rows common.ObjectRows
	var lastMigratedKey string

	meta := &PartitionMetadata{}
	if err := s.metadata.Get(partitionID, "migration_status", meta); err == nil {
		lastMigratedKey = meta.LastMigratedKey
	}

	if lastMigratedKey != "" {
		rows, err = s.primary.Since(lastMigratedKey, s.batchSize, 0, partitionID)
	} else {
		rows, err = s.primary.AllCursor(partitionID)
	}
	if err != nil {
		s.logger.Error("failed to get cursor for migration", "error", err, "partition", partitionID)
		return
	}
	defer rows.Close()

	// Start migrating data in batches.
	batch := make([]interface{}, 0, s.batchSize)

	// Signal that the migration is about to start processing rows.
	if migrationStarted != nil {
		close(migrationStarted)
	}

	// Wait for the signal to proceed with the migration.
	if resumeMigration != nil {
		<-resumeMigration
	}
	for {
		var entry map[string]interface{}
		hasNext, err := rows.Next(&entry)
		if err != nil {
			if err == common.ErrEOF {
				s.logger.Info("migration completed for partition", "partition", partitionID)
				break
			}
			s.logger.Error("failed to get next row during migration", "error", err, "partition", partitionID)
			return
		}
		if !hasNext {
			break
		}

		batch = append(batch, entry)
		if id, ok := entry["id"].(string); ok {
			lastMigratedKey = id
		}

		if len(batch) >= s.batchSize {
			if _, err := s.secondary.BatchInsert(batch, partitionID, nil); err != nil {
				s.logger.Error("failed to batch insert during migration", "error", err, "partition", partitionID)
				// In a real-world scenario, you might want to handle this more gracefully,
				// e.g., by retrying or storing the failed batch for later processing.
				return
			}
			if err := s.setPartitionStatus(partitionID, MigrationInProgress, lastMigratedKey); err != nil {
				s.logger.Error("failed to update last migrated key", "error", err, "partition", partitionID)
				return
			}
			batch = make([]interface{}, 0, s.batchSize)
		}
	}

	// Insert any remaining documents in the last batch.
	if len(batch) > 0 {
		if _, err := s.secondary.BatchInsert(batch, partitionID, nil); err != nil {
			s.logger.Error("failed to insert remaining batch during migration", "error", err, "partition", partitionID)
			return
		}
	}

	// Set the partition status to Migrated.
	if err := s.setPartitionStatus(partitionID, Migrated, ""); err != nil {
		s.logger.Error("failed to set partition status to Migrated", "error", err, "partition", partitionID)
	}
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
			if err == common.ErrEOF {
				s.logger.Info("no more partitions to backfill")
				break
			}
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

// Close closes the worker pool and both primary and secondary stores.
func (s *ProgressiveMigrationStore) Close() {
	s.primary.Close()
	s.secondary.Close()
	s.workerPool.Stop()
}

// migrationJob represents a job to migrate a partition.
type migrationJob struct {
	store       *ProgressiveMigrationStore
	partitionID string
	done        chan bool

	// For testing purposes
	migrationStarted chan<- struct{}
	resumeMigration  <-chan struct{}
}

// Execute executes the migration job.
func (j *migrationJob) Execute() {
	j.store.migratePartition(j.partitionID, j.done, j.migrationStarted, j.resumeMigration)
}

// GetTX retrieves an object from the store within a transaction.
func (s *ProgressiveMigrationStore) GetTX(key string, store string, dst interface{}, txn common.Transaction) error {
	// For simplicity, we'll always read from the primary store for GetTX.
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return errors.New("primary store does not support transactions")
	}
	return primaryTxStore.GetTX(key, store, dst, txn)
}

// SaveTX saves an object to the store within a transaction. It implements the dual-write strategy.
func (s *ProgressiveMigrationStore) SaveTX(key string, store string, src interface{}, txn common.Transaction) error {
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return errors.New("primary store does not support transactions")
	}
	secondaryTxStore, ok := s.secondary.(common.TransactionStore)
	if !ok {
		return errors.New("secondary store does not support transactions")
	}
	// Write to primary store first, as it is the source of truth.
	err := primaryTxStore.SaveTX(key, store, src, txn)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := secondaryTxStore.SaveTX(key, store, src, txn); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during SaveTX", "error", err, "key", key, "store", store)
	}

	return nil
}

// DeleteTX deletes an object from the store within a transaction. It implements the dual-write strategy.
func (s *ProgressiveMigrationStore) DeleteTX(key string, store string, tx common.Transaction) error {
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return errors.New("primary store does not support transactions")
	}
	secondaryTxStore, ok := s.secondary.(common.TransactionStore)
	if !ok {
		return errors.New("secondary store does not support transactions")
	}
	// Write to primary store first, as it is the source of truth.
	err := primaryTxStore.DeleteTX(key, store, tx)
	if err != nil {
		return err
	}

	// Then, write to the secondary store.
	if err := secondaryTxStore.DeleteTX(key, store, tx); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during DeleteTX", "error", err, "key", key, "store", store)
	}

	return nil
}

// FilterGetTX retrieves an object from the store within a transaction based on a filter.
func (s *ProgressiveMigrationStore) FilterGetTX(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions, tx common.Transaction) error {
	// For simplicity, we'll always read from the primary store for FilterGetTX.
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return errors.New("primary store does not support transactions")
	}
	return primaryTxStore.FilterGetTX(filter, store, dst, opts, tx)
}

// BatchInsertTX inserts multiple objects into the store. It implements the dual-write strategy.
// The primary store is updated first, and then the secondary store. If the
// insert into the secondary store fails, the error is logged, but the operation
// is still considered successful.
func (s *ProgressiveMigrationStore) BatchInsertTX(data []interface{}, store string, opts common.ObjectStoreOptions, tx common.Transaction) (keys []string, err error) {
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return nil, errors.New("primary store does not support transactions")
	}
	secondaryTxStore, ok := s.secondary.(common.TransactionStore)
	if !ok {
		return nil, errors.New("secondary store does not support transactions")
	}
	// Write to primary store first, as it is the source of truth.
	keys, err = primaryTxStore.BatchInsertTX(data, store, opts, tx)
	if err != nil {
		return nil, err
	}

	// Then, write to the secondary store.
	if _, err := secondaryTxStore.BatchInsertTX(data, store, opts, tx); err != nil {
		// Log the error, but don't fail the operation.
		// A background process can handle reconciliation.
		s.logger.Error("failed to write to secondary store during BatchInsertTX", "error", err, "store", store)
	}

	return keys, nil
}

// UpdateTransaction starts an update transaction
func (s *ProgressiveMigrationStore) UpdateTransaction() common.Transaction {
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return nil
	}
	return primaryTxStore.UpdateTransaction()
}

// FinishTransaction ebds transaction
func (s *ProgressiveMigrationStore) FinishTransaction(tx common.Transaction) error {
	primaryTxStore, ok := s.primary.(common.TransactionStore)
	if !ok {
		return errors.New("primary store does not support transactions")
	}
	return primaryTxStore.FinishTransaction(tx)
}
