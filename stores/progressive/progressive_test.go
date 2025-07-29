package progressive

import (
	"reflect"
	"testing"
	"time"

	"github.com/osiloke/gostore/common"
	mocks "github.com/osiloke/gostore/mocks"
	"github.com/osiloke/gostore/stores/memory"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestNew(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition"
	}, logger, 10, 5)

	assert.NotNil(t, store)
	assert.Equal(t, primary, store.primary)
	assert.Equal(t, secondary, store.secondary)
	assert.Equal(t, metadata, store.metadata)
	assert.NotNil(t, store.partitioner)
	assert.Equal(t, logger, store.logger)
}

func TestGet_OnDemandMigration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primaryData := map[string]interface{}{"key1": map[string]interface{}{"id": "key1", "data": "somedata"}}
	secondaryData := make(map[string]interface{})

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := memory.NewMemoryStore() // Use a real metadata store for polling
	logger := common.Logger("test")
	partitionID := "partition1"

	store := New(primary, secondary, metadata, func(key string) string {
		return partitionID
	}, logger, 10, 5)
	defer store.Close()

	done := make(chan bool, 1)
	store.migrationJobHook = func(job *migrationJob) {
		job.done = done
	}

	// --- Mock Setup ---
	primary.EXPECT().Close()
	secondary.EXPECT().Close()

	// Primary Get
	primary.EXPECT().Get("key1", partitionID, gomock.Any()).DoAndReturn(func(key, store string, dst any) error {
		if val, ok := primaryData[key]; ok {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(val))
			return nil
		}
		return common.ErrNotFound
	})

	// Primary AllCursor
	rows := mocks.NewMockObjectRows(ctrl)
	primary.EXPECT().AllCursor(partitionID).Return(rows, nil)
	var entries []interface{}
	for _, v := range primaryData {
		entries = append(entries, v)
	}
	gomock.InOrder(
		rows.EXPECT().Next(gomock.Any()).SetArg(0, entries[0]).Return(true, nil),
		rows.EXPECT().Next(gomock.Any()).Return(false, nil),
	)
	rows.EXPECT().Close().Return()

	// Secondary BatchInsert
	secondary.EXPECT().BatchInsert(gomock.Any(), partitionID, nil).DoAndReturn(func(data []interface{}, store string, opts common.ObjectStoreOptions) ([]string, error) {
		var ids []string
		for _, item := range data {
			d := item.(map[string]interface{})
			id := d["id"].(string)
			secondaryData[id] = d
			ids = append(ids, id)
		}
		return ids, nil
	})

	// --- Test Execution ---
	var dst any
	err := store.Get("key1", partitionID, &dst)
	assert.NoError(t, err)
	assert.NotNil(t, dst)
	assert.Equal(t, "somedata", dst.(map[string]interface{})["data"])

	// --- Verification ---
	// Wait for the migration to complete
	select {
	case <-done:
		// Migration finished
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for migration to complete")
	}

	var meta PartitionMetadata
	err = metadata.Get(partitionID, "migration_status", &meta)
	assert.NoError(t, err)
	assert.Equal(t, Migrated, meta.Status, "partition should be marked as migrated")

	// Verify that the data was migrated to the secondary store.
	assert.Equal(t, primaryData, secondaryData, "secondary data should match primary data after migration")
}

func TestGet_Migrated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the metadata store to get the partition status.
	// Return Migrated.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).SetArg(2, PartitionMetadata{
		ID:     "partition1",
		Status: Migrated,
	}).Return(nil)

	// Expect a call to the secondary store to get the data.
	secondary.EXPECT().Get("key1", "store1", gomock.Any()).Return(nil)

	var dst any
	err := store.Get("key1", "store1", &dst)
	assert.NoError(t, err)
}

func TestSave_DualWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the primary store's Save method.
	primary.EXPECT().Save("key1", "store1", "data").Return("key1", nil)

	// Expect a call to the secondary store's Save method.
	secondary.EXPECT().Save("key1", "store1", "data").Return("key1", nil)

	_, err := store.Save("key1", "store1", "data")
	assert.NoError(t, err)
}

func TestReplace_DualWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the primary store's Replace method.
	primary.EXPECT().Replace("key1", "store1", "data").Return(nil)

	// Expect a call to the secondary store's Replace method.
	secondary.EXPECT().Replace("key1", "store1", "data").Return(nil)

	err := store.Replace("key1", "store1", "data")
	assert.NoError(t, err)
}

func TestFilterUpdate_DualWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	filter := map[string]interface{}{"field": "value"}
	var opts common.ObjectStoreOptions

	// Expect a call to the primary store's FilterUpdate method.
	primary.EXPECT().FilterUpdate(filter, "data", "store1", opts).Return(nil)

	// Expect a call to the secondary store's FilterUpdate method.
	secondary.EXPECT().FilterUpdate(filter, "data", "store1", opts).Return(nil)

	err := store.FilterUpdate(filter, "data", "store1", opts)
	assert.NoError(t, err)
}

func TestFilterDelete_DualWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	filter := map[string]interface{}{"field": "value"}
	var opts common.ObjectStoreOptions

	// Expect a call to the primary store's FilterDelete method.
	primary.EXPECT().FilterDelete(filter, "store1", opts).Return(nil)

	// Expect a call to the secondary store's FilterDelete method.
	secondary.EXPECT().FilterDelete(filter, "store1", opts).Return(nil)

	err := store.FilterDelete(filter, "store1", opts)
	assert.NoError(t, err)
}

func TestBatchInsert_DualWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	data := []any{
		map[string]interface{}{"id": "id1", "data": "data1"},
		map[string]interface{}{"id": "id2", "data": "data2"},
	}
	var opts common.ObjectStoreOptions
	ids := []string{"id1", "id2"}

	// Expect a call to the primary store's BatchInsert method.
	primary.EXPECT().BatchInsert(data, "store1", opts).Return(ids, nil)

	// Expect a call to the secondary store's BatchInsert method.
	secondary.EXPECT().BatchInsert(data, "store1", opts).Return(ids, nil)

	returnedIDs, err := store.BatchInsert(data, "store1", opts)
	assert.NoError(t, err)
	assert.Equal(t, ids, returnedIDs)
}

func TestAll_Migrated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the metadata store to get the partition status.
	// Return Migrated.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).SetArg(2, PartitionMetadata{
		ID:     "partition1",
		Status: Migrated,
	}).Return(nil)

	// Expect a call to the secondary store's All method.
	secondary.EXPECT().All(10, 0, "partition1").Return(nil, nil)

	_, err := store.All(10, 0, "partition1")
	assert.NoError(t, err)
}

func TestAllWithinRange_Migrated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the metadata store to get the partition status.
	// Return Migrated.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).SetArg(2, PartitionMetadata{
		ID:     "partition1",
		Status: Migrated,
	}).Return(nil)

	// Expect a call to the secondary store's All method.
	secondary.EXPECT().AllWithinRange(nil, 10, 0, "partition1", nil).Return(nil, nil)

	_, err := store.AllWithinRange(nil, 10, 0, "partition1", nil)
	assert.NoError(t, err)
}

func TestSince_Migrated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// Expect a call to the metadata store to get the partition status.
	// Return Migrated.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).SetArg(2, PartitionMetadata{
		ID:     "partition1",
		Status: Migrated,
	}).Return(nil)

	// Expect a call to the secondary store's Since method.
	secondary.EXPECT().Since("some_id", 10, 0, "partition1").Return(nil, nil)

	_, err := store.Since("some_id", 10, 0, "partition1")
	assert.NoError(t, err)
}

func TestBatchInsert_DuringMigration_Integration(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Setup
	primary := mocks.NewMockObjectStore(ctrl)   // MOCK
	secondary := mocks.NewMockObjectStore(ctrl) // MOCK
	metadata := memory.NewMemoryStore()         // REAL
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "p1"
	}, logger, 1, 1) // batch size of 1
	defer store.Close()

	partitionID := "p1"
	migratedData1 := map[string]interface{}{"id": "m_key1", "data": "migrated 1"}
	migratedData2 := map[string]interface{}{"id": "m_key2", "data": "migrated 2"}
	batchData := []any{map[string]interface{}{"id": "b_key1", "data": "batch inserted"}}

	// --- MOCK EXPECTATIONS ---
	primary.EXPECT().Close()
	secondary.EXPECT().Close()

	// 1. The dual-write from the test's BatchInsert call
	primary.EXPECT().BatchInsert(batchData, partitionID, nil).Return([]string{"b_key1"}, nil)
	secondary.EXPECT().BatchInsert(batchData, partitionID, nil).Return([]string{"b_key1"}, nil)

	// 2. The migration process
	rows := mocks.NewMockObjectRows(ctrl)
	primary.EXPECT().AllCursor(partitionID).Return(rows, nil)
	gomock.InOrder(
		rows.EXPECT().Next(gomock.Any()).SetArg(0, migratedData1).Return(true, nil),
		rows.EXPECT().Next(gomock.Any()).SetArg(0, migratedData2).Return(true, nil),
		rows.EXPECT().Next(gomock.Any()).Return(false, nil),
	)
	rows.EXPECT().Close()

	// Migration will insert the data it gets from `rows`
	secondary.EXPECT().BatchInsert([]any{migratedData1}, partitionID, nil).Return([]string{"m_key1"}, nil)
	secondary.EXPECT().BatchInsert([]any{migratedData2}, partitionID, nil).Return([]string{"m_key2"}, nil)

	// --- TEST EXECUTION ---
	migrationStarted := make(chan struct{})
	resumeMigration := make(chan struct{})
	migrationDone := make(chan bool, 1)

	// Set metadata to NotMigrated to ensure migration is triggered
	_, err := metadata.Save(partitionID, "migration_status", &PartitionMetadata{
		ID:     partitionID,
		Status: NotMigrated,
	})
	assert.NoError(t, err)

	// Enqueue the migration job directly to control its execution
	store.workerPool.Enqueue(&migrationJob{
		store:            store,
		partitionID:      partitionID,
		done:             migrationDone,
		migrationStarted: migrationStarted,
		resumeMigration:  resumeMigration,
	})

	// Wait for migration to pause
	<-migrationStarted

	// Perform the concurrent BatchInsert
	_, err = store.BatchInsert(batchData, partitionID, nil)
	assert.NoError(t, err)

	// Resume and wait for migration to finish
	close(resumeMigration)
	<-migrationDone

	// --- VERIFICATION ---
	// Verify partition is marked as migrated
	var meta PartitionMetadata
	err = metadata.Get(partitionID, "migration_status", &meta)
	assert.NoError(t, err)
	assert.Equal(t, Migrated, meta.Status, "partition should be marked as migrated")
}
