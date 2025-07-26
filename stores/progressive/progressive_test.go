package progressive

import (
	"testing"
	"time"

	"github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/mocks"
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
	}, logger, 10)

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

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10)

	// Expect a call to the metadata store to get the partition status.
	// Return NotMigrated.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).Return(common.ErrNotFound)

	// Expect a call to the primary store to get the data.
	primary.EXPECT().Get("key1", "store1", gomock.Any()).Return(nil)

	// Expect a call to the metadata store to update the status to InProgress.
	metadata.EXPECT().Save("partition1", "migration_status", gomock.Any()).Return("partition1", nil)

	// Expect a call to the primary store to get all the data for the partition.
	// Expect a call to the primary store to get all the data for the partition.
	rows := mocks.NewMockObjectRows(ctrl)
	primary.EXPECT().AllCursor("partition1").Return(rows, nil)

	// This part of the test is tricky because the migration runs in a separate
	// goroutine. For this test, we'll assume the migration completes instantly.
	// In a real-world scenario, you would use channels or other synchronization
	// primitives to coordinate the test.
	rows.EXPECT().Next(gomock.Any()).SetArg(0, map[string]interface{}{"id": "obj1", "data": "somedata"}).Return(true, nil)
	rows.EXPECT().Next(gomock.Any()).Return(false, nil)
	rows.EXPECT().Close()

	secondary.EXPECT().BatchInsert(gomock.Any(), "partition1", nil).Return([]string{"obj1"}, nil)

	// Expect a call to the metadata store to update the status to Migrated.
	metadata.EXPECT().Save("partition1", "migration_status", gomock.Any()).Return("partition1", nil)

	var dst any
	err := store.Get("key1", "store1", &dst)
	assert.NoError(t, err)

	// Wait for the migration to complete.
	// In a real-world scenario, you would use a more robust synchronization mechanism.
	time.Sleep(100 * time.Millisecond)
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
	}, logger, 10)

	// Expect a call to the primary store's Save method.
	primary.EXPECT().Save("key1", "store1", "data").Return("key1", nil)

	// Expect a call to the secondary store's Save method.
	secondary.EXPECT().Save("key1", "store1", "data").Return("key1", nil)

	_, err := store.Save("key1", "store1", "data")
	assert.NoError(t, err)
}

func TestMigratePartition_Resumption(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10)

	// Expect a call to the metadata store to get the partition status.
	// Return InProgress with a LastMigratedKey.
	metadata.EXPECT().Get("partition1", "migration_status", gomock.Any()).SetArg(2, PartitionMetadata{
		ID:              "partition1",
		Status:          MigrationInProgress,
		LastMigratedKey: "obj5",
	}).Return(nil)

	// Expect a call to the primary store's Since method to resume the migration.
	rows := mocks.NewMockObjectRows(ctrl)
	primary.EXPECT().Since("obj5", 10, 0, "partition1").Return(rows, nil)
	rows.EXPECT().Close()
	rows.EXPECT().Next(gomock.Any()).Return(false, nil)

	// Expect a call to the metadata store to update the status to Migrated.
	metadata.EXPECT().Save("partition1", "migration_status", gomock.Any()).Return("partition1", nil)

	store.migratePartition("partition1")
}
