package progressive

import (
	"reflect"
	"testing"

	"github.com/osiloke/gostore/common"
	mocks "github.com/osiloke/gostore/mocks"
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
	metadataData := make(map[string]interface{})

	primary := mocks.NewMockObjectStore(ctrl)
	secondary := mocks.NewMockObjectStore(ctrl)
	metadata := mocks.NewMockObjectStore(ctrl)
	logger := common.Logger("test")

	store := New(primary, secondary, metadata, func(key string) string {
		return "partition1"
	}, logger, 10, 5)

	// --- Mock Setup ---

	// Metadata Get
	metadata.EXPECT().Get(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(partitionID, key string, dst *PartitionMetadata) error {
		if val, ok := metadataData[partitionID]; ok {
			*dst = val.(PartitionMetadata)
			return nil
		}
		return common.ErrNotFound
	}).AnyTimes()

	// Metadata Save
	metadata.EXPECT().Save(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(partitionID, key string, data *PartitionMetadata) (string, error) {
		metadataData[partitionID] = *data
		return partitionID, nil
	}).AnyTimes()

	// Primary Get
	primary.EXPECT().Get("key1", "partition1", gomock.Any()).DoAndReturn(func(key, store string, dst any) error {
		if val, ok := primaryData[key]; ok {
			reflect.ValueOf(dst).Elem().Set(reflect.ValueOf(val))
			return nil
		}
		return common.ErrNotFound
	})

	// Primary AllCursor
	rows := mocks.NewMockObjectRows(ctrl)
	primary.EXPECT().AllCursor("partition1").Return(rows, nil)
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
	secondary.EXPECT().BatchInsert(gomock.Any(), "partition1", nil).DoAndReturn(func(data []interface{}, store string, opts common.ObjectStoreOptions) ([]string, error) {
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
	err := store.Get("key1", "partition1", &dst)
	assert.NoError(t, err)

	// --- Verification ---
	// Verify that the data was migrated to the secondary store.
	assert.Equal(t, primaryData["key1"], secondaryData["key1"])

	assert.Equal(t, primaryData, secondaryData, "secondary data should match primary data after migration")
	// Verify that the partition is now marked as migrated.
	status, err := store.getPartitionStatus("partition1")
	assert.NoError(t, err)
	assert.Equal(t, Migrated, status)
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

func TestMigratePartition_Resumption(t *testing.T) {
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

	store.migratePartition("partition1", nil)
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
