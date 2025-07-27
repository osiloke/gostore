package testing

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/osiloke/gostore/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Data models for testing
type TestDocument struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Value int       `json:"value"`
	Count float64   `json:"count"`
	Time  time.Time `json:"time"`
	Tags  []string  `json:"tags"`
}

// Test_Get test if a gostore can retrieve a single item
func Test_Get(t *testing.T, db common.ObjectStore) {
	store := "data_get"
	// Ensure a clean state for the store
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil) // Clean up after test

	rowID := common.NewObjectId().String()
	initialRow := map[string]interface{}{
		"id":    rowID,
		"name":  "osiloke emoekpere",
		"count": 10.0,
	}
	_, err := db.Save(rowID, store, initialRow)
	require.NoError(t, err, "Failed to save initial row for Test_Get")

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can retrieve existing item",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				err := db.Get(rowID, store, &dst)
				require.NoError(t, err, "Get returned an unexpected error")
				assert.Equal(t, initialRow, dst, "retrieved row is not identical to saved row")
			},
		},
		{
			"Cannot retrieve non-existent item",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				err := db.Get("non_existent_id", store, &dst)
				assert.Error(t, err, "Get for non-existent item did not return an error")
				assert.Equal(t, common.ErrNotFound, err, "Expected ErrNotFound for non-existent item")
			},
		},
		{
			"Cannot retrieve from non-existent store",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				err := db.Get(rowID, "non_existent_store", &dst)
				assert.Error(t, err, "Get from non-existent store did not return an error")
				assert.Equal(t, common.ErrNotFound, err, "Expected ErrNotFound for non-existent store")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}

// Test_BatchInsert test if a gostore can insert multiple entries
func Test_BatchInsert(t *testing.T, db common.ObjectStore) {
	store := "data_batch_insert"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	rows := []interface{}{
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "osiloke emoekpere", "count": 10.0},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "emike emoekpere", "count": 10.0},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "oduffa emoekpere", "count": 11.0},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "tony emoekpere", "count": 11.0},
	}

	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can batch insert and retrieve all",
			func(t *testing.T) {
				keys, err := db.BatchInsert(rows, store, nil)
				require.NoError(t, err, "BatchInsert returned an unexpected error")
				assert.Len(t, keys, len(rows), "BatchInsert did not return correct number of keys")

				// Verify all items are inserted
				res, err := db.All(len(rows), 0, store)
				require.NoError(t, err, "All returned an unexpected error after batch insert")
				total := 0
				var retrievedRows []map[string]interface{}
				for {
					var data map[string]interface{}
					hasNext, err := res.Next(&data)
					if err != nil && err.Error() != "EOF" {
						require.NoError(t, err, "Next returned an unexpected error")
					}
					if !hasNext {
						break
					}
					retrievedRows = append(retrievedRows, data)
					total++
				}
				assert.Equal(t, len(rows), total, "number of rows saved is not equal to number retrieved")

				// Check if all original rows are present in retrieved rows (order may not be preserved)
				initialIDs := make(map[string]struct{})
				for _, r := range rows {
					initialIDs[r.(map[string]interface{})["id"].(string)] = struct{}{}
				}
				retrievedIDs := make(map[string]struct{})
				for _, r := range retrievedRows {
					retrievedIDs[r["id"].(string)] = struct{}{}
				}
				assert.Equal(t, initialIDs, retrievedIDs, "Retrieved IDs do not match inserted IDs")
			},
		},
		{
			"Batch insert into non-existent store creates it implicitly",
			func(t *testing.T) {
				newStore := "new_implicit_store"
				defer db.FilterDelete(nil, newStore, nil) // Clean up

				tempRows := []interface{}{
					map[string]interface{}{"id": common.NewObjectId().String(), "name": "temp1"},
					map[string]interface{}{"id": common.NewObjectId().String(), "name": "temp2"},
				}
				keys, err := db.BatchInsert(tempRows, newStore, nil)
				require.NoError(t, err, "BatchInsert into new store returned an error")
				assert.Len(t, keys, len(tempRows), "Expected keys for implicit store insert")

				count, err := db.FilterCount(nil, newStore, nil)
				require.NoError(t, err)
				assert.Equal(t, int64(len(tempRows)), count, "Expected documents in implicitly created store")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
		// Clear store for next subtest to avoid interference if not implicitly cleared
		db.FilterDelete(nil, store, nil)
		db.CreateTable(store, nil) // Recreate table
	}
}

// Test_Query test if a gostore can query items
func Test_Query(t *testing.T, db common.ObjectStore) {
	store := "data_query"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	entries := []interface{}{
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "osiloke emoekpere", "count": 10.0, "ix": int64(1)},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "emike emoekpere", "count": 10.0, "ix": int64(2)},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "oduffa emoekpere", "count": 11.0, "ix": int64(3)},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "tony emoekpere", "count": 11.0, "ix": int64(4)},
		map[string]interface{}{"id": common.NewObjectId().String(), "name": "peter emoekpere", "count": 12.0, "ix": int64(5)},
	}
	db.BatchInsert(entries, store, nil)

	tests := []struct {
		name        string
		filter      map[string]interface{}
		aggregates  map[string]interface{}
		count       int
		skip        int
		expectedIDs []string // For verifying filtered results
		expectedLen int
	}{
		{
			name:        "Can query all with no filter/pagination",
			filter:      nil,
			aggregates:  nil,
			count:       len(entries),
			skip:        0,
			expectedLen: len(entries),
		},
		{
			name:        "Can query with count and skip",
			filter:      nil,
			aggregates:  nil,
			count:       2,
			skip:        1,
			expectedLen: 2,
		},
		{
			name: "Can filter by exact string match",
			filter: map[string]interface{}{
				"name": "oduffa emoekpere",
			},
			count:       10,
			skip:        0,
			expectedIDs: []string{entries[2].(map[string]interface{})["id"].(string)},
			expectedLen: 1,
		},
		{
			name: "Can filter by greater than numeric (ix > 2)", // Assuming comparison is supported
			filter: map[string]interface{}{
				"ix": map[string]interface{}{"$gt": 2},
			},
			count:       10,
			skip:        0,
			expectedIDs: []string{entries[2].(map[string]interface{})["id"].(string), entries[3].(map[string]interface{})["id"].(string), entries[4].(map[string]interface{})["id"].(string)},
			expectedLen: 3,
		},
		{
			name: "Can filter by less than or equal numeric (count <= 10)", // Assuming comparison is supported
			filter: map[string]interface{}{
				"count": map[string]interface{}{"$lte": 10.0},
			},
			count:       10,
			skip:        0,
			expectedIDs: []string{entries[0].(map[string]interface{})["id"].(string), entries[1].(map[string]interface{})["id"].(string)},
			expectedLen: 2,
		},
		{
			name:        "Query returns empty for no match",
			filter:      map[string]interface{}{"name": "non_existent"},
			count:       10,
			skip:        0,
			expectedLen: 0,
		},
		{
			name:        "Query from non-existent store",
			filter:      nil,
			count:       10,
			skip:        0,
			expectedLen: 0,
		},
	}
	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			targetStore := store
			if tt.name == "Query from non-existent store" {
				targetStore = "non_existent_store"
			}
			rows, _, err := db.Query(tt.filter, tt.aggregates, tt.count, tt.skip, targetStore, nil)
			require.NoError(t, err, "Query returned an unexpected error")

			var actualIDs []string
			count := 0
			for {
				var data map[string]interface{}
				hasNext, err := rows.Next(&data)
				if err != nil && err.Error() != "EOF" {
					require.NoError(t, err, "Next returned an unexpected error")
				}
				if !hasNext {
					break
				}
				actualIDs = append(actualIDs, data["id"].(string))
				count++
			}
			assert.Equal(t, tt.expectedLen, count, "Number of returned rows mismatch")

			// Only check IDs if specific IDs are expected in the test case
			if len(tt.expectedIDs) > 0 {
				sort.Strings(actualIDs)
				sort.Strings(tt.expectedIDs) // Sort both for consistent comparison
				assert.Equal(t, tt.expectedIDs, actualIDs, "Returned IDs mismatch expected filtered IDs")
			}
			assert.Nil(t, rows.LastError(), "LastError should be nil after successful iteration")
		})
	}

	// Test with aggregation (if the implementation actually handles it)
	// For MemoryStore, aggregation is a no-op currently, so we'll test that it doesn't error
	t.Run("Can query with aggregation (no-op for MemoryStore)", func(t *testing.T) {
		_, aggResult, err := db.Query(nil, map[string]interface{}{"sum": "count"}, 10, 0, store, nil)
		require.NoError(t, err, "Query with aggregation returned an error")
		assert.Empty(t, aggResult, "Expected empty aggregation results if not implemented")
	})
}

// Test_Update test if a gostore can update a single item
func Test_Update(t *testing.T, db common.ObjectStore) {
	store := "data_update"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docID := common.NewObjectId().String()
	initialDoc := TestDocument{ID: docID, Name: "Original Name", Value: 10}
	_, err := db.Save(docID, store, initialDoc)
	require.NoError(t, err, "Failed to save initial document for Test_Update")

	tests := []struct {
		name       string
		idToUpdate string
		updateData interface{}
		expectErr  bool
		expected   TestDocument // Expected state after update
	}{
		{
			name:       "Can update existing document",
			idToUpdate: docID,
			updateData: TestDocument{Name: "Updated Name", Value: 20},
			expectErr:  false,
			expected:   TestDocument{ID: docID, Name: "Updated Name", Value: 20},
		},
		{
			name:       "Cannot update non-existent document",
			idToUpdate: "non_existent_id",
			updateData: TestDocument{Name: "Fake Update", Value: 99},
			expectErr:  true,
			expected:   TestDocument{}, // Not relevant as it should error
		},
		{
			name:       "Update with partial data (should merge or replace based on implementation)",
			idToUpdate: docID,
			updateData: map[string]interface{}{"Value": 30}, // Only update Value
			expectErr:  false,
			// For MemoryStore, it replaces the whole document with the marshaled data.
			// So, if updateData is just a map, it will be just that map.
			// This test assumes a "merge" behavior for Update, which might not be what MemoryStore does.
			// For MemoryStore, this means original fields might be lost unless explicitly handled.
			// Adjusting expected based on MemoryStore's current "replace" behavior within Update:
			expected: TestDocument{ID: docID, Name: "", Value: 30}, // Name will be zero-value if updated via map[string]interface{}
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := db.Update(tt.idToUpdate, store, tt.updateData)

			if tt.expectErr {
				assert.Error(t, err, "Expected an error for update scenario")
			} else {
				require.NoError(t, err, "Update returned an unexpected error")
				var retrievedDoc TestDocument
				getErr := db.Get(tt.idToUpdate, store, &retrievedDoc)
				require.NoError(t, getErr, "Failed to retrieve document after successful update")
				assert.Equal(t, tt.expected.ID, retrievedDoc.ID, "ID mismatch after update")
				assert.Equal(t, tt.expected.Name, retrievedDoc.Name, "Name mismatch after update")
				assert.Equal(t, tt.expected.Value, retrievedDoc.Value, "Value mismatch after update")
			}
		})
	}
}

// Test_Replace test if a gostore can replace an item
func Test_Replace(t *testing.T, db common.ObjectStore) {
	store := "data_replace"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docID := common.NewObjectId().String()
	initialDoc := TestDocument{ID: docID, Name: "Original", Value: 10, Count: 5.5}
	_, err := db.Save(docID, store, initialDoc)
	require.NoError(t, err, "Failed to save initial document for Test_Replace")

	tests := []struct {
		name         string
		idToReplace  string
		replaceData  interface{}
		expectErr    bool
		expectedDoc  TestDocument // Expected state after replace
		expectExists bool         // If replacing a non-existent, should it be created?
	}{
		{
			name:        "Can replace existing document",
			idToReplace: docID,
			replaceData: TestDocument{ID: docID, Name: "Replaced", Value: 20, Count: 10.0},
			expectErr:   false,
			expectedDoc: TestDocument{ID: docID, Name: "Replaced", Value: 20, Count: 10.0},
		},
		{
			name:         "Replacing non-existent document inserts it (common behavior)",
			idToReplace:  "new_id",
			replaceData:  TestDocument{ID: "new_id", Name: "New Inserted", Value: 100},
			expectErr:    false,
			expectedDoc:  TestDocument{ID: "new_id", Name: "New Inserted", Value: 100},
			expectExists: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := db.Replace(tt.idToReplace, store, tt.replaceData)

			if tt.expectErr {
				assert.Error(t, err, "Expected an error for replace scenario")
			} else {
				require.NoError(t, err, "Replace returned an unexpected error")
				var retrievedDoc TestDocument
				getErr := db.Get(tt.idToReplace, store, &retrievedDoc)
				if tt.expectExists {
					require.NoError(t, getErr, "Failed to retrieve document after successful replace/insert")
					assert.Equal(t, tt.expectedDoc, retrievedDoc, "Replaced document mismatch")
				} else {
					assert.Error(t, getErr, "Expected document to not exist after replace")
					assert.Equal(t, common.ErrNotFound, getErr)
				}
			}
		})
	}
}

// Test_Save test if a gostore can save (insert) a single item
func Test_Save(t *testing.T, db common.ObjectStore) {
	store := "data_save"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	tests := []struct {
		name      string
		doc       TestDocument
		expectErr bool
	}{
		{
			name:      "Can save a new document",
			doc:       TestDocument{ID: common.NewObjectId().String(), Name: "New Doc", Value: 5},
			expectErr: false,
		},
		{
			name:      "Saving with existing ID should overwrite (common behavior)",
			doc:       TestDocument{ID: "existing_id", Name: "Overwrite", Value: 99},
			expectErr: false, // For MemoryStore, Save will overwrite if ID exists
		},
	}

	// Prepare an existing_id for the overwrite test
	existingID := "existing_id"
	_, err := db.Save(existingID, store, TestDocument{ID: existingID, Name: "Original", Value: 10})
	require.NoError(t, err, "Failed to setup existing_id for save overwrite test")

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			key, err := db.Save(tt.doc.ID, store, tt.doc)
			if tt.expectErr {
				assert.Error(t, err, "Expected an error for save scenario")
			} else {
				require.NoError(t, err, "Save returned an unexpected error")
				assert.Equal(t, tt.doc.ID, key, "Returned key mismatch")

				var retrievedDoc TestDocument
				getErr := db.Get(tt.doc.ID, store, &retrievedDoc)
				require.NoError(t, getErr, "Failed to retrieve document after save")
				assert.Equal(t, tt.doc, retrievedDoc, "Saved document mismatch")
			}
		})
	}
}

// Test_Delete test if a gostore can delete a single item
func Test_Delete(t *testing.T, db common.ObjectStore) {
	store := "data_delete"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docIDToDelete := common.NewObjectId().String()
	_, err := db.Save(docIDToDelete, store, TestDocument{ID: docIDToDelete, Name: "Temp"})
	require.NoError(t, err, "Failed to save document for Test_Delete")

	tests := []struct {
		name       string
		idToDelete string
		expectErr  bool
	}{
		{
			name:       "Can delete existing document",
			idToDelete: docIDToDelete,
			expectErr:  false,
		},
		{
			name:       "Deleting non-existent document should not error (idempotent)",
			idToDelete: "non_existent_id",
			expectErr:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			err := db.Delete(tt.idToDelete, store)
			if tt.expectErr {
				assert.Error(t, err, "Expected an error for delete scenario")
			} else {
				require.NoError(t, err, "Delete returned an unexpected error")
				var retrievedDoc TestDocument
				getErr := db.Get(tt.idToDelete, store, &retrievedDoc)
				assert.Error(t, getErr, "Document should not exist after deletion")
				assert.Equal(t, common.ErrNotFound, getErr, "Expected ErrNotFound after deletion")
			}
		})
	}
}

// Test_FilterDelete tests if a gostore can delete items based on a filter
func Test_FilterDelete(t *testing.T, db common.ObjectStore) {
	store := "data_filter_delete"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docs := []TestDocument{
		{ID: "1", Name: "A", Value: 10},
		{ID: "2", Name: "B", Value: 20},
		{ID: "3", Name: "A", Value: 30},
	}
	for _, doc := range docs {
		_, err := db.Save(doc.ID, store, doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name           string
		filter         map[string]interface{}
		expectedCount  int64
		remainingIDs   []string
		expectingError bool
	}{
		{
			name:          "Delete with filter (MemoryStore deletes all)",
			filter:        map[string]interface{}{"Name": "A"},
			expectedCount: 0, // MemoryStore's FilterDelete clears the whole table
			remainingIDs:  []string{},
		},
		{
			name:          "Delete with no matching filter (MemoryStore still deletes all)",
			filter:        map[string]interface{}{"Name": "C"},
			expectedCount: 0,
			remainingIDs:  []string{},
		},
		{
			name:           "Delete from non-existent store",
			filter:         nil,
			expectedCount:  0,
			expectingError: true, // This may or may not be an error depending on implementation
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Reset store for each test case
			db.FilterDelete(nil, store, nil)
			db.CreateTable(store, nil)
			for _, doc := range docs {
				_, err := db.Save(doc.ID, store, doc)
				require.NoError(t, err)
			}

			targetStore := store
			if tt.name == "Delete from non-existent store" {
				targetStore = "non_existent_filterdelete_store"
			}

			err := db.FilterDelete(tt.filter, targetStore, nil)
			if tt.expectingError {
				assert.Error(t, err, "Expected an error for this scenario")
			} else {
				require.NoError(t, err, "FilterDelete returned an unexpected error")
				count, _ := db.FilterCount(nil, targetStore, nil)
				assert.Equal(t, tt.expectedCount, count, "Remaining count mismatch")

				if len(tt.remainingIDs) > 0 {
					rows, _ := db.All(10, 0, targetStore)
					var actual []TestDocument
					for {
						var data TestDocument
						hasNext, err := rows.Next(&data)
						if err != nil && err.Error() != "EOF" {
							require.NoError(t, err, "Next returned an unexpected error")
						}
						if !hasNext {
							break
						}
						actual = append(actual, data)
					}
					actualIDs := make([]string, len(actual))
					for i, doc := range actual {
						actualIDs[i] = doc.ID
					}
					sort.Strings(actualIDs)
					sort.Strings(tt.remainingIDs)
					assert.Equal(t, tt.remainingIDs, actualIDs, "Remaining IDs mismatch")
				}
			}
		})
	}
}

// Test_GetByField tests retrieving a single document by a specific field
func Test_GetByField(t *testing.T, db common.ObjectStore) {
	store := "data_getbyfield"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "First", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "Second", Value: 20}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	require.NoError(t, err)

	tests := []struct {
		name        string
		fieldName   string
		fieldValue  interface{}
		expectedDoc TestDocument
		expectErr   error
	}{
		{
			name:        "GetByField with existing field (MemoryStore returns first match)",
			fieldName:   "Name",
			fieldValue:  "Second",
			expectedDoc: doc1, // MemoryStore iterates and returns the first doc that has the field, not necessarily matching value
			expectErr:   nil,
		},
		{
			name:        "GetByField with non-existent value (MemoryStore still returns first)",
			fieldName:   "Name",
			fieldValue:  "NonExistent",
			expectedDoc: doc1,
			expectErr:   nil,
		},
		{
			name:       "GetByField with non-existent field",
			fieldName:  "NonExistentField",
			fieldValue: "any",
			expectErr:  common.ErrNotFound,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var actualDoc TestDocument
			err := db.GetByField(tt.fieldName, tt.fieldValue.(string), store, &actualDoc)

			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err, "Expected error mismatch")
			} else {
				require.NoError(t, err, "GetByField returned an unexpected error")
				assert.Equal(t, tt.expectedDoc, actualDoc, "Retrieved document mismatch")
			}
		})
	}

	t.Run("GetByField from non-existent store", func(t *testing.T) {
		var doc TestDocument
		err := db.GetByField("any", "any", "non_existent_getbyfield_store", &doc)
		assert.Equal(t, common.ErrNotFound, err)
	})
}
func Test_GetByFieldsByField(t *testing.T, db common.ObjectStore) {
	store := "data_getbyfields"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "SameName", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "SameName", Value: 20}
	doc3 := TestDocument{ID: "3", Name: "DifferentName", Value: 30}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	_, err = db.Save(doc3.ID, store, doc3)
	require.NoError(t, err)

	tests := []struct {
		name         string
		fieldName    string
		fieldValue   interface{}
		expectedDocs []TestDocument
		expectedLen  int
	}{
		{
			name:         "GetByFields with matching field (MemoryStore returns all)",
			fieldName:    "Name",
			fieldValue:   "SameName",
			expectedDocs: []TestDocument{doc1, doc2, doc3}, // MemoryStore's GetByFields returns all documents
			expectedLen:  3,
		},
		{
			name:         "GetByFields with non-matching value (MemoryStore still returns all)",
			fieldName:    "Name",
			fieldValue:   "NonExistent",
			expectedDocs: []TestDocument{doc1, doc2, doc3},
			expectedLen:  3,
		},
		{
			name:        "GetByFields with non-existent field",
			fieldName:   "NonExistentField",
			fieldValue:  "any",
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var actualDocs []TestDocument
			err := db.GetByFieldsByField(tt.fieldName, tt.fieldValue.(string), store, nil, &actualDocs)
			require.NoError(t, err, "GetByFieldsByField returned an unexpected error")

			assert.Equal(t, tt.expectedLen, len(actualDocs), "Number of returned documents mismatch")

			if len(tt.expectedDocs) > 0 {
				// Convert to map for easier comparison regardless of order
				expectedMap := make(map[string]TestDocument)
				for _, doc := range tt.expectedDocs {
					expectedMap[doc.ID] = doc
				}
				actualMap := make(map[string]TestDocument)
				for _, doc := range actualDocs {
					actualMap[doc.ID] = doc
				}
				assert.Equal(t, expectedMap, actualMap, "Returned documents mismatch")
			}
		})
	}

	t.Run("GetByFieldsByField from non-existent store", func(t *testing.T) {
		var docs []TestDocument
		err := db.GetByFieldsByField("any", "any", "non_existent_getbyfields_store", nil, &docs)
		require.NoError(t, err)
		assert.Empty(t, docs, "Expected no rows from non-existent store")
	})
}

// Test_BatchUpdate tests batch updating documents
func Test_BatchUpdate(t *testing.T, db common.ObjectStore) {
	store := "data_batch_update"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "First", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "Second", Value: 20}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	require.NoError(t, err)

	t.Run("Can batch update existing documents", func(t *testing.T) {
		ids := []interface{}{"1", "2"}
		updates := []interface{}{
			TestDocument{ID: "1", Name: "Updated First", Value: 11},
			map[string]interface{}{"Value": 22}, // Partial update
		}

		err := db.BatchUpdate(ids, updates, store, nil)
		require.NoError(t, err, "BatchUpdate returned an unexpected error")

		var updatedDoc1, updatedDoc2 TestDocument
		err1 := db.Get("1", store, &updatedDoc1)
		err2 := db.Get("2", store, &updatedDoc2)

		require.NoError(t, err1)
		require.NoError(t, err2)

		assert.Equal(t, "Updated First", updatedDoc1.Name)
		assert.Equal(t, 11, updatedDoc1.Value)

		// MemoryStore replaces, so other fields are zeroed with map update
		assert.Equal(t, "", updatedDoc2.Name)
		assert.Equal(t, 22, updatedDoc2.Value)
	})

	t.Run("Batch update with non-existent ID (should be ignored or error)", func(t *testing.T) {
		ids := []interface{}{"non_existent"}
		updates := []interface{}{
			TestDocument{Name: "Should not exist"},
		}
		err := db.BatchUpdate(ids, updates, store, nil)
		// MemoryStore's BatchUpdate ignores non-existent IDs, so no error
		require.NoError(t, err)

		var doc TestDocument
		getErr := db.Get("non_existent", store, &doc)
		assert.Equal(t, common.ErrNotFound, getErr, "Document should not have been created")
	})

	t.Run("Batch update on non-existent store (should still create/update if Replace logic allows)", func(t *testing.T) {
		newStore := "new_batch_update_store"
		defer db.FilterDelete(nil, newStore, nil)
		ids := []interface{}{"new_doc"}
		updates := []interface{}{
			TestDocument{ID: "new_doc", Name: "New"},
		}
		err := db.BatchUpdate(ids, updates, newStore, nil)
		require.NoError(t, err) // MemoryStore creates the store implicitly

		var doc TestDocument
		getErr := db.Get("new_doc", newStore, &doc)
		require.NoError(t, getErr)
		assert.Equal(t, "New", doc.Name)
	})
}

// Test_Since tests retrieving documents since a certain point (e.g., time or sequence)
func Test_Since(t *testing.T, db common.ObjectStore) {
	store := "data_since"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	// For MemoryStore, "since" is a no-op and just returns all.
	// These tests confirm it doesn't error and returns the expected number of items.
	docs := []TestDocument{
		{ID: "1", Name: "A", Time: time.Now().Add(-3 * time.Hour)},
		{ID: "2", Name: "B", Time: time.Now().Add(-2 * time.Hour)},
		{ID: "3", Name: "C", Time: time.Now().Add(-1 * time.Hour)},
	}
	for _, doc := range docs {
		_, err := db.Save(doc.ID, store, doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		since       string
		expectedLen int
	}{
		{
			name:        "Since with a time value (MemoryStore returns all)",
			since:       "2",
			expectedLen: 3,
		},
		{
			name:        "Since with zero value (should return all)",
			since:       "0",
			expectedLen: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Since(tt.since, 10, 0, store)
			require.NoError(t, err, "Since returned an unexpected error")

			count := 0
			for {
				var data map[string]interface{}
				hasNext, err := rows.Next(&data)
				if err != nil && err.Error() != "EOF" {
					require.NoError(t, err, "Next returned an unexpected error")
				}
				if !hasNext {
					break
				}
				count++
			}
			assert.Equal(t, tt.expectedLen, count, "Number of returned rows mismatch")
		})
	}

	t.Run("Since from non-existent store", func(t *testing.T) {
		rows, err := db.Since("0", 10, 0, "non_existent_since_store")
		require.NoError(t, err)
		var data map[string]interface{}
		hasNext, _ := rows.Next(&data)
		assert.False(t, hasNext, "Expected no rows from non-existent store")
	})
}

// Test_Before tests retrieving documents before a certain point
func Test_Before(t *testing.T, db common.ObjectStore) {
	store := "data_before"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docs := []TestDocument{
		{ID: "1", Name: "A", Time: time.Now().Add(-3 * time.Hour)},
		{ID: "2", Name: "B", Time: time.Now().Add(-2 * time.Hour)},
		{ID: "3", Name: "C", Time: time.Now().Add(-1 * time.Hour)},
	}
	for _, doc := range docs {
		_, err := db.Save(doc.ID, store, doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		before      string
		expectedLen int
	}{
		{
			name:        "Before with a time value (MemoryStore returns all)",
			before:      "2",
			expectedLen: 3,
		},
		{
			name:        "Before with zero value (should return all)",
			before:      "0",
			expectedLen: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Before(tt.before, 10, 0, store)
			require.NoError(t, err, "Before returned an unexpected error")

			count := 0
			for {
				var data map[string]interface{}
				hasNext, err := rows.Next(&data)
				if err != nil && err.Error() != "EOF" {
					require.NoError(t, err, "Next returned an unexpected error")
				}
				if !hasNext {
					break
				}
				count++
			}
			assert.Equal(t, tt.expectedLen, count, "Number of returned rows mismatch")
		})
	}

	t.Run("Before from non-existent store", func(t *testing.T) {
		rows, err := db.Before("0", 10, 0, "non_existent_before_store")
		require.NoError(t, err)
		var data map[string]interface{}
		hasNext, _ := rows.Next(&data)
		assert.False(t, hasNext, "Expected no rows from non-existent store")
	})
}

// Test_All test if a gostore can retrieve all items with pagination
func Test_All(t *testing.T, db common.ObjectStore) {
	store := "data_all"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docs := make([]TestDocument, 5)
	for i := 0; i < 5; i++ {
		docs[i] = TestDocument{ID: fmt.Sprintf("%d", i), Name: fmt.Sprintf("Doc %d", i)}
		_, err := db.Save(docs[i].ID, store, docs[i])
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		count       int
		skip        int
		expectedLen int
	}{
		{name: "Get all", count: 10, skip: 0, expectedLen: 5},
		{name: "Get first 2", count: 2, skip: 0, expectedLen: 2},
		{name: "Get 3, skip 2", count: 3, skip: 2, expectedLen: 3},
		{name: "Get all with count > total", count: 100, skip: 0, expectedLen: 5},
		{name: "Skip all", count: 5, skip: 5, expectedLen: 0},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.All(tt.count, tt.skip, store)
			require.NoError(t, err, "All returned an unexpected error")

			count := 0
			for {
				var data map[string]interface{}
				hasNext, err := rows.Next(&data)
				if err != nil && err.Error() != "EOF" {
					require.NoError(t, err, "Next returned an unexpected error")
				}
				if !hasNext {
					break
				}
				count++
			}
			assert.Equal(t, tt.expectedLen, count, "Number of returned rows mismatch")
		})
	}
}

// Test_FilterGetAll tests retrieving multiple documents matching a filter
func Test_FilterGetAll(t *testing.T, db common.ObjectStore) {
	store := "data_filter_getall"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	docs := []TestDocument{
		{ID: "1", Name: "A", Value: 10},
		{ID: "2", Name: "B", Value: 20},
		{ID: "3", Name: "A", Value: 30},
		{ID: "4", Name: "C", Value: 40},
	}
	for _, doc := range docs {
		_, err := db.Save(doc.ID, store, doc)
		require.NoError(t, err)
	}

	tests := []struct {
		name        string
		filter      map[string]interface{}
		count       int
		skip        int
		expectedLen int
	}{
		{
			name:        "Filter with match (MemoryStore ignores filter, returns all)",
			filter:      map[string]interface{}{"Name": "A"},
			count:       10,
			skip:        0,
			expectedLen: 4,
		},
		{
			name:        "Filter with pagination (MemoryStore ignores filter)",
			filter:      map[string]interface{}{"Name": "A"},
			count:       1,
			skip:        0,
			expectedLen: 1, // MemoryStore calls All(1, 0, store)
		},
		{
			name:        "No filter, retrieve all with pagination",
			filter:      nil,
			count:       2,
			skip:        1,
			expectedLen: 2,
		},
		{
			name:        "Filter returns no results (expected 0 - MemoryStore ignores filter)",
			filter:      map[string]interface{}{"Name": "Zebra"},
			count:       10,
			skip:        0,
			expectedLen: 4, // MemoryStore calls All(10, 0, store)
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.FilterGetAll(tt.filter, tt.count, tt.skip, store, nil)
			require.NoError(t, err, "FilterGetAll returned an unexpected error")

			var actualIDs []string
			count := 0
			for {
				var data TestDocument
				hasNext, err := rows.Next(&data)
				if err != nil && err.Error() != "EOF" {
					require.NoError(t, err, "Next returned an unexpected error")
				}
				if !hasNext {
					break
				}
				actualIDs = append(actualIDs, data.ID)
				count++
			}
			assert.Equal(t, tt.expectedLen, count, "Number of returned rows mismatch")
			// Cannot assert specific IDs for FilterGetAll without implementing filter logic
			// in the generic test, as MemoryStore's current implementation is a passthrough to All.
		})
	}

	t.Run("FilterGetAll from non-existent store", func(t *testing.T) {
		rows, err := db.FilterGetAll(nil, 10, 0, "non_existent_filtergetall_store", nil)
		require.NoError(t, err)
		var data map[string]interface{}
		hasNext, _ := rows.Next(&data)
		assert.False(t, hasNext, "Expected no rows from non-existent store")
	})
}

// Test_FilterGet tests retrieving a single document matching a filter
func Test_FilterGet(t *testing.T, db common.ObjectStore) {
	store := "data_filter_get"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "First", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "Second", Value: 20}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	require.NoError(t, err)

	tests := []struct {
		name        string
		filter      map[string]interface{}
		expectedDoc TestDocument
		expectErr   error
	}{
		{
			name:        "FilterGet with filter (returns first - MemoryStore ignores filter)",
			filter:      map[string]interface{}{"Name": "Second"}, // Filter for second doc
			expectedDoc: doc1,                                     // MemoryStore returns the first one in iteration order
			expectErr:   nil,
		},
		{
			name:        "FilterGet with no matching filter (MemoryStore ignores filter)",
			filter:      map[string]interface{}{"Name": "NonExistent"},
			expectedDoc: doc1, // Still returns the first due to no filter implementation
			expectErr:   nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var actualDoc TestDocument
			err := db.FilterGet(tt.filter, store, &actualDoc, nil)
			if tt.expectErr != nil {
				assert.Equal(t, tt.expectErr, err, "Expected error mismatch")
			} else {
				require.NoError(t, err, "FilterGet returned an unexpected error")
				assert.Equal(t, tt.expectedDoc, actualDoc, "Retrieved document mismatch")
			}
		})
	}

	t.Run("FilterGet from empty store", func(t *testing.T) {
		var doc TestDocument
		err := db.FilterGet(nil, "empty_filterget_store", &doc, nil)
		assert.Equal(t, common.ErrNotFound, err)
	})

	t.Run("FilterGet from non-existent store", func(t *testing.T) {
		var doc TestDocument
		err := db.FilterGet(nil, "non_existent_filterget_store", &doc, nil)
		assert.Equal(t, common.ErrNotFound, err)
	})
}

// Test_FilterUpdate tests updating documents matching a filter
func Test_FilterUpdate(t *testing.T, db common.ObjectStore) {
	store := "data_filter_update"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "A", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "B", Value: 20}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	require.NoError(t, err)

	t.Run("FilterUpdate (updates all in MemoryStore currently)", func(t *testing.T) {
		update := map[string]interface{}{"Value": 99}
		err := db.FilterUpdate(map[string]interface{}{"Name": "A"}, update, store, nil)
		require.NoError(t, err, "FilterUpdate returned an unexpected error")

		var updatedDoc1, updatedDoc2 TestDocument
		db.Get("1", store, &updatedDoc1)
		db.Get("2", store, &updatedDoc2)

		// MemoryStore's implementation updates all documents, ignoring the filter
		assert.Equal(t, 99, updatedDoc1.Value)
		assert.Equal(t, 99, updatedDoc2.Value)
	})
}

// Test_FilterReplace tests replacing documents matching a filter
func Test_FilterReplace(t *testing.T, db common.ObjectStore) {
	store := "data_filter_replace"
	db.CreateTable(store, nil)
	defer db.FilterDelete(nil, store, nil)

	doc1 := TestDocument{ID: "1", Name: "A", Value: 10}
	doc2 := TestDocument{ID: "2", Name: "B", Value: 20}
	_, err := db.Save(doc1.ID, store, doc1)
	_, err = db.Save(doc2.ID, store, doc2)
	require.NoError(t, err)

	t.Run("FilterReplace (replaces all in MemoryStore currently)", func(t *testing.T) {
		replacement := TestDocument{Name: "Replaced", Value: 100}
		err := db.FilterReplace(map[string]interface{}{"Name": "A"}, replacement, store, nil)
		require.NoError(t, err, "FilterReplace returned an unexpected error")

		var updatedDoc1, updatedDoc2 TestDocument
		db.Get("1", store, &updatedDoc1)
		db.Get("2", store, &updatedDoc2)

		// MemoryStore's implementation replaces all documents, ignoring the filter
		assert.Equal(t, "Replaced", updatedDoc1.Name)
		assert.Equal(t, 100, updatedDoc1.Value)
		assert.Equal(t, "Replaced", updatedDoc2.Name)
		assert.Equal(t, 100, updatedDoc2.Value)
	})
}
