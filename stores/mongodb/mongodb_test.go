package mongodb_test

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/stores/mongodb"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name    string
		uri     string
		dbName  string
		want    *mongodb.MongoDBStore
		wantErr bool
	}{
		{
			name:    "Success",
			uri:     "mongodb://localhost:27017",
			dbName:  "testdb",
			want:    &mongodb.MongoDBStore{IDField: "_id"},
			wantErr: false,
		},
		{
			name:    "Invalid URI",
			uri:     "invalid-uri",
			dbName:  "testdb",
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			got, err := mongodb.New(ctx, tt.uri, tt.dbName)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil && tt.want != nil {
				t.Errorf("New() = %v, want not nil", got)
			}
			if got != nil {
				defer got.Close()
			}
		})
	}
}

func cleanupCollection(store *mongodb.MongoDBStore, collectionName string) {
	if store != nil {
		_, _ = store.DB().Collection(collectionName).DeleteMany(context.Background(), bson.M{})
	}
}

func TestMongoDBStore_Save(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "testcollection"
	defer cleanupCollection(store, collectionName)

	tests := []struct {
		name    string
		key     string
		store   string
		src     interface{}
		want    string
		wantErr bool
	}{
		{
			name:  "Success",
			key:   "testkey",
			store: collectionName,
			src: map[string]interface{}{
				"id":  "testkey",
				"foo": "bar",
			},
			want:    "testkey",
			wantErr: false,
		},
		{
			name:    "Missing ID",
			key:     "testkey2",
			store:   collectionName,
			src:     map[string]interface{}{"foo": "bar"},
			want:    "testkey2",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.Save(tt.key, tt.store, tt.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("Save() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Save() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMongoDBStore_Get(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "testcollection"
	defer cleanupCollection(store, collectionName)

	// Save a document first to ensure Get can retrieve it
	_, err = store.Save("testkey", collectionName, map[string]interface{}{"id": "testkey", "foo": "bar"})
	if err != nil {
		t.Fatalf("Failed to save document for Get test: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		store   string
		dst     interface{}
		wantErr bool
	}{
		{
			name:    "Success",
			key:     "testkey",
			store:   collectionName,
			dst:     map[string]interface{}{},
			wantErr: false,
		},
		{
			name:    "Not Found",
			key:     "nonexistentkey",
			store:   collectionName,
			dst:     map[string]interface{}{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Get(tt.key, tt.store, &tt.dst)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if _, ok := tt.dst.(map[string]interface{}); ok {
					if dstMap, ok := tt.dst.(map[string]interface{}); ok {
						if _, ok := dstMap["foo"]; !ok {
							t.Errorf("Get() did not retrieve data correctly, missing field 'foo'")
						}
					}
				}
			}
		})
	}
}

func TestMongoDBStore_Delete(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "testcollection"
	defer cleanupCollection(store, collectionName)

	// Save a document first to ensure Delete can delete it
	_, err = store.Save("testkey", collectionName, map[string]interface{}{"id": "testkey", "foo": "bar"})
	if err != nil {
		t.Fatalf("Failed to save document for Delete test: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		store   string
		wantErr bool
	}{
		{
			name:    "Success",
			key:     "testkey",
			store:   collectionName,
			wantErr: false,
		},
		{
			name:    "Not Found",
			key:     "nonexistentkey",
			store:   collectionName,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Delete(tt.key, tt.store)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// Verify deletion by trying to Get, should return NotFound error
			var dst map[string]interface{} = make(map[string]interface{})
			errGet := store.Get(tt.key, tt.store, &dst)
			if tt.name == "Success" && !errors.Is(errGet, common.ErrNotFound) {
				t.Errorf("Delete() should have deleted the document, Get() error = %v, wantErr %v", errGet, common.ErrNotFound)
			}
		})
	}
}

func TestMongoDBStore_All(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "allcollection"
	defer cleanupCollection(store, collectionName)
	emptyCollectionName := "emptycollection"
	defer cleanupCollection(store, emptyCollectionName)

	// Insert some test data
	store.Save("key1", collectionName, map[string]interface{}{"id": "key1", "value": "val1"})
	store.Save("key2", collectionName, map[string]interface{}{"id": "key2", "value": "val2"})

	tests := []struct {
		name      string
		count     int
		skip      int
		store     string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "Success - All documents",
			count:     10,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:      "Success - Limit 1",
			count:     1,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "Success - Skip 1",
			count:     10,
			skip:      1,
			store:     collectionName,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "Success - No documents",
			count:     10,
			skip:      0,
			store:     emptyCollectionName,
			wantErr:   false,
			wantCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := store.All(tt.count, tt.skip, tt.store)
			if (err != nil) != tt.wantErr {
				t.Errorf("All() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if rows == nil && tt.wantCount > 0 {
				t.Errorf("All() rows = nil, want non-nil for count > 0")
				return
			}
			if rows != nil {
				defer rows.Close()
				count := 0
				var dst map[string]interface{}
				for {
					ok, err := rows.Next(&dst)
					if err != nil {
						t.Fatalf("rows.Next() error = %v", err)
					}
					if !ok {
						break
					}
					count++
				}
				if count != tt.wantCount {
					t.Errorf("All() got count = %v, wantCount %v", count, tt.wantCount)
				}
			}
		})
	}
}

func TestMongoDBStore_GetAll(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "getallcollection"
	defer cleanupCollection(store, collectionName)
	emptyCollectionName := "emptycollection"
	defer cleanupCollection(store, emptyCollectionName)

	// Insert some test data
	store.Save("key1", collectionName, map[string]interface{}{"id": "key1", "value": "val1"})
	store.Save("key2", collectionName, map[string]interface{}{"id": "key2", "value": "val2"})

	tests := []struct {
		name      string
		count     int
		skip      int
		store     string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "Success - All documents",
			count:     10,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:      "Success - Limit 1",
			count:     1,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "Success - Skip 1",
			count:     10,
			skip:      1,
			store:     collectionName,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "Success - No documents",
			count:     10,
			skip:      0,
			store:     emptyCollectionName,
			wantErr:   false,
			wantCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rawRows, err := store.GetAll(tt.count, tt.skip, tt.store)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(rawRows) != tt.wantCount {
				t.Errorf("GetAll() got count = %v, wantCount %v", len(rawRows), tt.wantCount)
			}
		})
	}
}

func TestMongoDBStore_Filter(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "filtercollection"
	defer cleanupCollection(store, collectionName)

	// Insert some test data
	store.Save("key1", collectionName, map[string]interface{}{"id": "key1", "value": "val1", "type": "A"})
	store.Save("key2", collectionName, map[string]interface{}{"id": "key2", "value": "val2", "type": "B"})
	store.Save("key3", collectionName, map[string]interface{}{"id": "key3", "value": "val3", "type": "A"})

	tests := []struct {
		name      string
		filter    map[string]interface{}
		count     int
		skip      int
		store     string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "Success - Filter by type A",
			filter:    map[string]interface{}{"type": "A"},
			count:     10,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:      "Success - Filter by type B",
			filter:    map[string]interface{}{"type": "B"},
			count:     10,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 1,
		},
		{
			name:      "Success - No matching filter",
			filter:    map[string]interface{}{"type": "C"},
			count:     10,
			skip:      0,
			store:     collectionName,
			wantErr:   false,
			wantCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := store.Filter(tt.filter, tt.count, tt.skip, tt.store)
			if (err != nil) != tt.wantErr {
				t.Errorf("Filter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if rows != nil {
				defer rows.Close()
				count := 0
				var dst map[string]interface{}
				for {
					ok, err := rows.Next(&dst)
					if err != nil {
						t.Fatalf("rows.Next() error = %v", err)
					}
					if !ok {
						break
					}
					count++
				}
				if count != tt.wantCount {
					t.Errorf("Filter() got count = %v, wantCount %v", count, tt.wantCount)
				}
			} else if tt.wantCount > 0 {
				t.Errorf("Filter() rows = nil, want non-nil for count > 0")
			}
		})
	}
}

func TestMongoDBStore_Count(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "countcollection"
	defer cleanupCollection(store, collectionName)
	emptyCollectionName := "emptycollection"
	defer cleanupCollection(store, emptyCollectionName)

	// Insert some test data
	store.Save("key1", collectionName, map[string]interface{}{"id": "key1", "value": "val1"})
	store.Save("key2", collectionName, map[string]interface{}{"id": "key2", "value": "val2"})

	tests := []struct {
		name      string
		store     string
		wantErr   bool
		wantCount int
	}{
		{
			name:      "Success - Count documents",
			store:     collectionName,
			wantErr:   false,
			wantCount: 2,
		},
		{
			name:      "Success - No documents",
			store:     emptyCollectionName,
			wantErr:   false,
			wantCount: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := store.Count(tt.store)
			if (err != nil) != tt.wantErr {
				t.Errorf("Count() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if count != tt.wantCount {
				t.Errorf("Count() got count = %v, wantCount %v", count, tt.wantCount)
			}
		})
	}
}

func TestMongoDBStore_Update(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "updatecollection"
	defer cleanupCollection(store, collectionName)

	// Insert a document first to ensure Update can update it
	_, err = store.Save("testkey", collectionName, map[string]interface{}{"id": "testkey", "foo": "bar"})
	if err != nil {
		t.Fatalf("Failed to save document for Update test: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		store   string
		src     interface{}
		wantErr bool
	}{
		{
			name:    "Success",
			key:     "testkey",
			store:   collectionName,
			src:     map[string]interface{}{"foo": "baz"},
			wantErr: false,
		},
		{
			name:    "Not Found Key",
			key:     "nonexistentkey",
			store:   collectionName,
			src:     map[string]interface{}{"foo": "baz"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Update(tt.key, tt.store, tt.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("Update() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Verify update
			if tt.name == "Success" {
				var dst map[string]interface{} = make(map[string]interface{})
				errGet := store.Get(tt.key, tt.store, &dst)
				if errGet != nil {
					t.Fatalf("Get() after Update() error = %v", errGet)
				}
				if dst["foo"] != "baz" {
					t.Errorf("Update() did not update data correctly, got foo = %v, want foo = baz", dst["foo"])
				}
			}
		})
	}
}

func TestMongoDBStore_DB(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	db := store.DB()
	if db == nil {
		t.Errorf("DB() returned nil, want non-nil *mongo.Database")
	}
	if db.Name() != "test" {
		t.Errorf("DB().Name() got = %v, want = test", db.Name())
	}
}

func TestMongoDBStore_AllCursor(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "allcursorcollection"
	defer cleanupCollection(store, collectionName)

	// Insert some test data
	store.Save("key1", collectionName, map[string]interface{}{"id": "key1", "value": "val1"})
	store.Save("key2", collectionName, map[string]interface{}{"id": "key2", "value": "val2"})

	rows, err := store.AllCursor(collectionName)
	if err != nil {
		t.Errorf("AllCursor() error = %v", err)
	}
	defer rows.Close()

	count := 0
	var dst map[string]interface{}
	for {
		ok, err := rows.Next(&dst)
		if err != nil {
			t.Fatalf("rows.Next() error = %v", err)
		}
		if !ok {
			break
		}
		count++
	}
	if count != 2 {
		t.Errorf("AllCursor() got count = %v, want 2", count)
	}
}

func TestMongoDBStore_SaveAll(t *testing.T) {
	ctx := context.Background()
	store, err := mongodb.New(ctx, "mongodb://localhost:27017", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	collectionName := "saveallcollection"
	defer cleanupCollection(store, collectionName)

	docs := []interface{}{
		map[string]interface{}{"id": "doc1", "value": "val1"},
		map[string]interface{}{"id": "doc2", "value": "val2"},
	}

	keys, err := store.SaveAll(collectionName, docs...)
	if err != nil {
		t.Errorf("SaveAll() error = %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("SaveAll() returned %d keys, want 2", len(keys))
	}
}

// func TestMongoRows_Next_NextRaw_Close_LastError(t *testing.T) {

// 	mockCursor := &mongo.Cursor{}
// 	nextCalled := false
// 	mockCursor.Next = func(ctx context.Context) bool {
// 		nextCalled = true
// 		return true
// 	}
// 	mockCursor.Decode = func(dst interface{}) error {
// 		if nextCalled {
// 			dst.(*map[string]interface{})["_id"] = "key1"
// 			dst.(*map[string]interface{})["value"] = "val1"
// 		}
// 		return nil
// 	}
// 	mockCursor.Err = func() error { return nil }
// 	mockCursor.Close = func(ctx context.Context) {}
// 	rawBson := bson.D{{"_id", "key1"}, {"value", "val1"}}
// 	rawBytes, _ := bson.Marshal(rawBson)
// 	mockCursor.Current = rawBytes

// 	rows := &mongodb.MongoRows{cursor: mockCursor, ctx: context.Background()}

// 	var dst map[string]interface{}
// 	ok, err := rows.Next(&dst)
// 	if err != nil {
// 		t.Errorf("rows.Next() error = %v", err)
// 	}
// 	if !ok {
// 		t.Errorf("rows.Next() ok = false, want true")
// 	}
// 	if dst["_id"] != "key1" {
// 		t.Errorf("rows.Next() dst _id = %v, want key1", dst["_id"])
// 	}

// 	raw, ok := rows.NextRaw()
// 	if !ok {
// 		t.Errorf("rows.NextRaw() ok = false, want true")
// 	}
// 	var rawDst map[string]interface{}
// 	err = bson.Unmarshal(raw, &rawDst)
// 	if err != nil {
// 		t.Fatalf("bson.Unmarshal error = %v", err)
// 	}
// 	if rawDst["_id"] != "key1" {
// 		t.Errorf("rows.NextRaw() dst _id = %v, want key1", rawDst["_id"])
// 	}

// 	if err := rows.LastError(); err != nil {
// 		t.Errorf("LastError() error = %v, want nil", err)
// 	}

// 	rows.Close()

// 	ok, _ = rows.Next(&dst)
// 	if ok {
// 		t.Errorf("rows.Next() after close ok = true, want false")
// 	}
// }

// func TestMongoDBStore_BeginTransaction_Commit_Discard(t *testing.T) {
//
// 	mockSession := &mongo.Session{}
// 	mockSession.CommitTransaction = func(ctx context.Context) error { return nil }
// 	mockSession.AbortTransaction = func(ctx context.Context) {}

// 	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "test")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer store.Close()

// 	txn, err := store.BeginTransaction()
// 	if err != nil {
// 		t.Fatalf("BeginTransaction() error = %v", err)
// 	}

// 	err = txn.Commit()
// 	if err != nil {
// 		t.Errorf("Commit() error = %v", err)
// 	}

// 	txn2, err := store.BeginTransaction()
// 	if err != nil {
// 		t.Fatalf("BeginTransaction() error = %v", err)
// 	}
// 	txn2.Discard()
// }

// func TestMongoTransaction_NotImplemented(t *testing.T) {
// 	txn := &mongodb.MongoTransaction{}
// 	testCases := []struct {
// 		name string
// 		fn   func() error
// 	}{
// 		{"Set", func() error { return txn.Set([]byte("key"), []byte("value")) }},
// 		{"Get", func() error { _, err := txn.Get([]byte("key")); return err }},
// 		{"Delete", func() error { return txn.Delete([]byte("key")) }},
// 		{"Restart", func() error { return txn.Restart() }},
// 	}

// 	for _, tc := range testCases {
// 		err := tc.fn()
// 		if !errors.Is(err, common.ErrNotImplemented) {
// 			t.Errorf("%s() error = %v, want 'not implemented' error", tc.name, err)
// 		}
// 	}
// }
