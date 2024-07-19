package testing

import (
	"testing"

	"github.com/osiloke/gostore"
	"github.com/stretchr/testify/assert"
)

// Test_Get test if a gostore can retrieve a singlee item
func Test_Get(t *testing.T, db gostore.ObjectStore) {
	store := "data"
	db.CreateTable(store, nil)
	rows := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can retrieve",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				row := rows[0].(map[string]interface{})
				db.Get(row["id"].(string), store, &dst)
				assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}

// Test_BatchInsert test if a gostore can insert multiple entries
func Test_BatchInsert(t *testing.T, db gostore.ObjectStore) {
	store := "data"
	db.CreateTable(store, nil)
	rows := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can retrieve",
			func(t *testing.T) {
				res, _ := db.All(10, 0, store)
				total := 0
				ok := true
				for ok {
					_, ok = res.NextRaw()
					if ok {
						total++
					}
				}
				assert.Equal(t, total, len(rows), "number of rows saved is equal to number retrieved")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}

// Test_Query test if a gostore can query items
func Test_Query(t *testing.T, db gostore.ObjectStore) {
	store := "data"
	db.CreateTable(store, nil)
	entries := []interface{}{
		map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
			"ix":    int64(1),
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
			"ix":    int64(2),
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
			"ix":    int64(3),
		}, map[string]interface{}{
			"id":    gostore.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
			"ix":    int64(4),
		},
	}
	db.BatchInsert(entries, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can query",
			func(t *testing.T) {
				rows, _, err := db.Query(nil, nil, 1, 0, store, nil)
				assert.Empty(t, err, "An error was returned")
				if err == nil {
					var data map[string]interface{}
					rows.Next(&data)
					if ok := assert.Nil(t, rows.LastError(), ""); !ok {
						t.Error(rows.LastError())
						return
					}
					if ok := assert.Equal(t, entries[0], data, "Returned data was nol"); !ok {
						return
					}
					if ok := assert.Empty(t, rows.LastError(), "An error was returned after getting next item"); !ok {
						return
					}
					rows.NextRaw()
				}
				// assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
		{
			"Can filter",
			func(t *testing.T) {
				rows, _, err := db.Query(map[string]interface{}{
					"name": "oduffa emoekpere",
				}, nil, 1, 0, store, nil)
				assert.Empty(t, err, "An error was returned")
				if err == nil {
					var data map[string]interface{}
					rows.Next(&data)
					if ok := assert.Nil(t, rows.LastError(), ""); !ok {
						t.Error(rows.LastError())
						return
					}
					if ok := assert.Equal(t, entries[2], data, "Returned data was nol"); !ok {
						return
					}
					if ok := assert.Empty(t, rows.LastError(), "An error was returned after getting next item"); !ok {
						return
					}
					rows.NextRaw()
				}
				// assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
		{
			"Can filter",
			func(t *testing.T) {
				rows, _, err := db.Query(map[string]interface{}{
					"ix": ">:n2",
				}, nil, 1, 0, store, nil)
				assert.Empty(t, err, "An error was returned")
				if err == nil {
					var data map[string]interface{}
					rows.Next(&data)
					if ok := assert.Nil(t, rows.LastError(), ""); !ok {
						t.Error(rows.LastError())
						return
					}
					if ok := assert.Equal(t, entries[1], data, "Returned data was nol"); !ok {
						return
					}
					if ok := assert.Empty(t, rows.LastError(), "An error was returned after getting next item"); !ok {
						return
					}
					rows.NextRaw()
				}
				// assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}
}
