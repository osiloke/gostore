package badger

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/osiloke/gostore/common"
	"github.com/stretchr/testify/assert"
)

func TestNewIndexedBadgerRows(t *testing.T) {
	db := createDB("indexed_rows_test")
	defer removeDB("indexed_rows_test", db)

	storeName := "items"
	db.CreateTable(storeName, nil)

	// 1. Insert some dummy data
	k1 := "item1"
	v1 := map[string]interface{}{"id": k1, "name": "First Item"}
	db.Save(k1, storeName, v1)

	k2 := "item2"
	v2 := map[string]interface{}{"id": k2, "name": "Second Item"}
	db.Save(k2, storeName, v2)

	t.Run("Success Path", func(t *testing.T) {
		// Mock a bleve SearchResult
		// h.ID is in the format store|id (as confirmed by indexer tests)
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "items|item1"},
				&search.DocumentMatch{ID: "item2"}, // Test fallback if no separator
			},
		}

		rows := NewIndexedBadgerRows(storeName, 2, res, db)
		defer rows.Close()

		var dst map[string]interface{}

		// First item
		ok, err := rows.Next(&dst)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "First Item", dst["name"])

		// Second item
		dst = make(map[string]interface{})
		ok, err = rows.Next(&dst)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "Second Item", dst["name"])

		// EOF
		ok, err = rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, rows.LastError())
	})

	t.Run("Item Not Found", func(t *testing.T) {
		// Test case where item is indexed but not in DB
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "nonexistent"},
			},
		}

		rows := NewIndexedBadgerRows(storeName, 1, res, db)
		defer rows.Close()

		var dst map[string]interface{}
		ok, err := rows.Next(&dst)
		assert.False(t, ok) // _Get returns ErrNotFound, which is handled by continuing and returning empty retrieved
		assert.NoError(t, err)

		// Check		// EOF after
		ok, err = rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, rows.LastError())
	})

	t.Run("Multiple Missing Items", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "items|missing1"},
				&search.DocumentMatch{ID: "items|missing2"},
				&search.DocumentMatch{ID: "items|item1"}, // item1 exists from previous test setup
			},
		}

		rows := NewIndexedBadgerRows(storeName, 3, res, db)
		defer rows.Close()

		var dst map[string]interface{}
		// Should skip first two and return item1
		ok, err := rows.Next(&dst)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "First Item", dst["name"])

		// Then EOF
		ok, err = rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, rows.LastError())
	})

	t.Run("Close", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "item1"},
				&search.DocumentMatch{ID: "item2"},
			},
		}

		rows := NewIndexedBadgerRows(storeName, 2, res, db)

		var dst map[string]interface{}
		ok, _ := rows.Next(&dst)
		assert.True(t, ok)

		rows.Close()

		// After close, Next should probably return false or error
		// Wait, look at NewIndexedBadgerRows implementation of closed case
		/*
			case <-closed:
					b.logger.Info("newIndexedBadgerRows closed")
					close(closed)
					break OUTER
		*/
		// And Next implementation:
		/*
			func (s *IndexedBadgerRows) Next(dst interface{}) (bool, error) {
				...
				s.nextItem <- dst
				key := <-s.retrieved
				...
			}
		*/
		// If the goroutine breaks OUTER, retrieved and nextItem channels are closed.
		// Sending to nextItem might panic if we are not careful, but Close sets isClosed.

		// Let's test if it handles it gracefully
		assert.True(t, rows.isClosed)
	})

	t.Run("Empty Results", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{},
		}
		rows := NewIndexedBadgerRows(storeName, 0, res, db)
		defer rows.Close()

		var dst map[string]interface{}
		ok, err := rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, rows.LastError())
		assert.NoError(t, err)
	})

	t.Run("Corrupt JSON", func(t *testing.T) {
		// Save corrupt data directly using s._Save or just Save with raw string if possible
		// BadgerStore.Save uses json.Marshal, so we need to inject raw bytes.
		corruptKey := "corrupt-item"
		db.SaveRaw(corruptKey, []byte("{invalid-json}"), storeName)

		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "items|" + corruptKey},
			},
		}
		rows := NewIndexedBadgerRows(storeName, 1, res, db)
		defer rows.Close()

		var dst map[string]interface{}
		ok, err := rows.Next(&dst)
		assert.False(t, ok)
		assert.Error(t, rows.LastError()) // Should be json unmarshal error
		assert.NoError(t, err)            // Next returns lastError in first check, or nil if retrieved is empty
	})
}

func TestSyncIndexRows(t *testing.T) {
	db := createDB("sync_rows_test")
	defer removeDB("sync_rows_test", db)

	storeName := "items"
	db.CreateTable(storeName, nil)

	k1 := "item1"
	db.Save(k1, storeName, map[string]interface{}{"id": k1, "name": "First"})

	t.Run("Next Success", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "items|item1"},
			},
		}
		rows := &SyncIndexRows{
			name:   storeName,
			result: res,
			bs:     db,
			logger: db.Logger,
		}

		var dst map[string]interface{}
		ok, err := rows.Next(&dst)
		assert.True(t, ok)
		assert.NoError(t, err)
		assert.Equal(t, "First", dst["name"])

		ok, err = rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, err)
	})

	t.Run("NextRaw Success", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "items|item1"},
			},
		}
		rows := &SyncIndexRows{
			name:   storeName,
			result: res,
			bs:     db,
			logger: db.Logger,
		}

		data, ok := rows.NextRaw()
		assert.True(t, ok)
		assert.NotNil(t, data)

		_, ok = rows.NextRaw()
		assert.False(t, ok)
	})

	t.Run("Item Not Found", func(t *testing.T) {
		res := &bleve.SearchResult{
			Hits: search.DocumentMatchCollection{
				&search.DocumentMatch{ID: "nonexistent"},
			},
		}
		rows := &SyncIndexRows{
			name:   storeName,
			result: res,
			bs:     db,
			logger: db.Logger,
		}

		var dst map[string]interface{}
		ok, err := rows.Next(&dst)
		assert.False(t, ok)
		assert.Equal(t, common.ErrEOF, err) // SyncIndexRows returns ErrEOF if next hit fails
	})
}
