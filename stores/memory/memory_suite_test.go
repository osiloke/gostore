package memory

import (
	"testing"

	gostoretesting "github.com/osiloke/gostore/testing"
)

func TestMemoryStoreSuite(t *testing.T) {
	db := NewMemoryStore()

	// Basic CRUD and essential functionalities
	t.Run("Test_Save", func(t *testing.T) { gostoretesting.Test_Save(t, db) })
	t.Run("Test_Get", func(t *testing.T) { gostoretesting.Test_Get(t, db) })
	t.Run("Test_Update", func(t *testing.T) { gostoretesting.Test_Update(t, db) })
	t.Run("Test_Replace", func(t *testing.T) { gostoretesting.Test_Replace(t, db) })
	t.Run("Test_Delete", func(t *testing.T) { gostoretesting.Test_Delete(t, db) })

	// Batch operations
	t.Run("Test_BatchInsert", func(t *testing.T) { gostoretesting.Test_BatchInsert(t, db) })
	t.Run("Test_BatchUpdate", func(t *testing.T) { gostoretesting.Test_BatchUpdate(t, db) })

	// Retrieval methods
	t.Run("Test_All", func(t *testing.T) { gostoretesting.Test_All(t, db) })
	t.Run("Test_Since", func(t *testing.T) { gostoretesting.Test_Since(t, db) })
	t.Run("Test_Before", func(t *testing.T) { gostoretesting.Test_Before(t, db) })
	t.Run("Test_GetByField", func(t *testing.T) { gostoretesting.Test_GetByField(t, db) })
	t.Run("Test_GetByFieldsByField", func(t *testing.T) { gostoretesting.Test_GetByFieldsByField(t, db) })

	// Filtered operations
	t.Run("Test_Query", func(t *testing.T) { gostoretesting.Test_Query(t, db) })
	t.Run("Test_FilterGetAll", func(t *testing.T) { gostoretesting.Test_FilterGetAll(t, db) })
	t.Run("Test_FilterGet", func(t *testing.T) { gostoretesting.Test_FilterGet(t, db) })
	t.Run("Test_FilterUpdate", func(t *testing.T) { gostoretesting.Test_FilterUpdate(t, db) })
	t.Run("Test_FilterReplace", func(t *testing.T) { gostoretesting.Test_FilterReplace(t, db) })
	t.Run("Test_FilterDelete", func(t *testing.T) { gostoretesting.Test_FilterDelete(t, db) })

	db.Close()
}
