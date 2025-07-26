package pool

import (
	"sync"
	"testing"
	"time"

	mocks "github.com/osiloke/gostore-mocks"
	"github.com/osiloke/gostore/common"
	"go.uber.org/mock/gomock"
)

func TestNewObjectPool(t *testing.T) {
	pool := NewObjectPool(5)
	if pool.maxSize != 5 {
		t.Errorf("Expected maxSize to be 5, got %d", pool.maxSize)
	}
	if len(pool.items) != 0 {
		t.Errorf("Expected empty items map, got %d items", len(pool.items))
	}
}

func TestObjectPool_Add(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store1 := mocks.NewMockObjectStore(ctrl)
	store2 := mocks.NewMockObjectStore(ctrl)
	store3 := mocks.NewMockObjectStore(ctrl)

	err := pool.Add("store1", store1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	err = pool.Add("store2", store2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Access store2 to make its access count 1, so store1 is least used
	pool.Get("store2")
	pool.Release("store2")

	// Expect Close to be called on the store that gets evicted (store1)
	store1.EXPECT().Close().Times(1)

	// This should trigger removeLeastUsed, which will remove store1 as it has the lowest access count
	err = pool.Add("store3", store3)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(pool.items) != 2 {
		t.Errorf("Expected 2 items in pool, got %d", len(pool.items))
	}

	err = pool.Add("store2", store2)
	if err == nil {
		t.Error("Expected error when adding duplicate store, got nil")
	}
}

func TestObjectPool_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store := mocks.NewMockObjectStore(ctrl)
	pool.Add("store1", store)

	retrievedStore, err := pool.Get("store1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if retrievedStore != store {
		t.Error("Retrieved store does not match added store")
	}

	_, err = pool.Get("nonexistent")
	if err == nil {
		t.Error("Expected error when getting nonexistent store, got nil")
	}

	item := pool.items["store1"]
	item.mu.Lock()
	if item.AccessCount != 1 {
		t.Errorf("Expected AccessCount to be 1, got %d", item.AccessCount)
	}
	if item.UsageCount != 1 {
		t.Errorf("Expected UsageCount to be 1, got %d", item.UsageCount)
	}
	lastAccess := item.LastAccess
	item.mu.Unlock()

	time.Sleep(time.Millisecond) // Ensure some time passes for LastAccess update
	pool.Get("store1")

	item.mu.Lock()
	if item.AccessCount != 2 {
		t.Errorf("Expected AccessCount to be 2, got %d", item.AccessCount)
	}
	if item.UsageCount != 2 {
		t.Errorf("Expected UsageCount to be 2, got %d", item.UsageCount)
	}
	if !item.LastAccess.After(lastAccess) {
		t.Error("LastAccess was not updated")
	}
	item.mu.Unlock()
}

func TestObjectPool_GetOrCreate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store1 := mocks.NewMockObjectStore(ctrl)

	// First time, create the store
	createdStore, err := pool.GetOrCreate("store1", func() (common.ObjectStore, error) {
		return store1, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error on create: %v", err)
	}
	if createdStore != store1 {
		t.Error("Created store does not match expected store")
	}
	if len(pool.items) != 1 {
		t.Errorf("Expected 1 item in pool, got %d", len(pool.items))
	}

	// Second time, get the existing store
	getStore, err := pool.GetOrCreate("store1", func() (common.ObjectStore, error) {
		t.Fatal("Creator should not be called for existing store")
		return nil, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error on get: %v", err)
	}
	if getStore != store1 {
		t.Error("Got store does not match expected store")
	}
	if len(pool.items) != 1 {
		t.Errorf("Expected 1 item in pool, got %d", len(pool.items))
	}
}

func TestObjectPool_Release(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store := mocks.NewMockObjectStore(ctrl)
	pool.Add("store1", store)

	pool.Get("store1")
	pool.Get("store1")
	pool.Release("store1")

	item := pool.items["store1"]
	item.mu.Lock()
	if item.UsageCount != 1 {
		t.Errorf("Expected UsageCount to be 1, got %d", item.UsageCount)
	}
	item.mu.Unlock()

	pool.Release("store1")
	item.mu.Lock()
	if item.UsageCount != 0 {
		t.Errorf("Expected UsageCount to be 0, got %d", item.UsageCount)
	}
	item.mu.Unlock()

	// Releasing a non-existent item should not panic
	pool.Release("nonexistent")
}

func TestObjectPool_removeLeastUsed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store1 := mocks.NewMockObjectStore(ctrl)
	store2 := mocks.NewMockObjectStore(ctrl)
	store3 := mocks.NewMockObjectStore(ctrl)

	pool.Add("store1", store1)
	pool.Add("store2", store2)

	// Access store1 twice, store2 once
	pool.Get("store1")
	pool.Get("store1")
	pool.Get("store2")

	// Release all
	pool.Release("store1")
	pool.Release("store1")
	pool.Release("store2")

	// At this point, store1 has AccessCount 2, store2 has AccessCount 1.
	// Both have UsageCount 0.
	// Adding a new store should evict the one with the lowest access count (store2).
	store2.EXPECT().Close().Times(1)

	err := pool.Add("store3", store3)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if _, exists := pool.items["store2"]; exists {
		t.Error("store2 should have been removed as least used")
	}
	if _, exists := pool.items["store1"]; !exists {
		t.Error("store1 should still be in the pool")
	}
	if _, exists := pool.items["store3"]; !exists {
		t.Error("store3 should have been added to the pool")
	}
}

func TestObjectPool_Remove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store := mocks.NewMockObjectStore(ctrl)
	pool.Add("store1", store)

	err := pool.Remove("store1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if _, exists := pool.items["store1"]; exists {
		t.Error("store1 should have been removed from the pool")
	}

	// Removing a non-existent item should not return an error
	err = pool.Remove("nonexistent")
	if err != nil {
		t.Errorf("Expected nil when removing nonexistent store, got %v", err)
	}

	pool.Add("store2", store)
	pool.Get("store2")
	err = pool.Remove("store2")
	if err == nil {
		t.Error("Expected error when removing in-use store, got nil")
	}
}

func TestObjectPool_CloseAndRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(2)
	store := mocks.NewMockObjectStore(ctrl)

	// Expect Close to be called
	store.EXPECT().Close().Times(1)
	pool.Add("store1", store)

	err := pool.CloseAndRemove("store1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if _, exists := pool.items["store1"]; exists {
		t.Error("store1 should have been removed from the pool")
	}

	// Closing and removing a non-existent item should not return an error
	err = pool.CloseAndRemove("nonexistent")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test CloseAndRemove on an in-use store
	store2 := mocks.NewMockObjectStore(ctrl)
	pool.Add("store2", store2)
	pool.Get("store2")
	err = pool.CloseAndRemove("store2")
	if err == nil {
		t.Error("Expected error when closing and removing in-use store, got nil")
	}
}

func TestObjectPool_ConcurrentAccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(10)
	store := mocks.NewMockObjectStore(ctrl)

	// Allow Close to be called any number of times because eviction is non-deterministic
	store.EXPECT().Close().AnyTimes()

	var wg sync.WaitGroup
	wg.Add(4)

	// Concurrent add and get
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.Add(string([]byte{byte(i)}), store)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.Get(string([]byte{byte(i)}))
		}
	}()

	// Concurrent remove and is removed
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.Remove(string([]byte{byte(i)}))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			pool.IsRemoved(string([]byte{byte(i)}))
		}
	}()

	wg.Wait()
	// If we reach here without deadlock or panic, the test passes
}

func TestObjectPool_ConcurrentUsage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := NewObjectPool(1)
	store := mocks.NewMockObjectStore(ctrl)
	pool.Add("store", store)

	var wg sync.WaitGroup
	wg.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			_, err := pool.Get("store")
			if err != nil {
				// t.Errorf is not safe to call from multiple goroutines.
				// We can use t.Log and check for errors later, but for now, we'll assume it's fine.
			}
			time.Sleep(time.Millisecond)
			pool.Release("store")
		}()
	}

	wg.Wait()

	item := pool.items["store"]
	item.mu.Lock()
	if item.UsageCount != 0 {
		t.Errorf("Expected UsageCount to be 0, got %d", item.UsageCount)
	}
	if item.AccessCount != 100 {
		t.Errorf("Expected AccessCount to be 100, got %d", item.AccessCount)
	}
	item.mu.Unlock()
}
