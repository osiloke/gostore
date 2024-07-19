package pool

import (
	"sync"
	"testing"
	"time"

	"github.com/osiloke/gostore/mocks"
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
	pool := NewObjectPool(2)
	store1 := &mocks.MockObjectStore{}
	store2 := &mocks.MockObjectStore{}
	store3 := &mocks.MockObjectStore{}

	err := pool.Add("store1", store1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	err = pool.Add("store2", store2)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	err = pool.Add("store3", store3)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
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
	pool := NewObjectPool(2)
	store := &mocks.MockObjectStore{}
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

	time.Sleep(time.Millisecond) // Ensure some time passes for LastAccess update
	pool.Get("store1")
	item := pool.items["store1"]
	item.mu.Lock()
	if item.AccessCount != 2 {
		t.Errorf("Expected AccessCount to be 2, got %d", item.AccessCount)
	}
	if item.UsageCount != 2 {
		t.Errorf("Expected UsageCount to be 2, got %d", item.UsageCount)
	}
	if item.LastAccess.Before(time.Now().Add(-time.Second)) {
		t.Error("LastAccess was not updated")
	}
	item.mu.Unlock()
}

func TestObjectPool_Release(t *testing.T) {
	pool := NewObjectPool(2)
	store := &mocks.MockObjectStore{}
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
	pool := NewObjectPool(2)
	store1 := &mocks.MockObjectStore{}
	store2 := &mocks.MockObjectStore{}

	pool.Add("store1", store1)
	pool.Add("store2", store2)

	pool.Get("store1")
	pool.Get("store1")
	pool.Get("store2")
	pool.Release("store1")
	pool.Release("store1")
	pool.Release("store2")

	err := pool.Add("store3", &mocks.MockObjectStore{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

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
	pool := NewObjectPool(2)
	store := &mocks.MockObjectStore{}
	pool.Add("store1", store)

	err := pool.Remove("store1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if _, exists := pool.items["store1"]; exists {
		t.Error("store1 should have been removed from the pool")
	}

	err = pool.Remove("nonexistent")
	if err == nil {
		t.Error("Expected error when removing nonexistent store, got nil")
	}

	pool.Add("store2", store)
	pool.Get("store2")
	err = pool.Remove("store2")
	if err == nil {
		t.Error("Expected error when removing in-use store, got nil")
	}
}

func TestObjectPool_IsRemoved(t *testing.T) {
	pool := NewObjectPool(2)
	store := &mocks.MockObjectStore{}
	pool.Add("store1", store)

	if pool.IsRemoved("store1") {
		t.Error("store1 should not be marked as removed")
	}

	pool.Remove("store1")

	if !pool.IsRemoved("store1") {
		t.Error("store1 should be marked as removed")
	}

	if !pool.IsRemoved("nonexistent") {
		t.Error("Nonexistent store should be considered removed")
	}
}

func TestObjectPool_CloseAndRemove(t *testing.T) {
	pool := NewObjectPool(2)
	ctrl := gomock.NewController(t)
	store := mocks.NewMockObjectStore(ctrl)
	store.EXPECT().Close()
	pool.Add("store1", store)

	err := pool.CloseAndRemove("store1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// if !store.closed {
	// 	t.Error("Store should have been closed")
	// }

	if _, exists := pool.items["store1"]; exists {
		t.Error("store1 should have been removed from the pool")
	}

	// Closing and removing a non-existent item should not return an error
	err = pool.CloseAndRemove("nonexistent")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Test CloseAndRemove on an in-use store
	pool.Add("store2", &mocks.MockObjectStore{})
	pool.Get("store2")
	err = pool.CloseAndRemove("store2")
	if err == nil {
		t.Error("Expected error when closing and removing in-use store, got nil")
	}
}

func TestObjectPool_ConcurrentAccess(t *testing.T) {
	pool := NewObjectPool(10)
	store := &mocks.MockObjectStore{}

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

// func TestObjectPool_ConcurrentUsage(t *testing.T) {
// 	pool := NewObjectPool(1)
// 	store := &mocks.MockObjectStore{}
// 	pool.Add("store", store)

// 	var wg sync.WaitGroup
// 	wg.Add(100)

// 	for i := 0; i < 100; i++ {
// 		go func() {
// 			defer wg.Done()
// 			_, err := pool.Get("store")
// 			if err != nil {
// 				t.Errorf("Unexpected error: %v", err)
// 			}
// 			time.Sleep(time.Millisecond)
// 			pool.Release("store")
// 		}()
// 	}

// 	wg.Wait()

// 	item := pool.items["store"]
// 	item.mu.Lock()
// 	if item.UsageCount != 0 {
// 		t.Errorf("Expected UsageCount to be 0, got %d", item.UsageCount)
// 	}
// 	if item.AccessCount != 100 {
// 		t.Errorf
