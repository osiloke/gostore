package pool

import (
	"errors"
	"sync"
	"time"

	common "github.com/osiloke/gostore/common"
)

var ErrAlreadyExists = errors.New("object store with this name already exists")
var ErrNoItemsToUse = errors.New("all items are in use, cannot remove any")

type ObjectStoreItem struct {
	Store       common.ObjectStore
	LastAccess  time.Time
	AccessCount int
	UsageCount  int
	Removed     bool
	mu          sync.Mutex
}

func (o *ObjectStoreItem) Release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.UsageCount--
	if o.UsageCount == 0 && o.Removed {
		o.Store.Close()
	}
}

type ObjectPool struct {
	items   map[string]*ObjectStoreItem
	maxSize int
	mu      sync.RWMutex
}

func NewObjectPool(maxSize int) *ObjectPool {
	return &ObjectPool{
		items:   make(map[string]*ObjectStoreItem),
		maxSize: maxSize,
	}
}

func (p *ObjectPool) Get(name string) (common.ObjectStore, error) {
	p.mu.RLock()
	item, exists := p.items[name]
	p.mu.RUnlock()

	if !exists {
		return nil, errors.New("object store not found")
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	item.LastAccess = time.Now()
	item.AccessCount++
	item.UsageCount++

	return item.Store, nil
}

func (p *ObjectPool) GetItem(name string) (*ObjectStoreItem, error) {
	p.mu.RLock()
	item, exists := p.items[name]
	p.mu.RUnlock()

	if !exists {
		return nil, errors.New("object store not found")
	}

	return item, nil
}

func (p *ObjectPool) GetOrCreate(name string, creator func() (common.ObjectStore, error)) (common.ObjectStore, error) {
	p.mu.RLock()
	item, exists := p.items[name]
	p.mu.RUnlock()

	if exists {
		item.mu.Lock()
		item.LastAccess = time.Now()
		item.AccessCount++
		item.UsageCount++
		item.mu.Unlock()
		return item.Store, nil
	}

	// Item does not exist, so create it.
	// This part is tricky to do without holding the lock for a long time.
	// A common pattern is to lock, check again, and then create.
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check if the item was created while we were waiting for the lock
	item, exists = p.items[name]
	if exists {
		item.mu.Lock()
		item.LastAccess = time.Now()
		item.AccessCount++
		item.UsageCount++
		item.mu.Unlock()
		return item.Store, nil
	}

	// Create new store
	store, err := creator()
	if err != nil {
		return nil, err
	}

	// Add the new store to the pool
	if err := p.add(name, store); err != nil {
		store.Close() // Close the store if it can't be added to the pool
		return nil, err
	}

	return store, nil
}

func (p *ObjectPool) Release(name string) {
	p.mu.RLock()
	item, exists := p.items[name]
	p.mu.RUnlock()

	if !exists {
		return
	}

	item.Release()
}

func (p *ObjectPool) add(name string, store common.ObjectStore) error {

	if _, exists := p.items[name]; exists {
		return ErrAlreadyExists
	}

	if len(p.items) >= p.maxSize {
		if err := p.removeLeastUsed(); err != nil {
			return err
		}
	}

	p.items[name] = &ObjectStoreItem{
		Store:      store,
		LastAccess: time.Now(),
		Removed:    false,
	}

	return nil
}

func (p *ObjectPool) Add(name string, store common.ObjectStore) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.add(name, store)
}

func (p *ObjectPool) removeLeastUsed() error {
	var leastUsed *ObjectStoreItem
	var leastUsedName string

	for name, item := range p.items {
		item.mu.Lock()
		if item.UsageCount == 0 && (leastUsed == nil || item.AccessCount < leastUsed.AccessCount) {
			leastUsed = item
			leastUsedName = name
		}
		item.mu.Unlock()
	}

	if leastUsed == nil {
		return ErrNoItemsToUse
	}

	leastUsed.mu.Lock()
	if leastUsed.UsageCount == 0 {
		// Call Close on the store before removing it from the pool
		if c, ok := leastUsed.Store.(interface{ Close() }); ok {
			c.Close()
		}
	}
	leastUsed.Removed = true
	leastUsed.mu.Unlock()

	delete(p.items, leastUsedName)
	return nil
}

func (p *ObjectPool) Remove(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	item, exists := p.items[name]
	if !exists {
		return nil
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	if item.UsageCount > 0 {
		return errors.New("object store is currently in use")
	}

	item.Removed = true
	delete(p.items, name)
	return nil
}

func (p *ObjectPool) IsRemoved(name string) bool {
	p.mu.RLock()
	item, exists := p.items[name]
	p.mu.RUnlock()

	if !exists {
		return true
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	return item.Removed
}

func (p *ObjectPool) CloseAndRemove(name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	item, exists := p.items[name]
	if !exists {
		return nil // Already removed, no action needed
	}

	item.mu.Lock()
	defer item.mu.Unlock()

	if item.UsageCount > 0 {
		return errors.New("object store is currently in use")
	}

	// Call Close on the store before removing it from the pool
	if c, ok := item.Store.(interface{ Close() }); ok {
		c.Close()
	}

	item.Removed = true
	delete(p.items, name)
	return nil
}
func (p *ObjectPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, item := range p.items {
		item.mu.Lock()
		if c, ok := item.Store.(interface{ Close() }); ok {
			c.Close()
		}
		item.Removed = true
		item.mu.Unlock()
	}
	p.items = make(map[string]*ObjectStoreItem)
}

func (p *ObjectPool) GetAll() []common.ObjectStore {
	p.mu.RLock()
	defer p.mu.RUnlock()
	stores := make([]common.ObjectStore, 0, len(p.items))
	for _, item := range p.items {
		stores = append(stores, item.Store)
	}
	return stores
}
