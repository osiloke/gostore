package pool

import (
	"errors"
	"sync"
	"time"

	common "github.com/osiloke/gostore/common"
)

var ErrAlreadyExists = errors.New("object store with this name already exists")
var ErrNoItemsToUse = errors.New("all items are in use, cannot remove any")

type Closeable interface {
	Close() error
}

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
	o.mu.Unlock()
	o.UsageCount--
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

func (p *ObjectPool) GetOrCreate(name string, creator func() common.ObjectStore) (*ObjectStoreItem, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, exists := p.items[name]

	if !exists {
		store := creator()
		return p.items[name], p.add(name, store)
	}

	item.LastAccess = time.Now()
	item.AccessCount++
	item.UsageCount++

	return item, nil
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
		Store:       store,
		LastAccess:  time.Now(),
		AccessCount: 1,
		UsageCount:  1,
		Removed:     false,
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
		return errors.New("object store not found")
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

	// if closeable, ok := item.Store.(Closeable); ok {
	// 	if err := closeable.Close(); err != nil {
	// 		return err
	// 	}
	// }
	item.Store.Close()

	item.Removed = true
	delete(p.items, name)
	return nil
}
