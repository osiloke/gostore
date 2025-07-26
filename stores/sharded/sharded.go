package sharded

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/pool"
	"github.com/osiloke/gostore/stores/memory"
)

// ShardLocatorFunc defines the function signature for determining which shard to use.
type ShardLocatorFunc func(collection, key string, data ...interface{}) (string, error)

// ShardCreatorFunc defines the function signature for creating a new, context-aware store instance for a shard.
type ShardCreatorFunc func(storeType, shardName, collection, key string, data ...interface{}) (common.ObjectStore, error)

func DefaultShardLocator(numShards int) ShardLocatorFunc {
	return func(collection, key string, data ...interface{}) (string, error) {
		hash := sha1.New()
		hash.Write([]byte(key))
		hashBytes := hash.Sum(nil)
		hashInt := binary.BigEndian.Uint32(hashBytes[:4])
		shardIndex := int(hashInt) % numShards
		return fmt.Sprintf("shard-%d", shardIndex), nil
	}
}

func DefaultShardCreator() ShardCreatorFunc {
	return func(storeType, shardName, collection, key string, data ...interface{}) (common.ObjectStore, error) {
		return memory.NewMemoryStore(), nil
	}
}

// GenericShardedStore is a gostore implementation that distributes data across multiple shards.
type GenericShardedStore struct {
	pool      *pool.ObjectPool
	storeType string
	locate    ShardLocatorFunc
	create    ShardCreatorFunc
	infoStore common.ObjectStore
}

// NewGenericShardedStore creates a new generic sharded store.
func NewGenericShardedStore(poolSize int, storeType string, locator ShardLocatorFunc, creator ShardCreatorFunc, infoStore common.ObjectStore) *GenericShardedStore {
	if locator == nil {
		locator = DefaultShardLocator(poolSize)
	}
	if creator == nil {
		creator = DefaultShardCreator()
	}
	s := &GenericShardedStore{
		pool:      pool.NewObjectPool(poolSize),
		storeType: storeType,
		locate:    locator,
		create:    creator,
		infoStore: infoStore,
	}
	s.infoStore.CreateDatabase()
	s.infoStore.CreateTable("collections", nil)
	return s
}

func (s *GenericShardedStore) getShardsForCollection(collection string) ([]string, error) {
	var data map[string]interface{}
	err := s.infoStore.Get(collection, "collections", &data)
	if err != nil {
		return []string{}, nil
	}
	shards, ok := data["shards"].([]string)
	if !ok {
		return []string{}, nil
	}
	return shards, nil
}

func (s *GenericShardedStore) addShardToCollection(collection, shardName string) error {
	shards, err := s.getShardsForCollection(collection)
	if err != nil {
		return err
	}

	for _, s := range shards {
		if s == shardName {
			return nil
		}
	}

	shards = append(shards, shardName)
	_, err = s.infoStore.Save(collection, "collections", map[string]interface{}{"shards": shards})
	return err
}

// getStoreFor is a helper to get the correct shard store from the pool
func (s *GenericShardedStore) getStoreFor(collection, key string, data ...interface{}) (common.ObjectStore, string, error) {
	shardName, err := s.locate(collection, key, data...)
	if err != nil {
		return nil, "", fmt.Errorf("shard locator failed: %w", err)
	}

	// The creator function now captures the full context of the request.
	store, err := s.pool.GetOrCreate(shardName, func() (common.ObjectStore, error) {
		log.Printf("Creator called for shard: '%s' with collection: '%s' key: '%s'\n", shardName, collection, key)
		return s.create(s.storeType, shardName, collection, key, data...)
	})

	if err == nil {
		err = s.addShardToCollection(collection, shardName)
	}

	if err != nil {
		return nil, "", fmt.Errorf("could not get or create store for shard %s: %w", shardName, err)
	}
	return store, shardName, nil
}

// Save saves data to the appropriate shard.
func (s *GenericShardedStore) Save(key, store string, src interface{}) (string, error) {
	shardStore, shardName, err := s.getStoreFor(store, key, src)
	if err != nil {
		return "", err
	}
	defer s.pool.Release(shardName)
	return shardStore.Save(key, store, src)
}

// Get retrieves data from the appropriate shard.
func (s *GenericShardedStore) Get(key string, store string, dst interface{}) error {
	shardStore, shardName, err := s.getStoreFor(store, key)
	if err != nil {
		return err
	}
	defer s.pool.Release(shardName)
	return shardStore.Get(key, store, dst)
}

// Delete removes data from the appropriate shard.
func (s *GenericShardedStore) Delete(key string, store string) error {
	shardStore, shardName, err := s.getStoreFor(store, key)
	if err != nil {
		return err
	}
	defer s.pool.Release(shardName)
	return shardStore.Delete(key, store)
}

// Update updates data in the appropriate shard.
func (s *GenericShardedStore) Update(key string, store string, src interface{}) error {
	shardStore, shardName, err := s.getStoreFor(store, key)
	if err != nil {
		return err
	}
	defer s.pool.Release(shardName)
	return shardStore.Update(key, store, src)
}

// Replace replaces data in the appropriate shard.
func (s *GenericShardedStore) Replace(key string, store string, src interface{}) error {
	shardStore, shardName, err := s.getStoreFor(store, key, src)
	if err != nil {
		return err
	}
	defer s.pool.Release(shardName)
	return shardStore.Replace(key, store, src)
}

func (s *GenericShardedStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) AllCursor(store string) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) Since(id string, count int, skip int, store string) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	return 0, common.ErrNotImplemented
}

func (s *GenericShardedStore) getStoresForCollection(collection string) ([]common.ObjectStore, error) {
	shardNames, err := s.getShardsForCollection(collection)
	if err != nil {
		return nil, err
	}

	stores := make([]common.ObjectStore, 0, len(shardNames))
	for _, shardName := range shardNames {
		store, err := s.pool.Get(shardName)
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	return stores, nil
}

func (s *GenericShardedStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	stores, err := s.getStoresForCollection(store)
	if err != nil {
		return nil, err
	}
	results := make(chan common.ObjectRows)
	var wg sync.WaitGroup
	for _, shardStore := range stores {
		wg.Add(1)
		go func(shardStore common.ObjectStore) {
			defer wg.Done()
			rows, err := shardStore.FilterGetAll(filter, count, skip, store, opts)
			if err != nil {
				log.Printf("Error getting rows from shard: %v", err)
				return
			}
			results <- rows
		}(shardStore)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return &ShardedObjectRows{results: results}, nil
}

func (s *GenericShardedStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	stores, err := s.getStoresForCollection(store)
	if err != nil {
		return nil, nil, err
	}
	results := make(chan common.ObjectRows)
	aggResults := make(chan common.AggregateResult)
	var wg sync.WaitGroup
	for _, shardStore := range stores {
		wg.Add(1)
		go func(shardStore common.ObjectStore) {
			defer wg.Done()
			rows, agg, err := shardStore.Query(filter, aggregates, count, skip, store, opts)
			if err != nil {
				log.Printf("Error getting rows from shard: %v", err)
				return
			}
			results <- rows
			aggResults <- agg
		}(shardStore)
	}

	go func() {
		wg.Wait()
		close(results)
		close(aggResults)
	}()

	mergedAgg := make(common.AggregateResult)
	for agg := range aggResults {
		for k, v := range agg {
			mergedAgg[k] = v
		}
	}

	return &ShardedObjectRows{results: results}, mergedAgg, nil
}

func (s *GenericShardedStore) FilterDelete(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	return 0, common.ErrNotImplemented
}

func (s *GenericShardedStore) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	return nil, common.ErrNotImplemented
}

func (s *GenericShardedStore) CreateDatabase() error {
	// This is a tricky one. We'll just grab any store and create the database.
	// This assumes that all shards are of the same type.
	store, _, err := s.getStoreFor("", "")
	if err != nil {
		return err
	}
	return store.CreateDatabase()
}

func (s *GenericShardedStore) CreateTable(table string, sample interface{}) error {
	// This is a tricky one. We'll just grab any store and create the table.
	// This assumes that all shards are of the same type.
	store, _, err := s.getStoreFor(table, "")
	if err != nil {
		return err
	}
	return store.CreateTable(table, sample)
}

func (s *GenericShardedStore) GetStore() interface{} {
	// This is a tricky one. We'll just grab any store and return it.
	// This assumes that all shards are of the same type.
	store, _, err := s.getStoreFor("", "")
	if err != nil {
		return nil
	}
	return store.GetStore()
}

func (s *GenericShardedStore) Stats(store string) (map[string]interface{}, error) {
	// This is a tricky one. We'll just grab any store and return its stats.
	// This assumes that all shards are of the same type.
	shardStore, _, err := s.getStoreFor(store, "")
	if err != nil {
		return nil, err
	}
	return shardStore.Stats(store)
}

func (s *GenericShardedStore) GetByField(name, val, store string, dst interface{}) error {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return common.ErrNotImplemented
}

func (s *GenericShardedStore) Close() {
	// This will close all the stores in the pool
	s.pool.CloseAll()
	s.infoStore.Close()
}

func (s *GenericShardedStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, common.ErrNotImplemented
}

type ShardedObjectRows struct {
	results chan common.ObjectRows
	current common.ObjectRows
	lastErr error
}

func (r *ShardedObjectRows) Next(dst interface{}) (bool, error) {
	if r.current == nil {
		var ok bool
		r.current, ok = <-r.results
		if !ok {
			return false, nil
		}
	}
	ok, err := r.current.Next(dst)
	if err != nil {
		r.lastErr = err
		return false, err
	}
	if !ok {
		r.current = nil
		return r.Next(dst)
	}
	return true, nil
}

func (r *ShardedObjectRows) NextRaw() ([]byte, bool) {
	if r.current == nil {
		var ok bool
		r.current, ok = <-r.results
		if !ok {
			return nil, false
		}
	}
	raw, ok := r.current.NextRaw()
	if !ok {
		r.current = nil
		return r.NextRaw()
	}
	return raw, true
}

func (r *ShardedObjectRows) Close() {
	// Drain the channel
	for range r.results {
	}
}

func (r *ShardedObjectRows) LastError() error {
	return r.lastErr
}

func (s *GenericShardedStore) GetCollectionInfo(collectionName string) ([]string, error) {
	return s.getShardsForCollection(collectionName)
}

func (s *GenericShardedStore) GetShardInfo(shardName string) (map[string]interface{}, error) {
	item, err := s.pool.GetItem(shardName)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"lastAccess":  item.LastAccess,
		"accessCount": item.AccessCount,
		"usageCount":  item.UsageCount,
		"removed":     item.Removed,
	}, nil
}

func (s *GenericShardedStore) GetAllShardInfo() (map[string]interface{}, error) {
	stores := s.pool.GetAll()
	info := make(map[string]interface{})
	for _, store := range stores {
		// This is a bit of a hack, as we don't have a good way to get the shard name from the store.
		// We'll just use the store's string representation as the key.
		info[fmt.Sprintf("%v", store)] = "connected"
	}
	return info, nil
}
