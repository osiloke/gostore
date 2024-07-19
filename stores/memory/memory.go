package memory

import (
	"fmt"
	"sync"

	"dario.cat/mergo"
	. "github.com/osiloke/gostore/common"
	common "github.com/osiloke/gostore/common"
)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{stores: make(map[string]map[string]interface{})}
}

// MemoryStore implements the ObjectStore interface using a simple in-memory map.
type MemoryStore struct {
	sync.Mutex
	stores map[string]map[string]interface{}
}

// CreateDatabase creates a new database (store) in memory.
func (s *MemoryStore) CreateDatabase() error {
	s.Lock()
	defer s.Unlock()

	if s.stores == nil {
		s.stores = make(map[string]map[string]interface{})
	}
	return nil
}

// CreateTable creates a new table (collection) within a database.
func (s *MemoryStore) CreateTable(table string, sample interface{}) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.stores[table]; ok {
		return fmt.Errorf("table %s already exists", table)
	}

	s.stores[table] = make(map[string]interface{})
	return nil
}

// GetStore returns the underlying store (map) for a specific database.
func (s *MemoryStore) GetStore() interface{} {
	return s.stores
}

// Stats returns basic statistics about the store.
func (s *MemoryStore) Stats(store string) (map[string]interface{}, error) {
	s.Lock()
	defer s.Unlock()

	stats := map[string]interface{}{
		"count": len(s.stores[store]),
	}
	return stats, nil
}

// All retrieves all documents in a store.
func (s *MemoryStore) All(count int, skip int, store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()
	logger.Debug("All", "count", count, "skip", skip, "store", store)
	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if i >= skip && i < skip+count {
			rows = append(rows, v)
		}
		i++
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// AllCursor retrieves all documents in a store using a cursor.
func (s *MemoryStore) AllCursor(store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	for _, v := range s.stores[store] {
		rows = append(rows, v)
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// AllWithinRange retrieves all documents within a specified range.
func (s *MemoryStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply returns all documents
	// within the specified range.
	return s.All(count, skip, store)
}

// Since retrieves all documents after a specific ID.
func (s *MemoryStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	found := false
	i := 0
	for _, v := range s.stores[store] {
		if found {
			if i >= skip && i < skip+count {
				rows = append(rows, v)
			}
			i++
		} else {
			if _, ok := v.(map[string]interface{})["id"]; ok {
				if v.(map[string]interface{})["id"] == id {
					found = true
				}
			}
		}
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// Before retrieves all documents before a specific ID.
func (s *MemoryStore) Before(id string, count int, skip int, store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	found := false
	i := 0
	for _, v := range s.stores[store] {
		if !found {
			if _, ok := v.(map[string]interface{})["id"]; ok {
				if v.(map[string]interface{})["id"] == id {
					found = true
				} else {
					if i >= skip && i < skip+count {
						rows = append(rows, v)
					}
					i++
				}
			}
		}
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// FilterSince retrieves all documents after a specific ID and matching a filter.
func (s *MemoryStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply returns all documents
	// after the specified ID.
	return s.Since(id, count, skip, store)
}

// FilterBefore retrieves all documents before a specific ID and matching a filter.
func (s *MemoryStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply returns all documents
	// before the specified ID.
	return s.Before(id, count, skip, store)
}

// FilterBeforeCount counts all documents before a specific ID and matching a filter.
func (s *MemoryStore) FilterBeforeCount(id string, filter map[string]interface{}, size int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply counts all documents
	// before the specified ID.
	found := false
	count := 0
	for _, v := range s.stores[store] {
		if !found {
			if _, ok := v.(map[string]interface{})["id"]; ok {
				if v.(map[string]interface{})["id"] == id {
					found = true
				} else {
					count++
				}
			}
		}
	}

	return int64(count), nil
}

// Get retrieves a document by its key.
func (s *MemoryStore) Get(key string, store string, dst interface{}) error {
	s.Lock()
	defer s.Unlock()

	if v, ok := s.stores[store][key]; ok {
		// Unmarshal the data into the destination struct.
		// This assumes dst is a pointer to a struct.
		// You might need to handle different data types here.
		return unmarshalData(v, dst)
	}
	return fmt.Errorf("document with key %s not found", key)
}

// Save inserts a new document into the store.
func (s *MemoryStore) Save(key, store string, src interface{}) (string, error) {
	s.Lock()
	defer s.Unlock()

	// Serialize the data into a map[string]interface{}
	data, err := marshalData(src)
	if err != nil {
		return "", err
	}
	s.stores[store][key] = data
	return key, nil
}

// SaveAll inserts multiple documents into the store.
func (s *MemoryStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	s.Lock()
	defer s.Unlock()

	for _, data := range src {
		key, err := s.Save("", store, data)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Update updates an existing document.
func (s *MemoryStore) Update(key string, store string, src interface{}) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.stores[store][key]; !ok {
		return fmt.Errorf("document with key %s not found", key)
	}

	// Serialize the data into a map[string]interface{}
	data, err := marshalData(src)
	if err != nil {
		return err
	}

	s.stores[store][key] = data
	return nil
}

// Replace replaces an existing document with a new one.
func (s *MemoryStore) Replace(key string, store string, src interface{}) error {
	s.Lock()
	defer s.Unlock()

	// Serialize the data into a map[string]interface{}
	data, err := marshalData(src)
	if err != nil {
		return err
	}

	s.stores[store][key] = data
	return nil
}

// Delete removes a document from the store.
func (s *MemoryStore) Delete(key string, store string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.stores[store], key)
	return nil
}

// FilterUpdate updates multiple documents matching a filter.
func (s *MemoryStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply updates all documents.
	for key := range s.stores[store] {
		err := s.Update(key, store, src)
		if err != nil {
			return err
		}
	}
	return nil
}

// FilterReplace replaces multiple documents matching a filter.
func (s *MemoryStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply replaces all documents.
	for key := range s.stores[store] {
		err := s.Replace(key, store, src)
		if err != nil {
			return err
		}
	}
	return nil
}

// FilterGet retrieves a document matching a filter.
func (s *MemoryStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	logger.Info("FilterGet", "filter", filter, "store", store)

	// Currently, this method does not perform filtering. It simply retrieves the first document.
	for _, v := range s.stores[store] {
		return unmarshalData(v, dst)
	}
	return common.ErrNotFound
}

// FilterGetAll retrieves multiple documents matching a filter.
func (s *MemoryStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply returns all documents
	// within the specified range.
	return s.All(count, skip, store)
}

// Query retrieves documents matching a filter and calculates aggregations.
func (s *MemoryStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, AggregateResult, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	for _, v := range s.stores[store] {
		rows = append(rows, v)
	}
	logger.Info("query", "filter", filter, "rows", len(rows))
	// Currently, this method does not perform filtering or aggregation.
	return &TransactionRows{
		entries: rows,
	}, AggregateResult{}, nil
}

// FilterDelete removes multiple documents matching a filter.
func (s *MemoryStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply deletes all documents.
	for key := range s.stores[store] {
		delete(s.stores[store], key)
	}
	return nil
}

// FilterCount counts documents matching a filter.
func (s *MemoryStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply returns the count of all documents.
	return int64(len(s.stores[store])), nil
}

// GetByField retrieves a document by a specific field and value.
func (s *MemoryStore) GetByField(name, val, store string, dst interface{}) error {
	s.Lock()
	defer s.Unlock()

	for _, v := range s.stores[store] {
		if data, ok := v.(map[string]interface{})[name]; ok {
			if data == val {
				return unmarshalData(v, dst)
			}
		}
	}

	return common.ErrNotFound
}

// GetByFieldsByField retrieves a document by a specific field and value, selecting specific fields.
func (s *MemoryStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	s.Lock()
	defer s.Unlock()

	for _, v := range s.stores[store] {
		if data, ok := v.(map[string]interface{})[name]; ok {
			if data == val {
				filteredData := make(map[string]interface{})
				for _, field := range fields {
					if fieldValue, ok := v.(map[string]interface{})[field]; ok {
						filteredData[field] = fieldValue
					}
				}
				return unmarshalData(filteredData, dst)
			}
		}
	}

	return common.ErrNotFound
}

// BatchDelete removes multiple documents by their IDs.
func (s *MemoryStore) BatchDelete(ids []interface{}, store string, opts ObjectStoreOptions) (err error) {
	s.Lock()
	defer s.Unlock()

	for _, id := range ids {
		key, ok := id.(string)
		if !ok {
			return fmt.Errorf("invalid ID type: %T", id)
		}
		delete(s.stores[store], key)
	}
	return nil
}

// BatchUpdate updates multiple documents by their IDs.
func (s *MemoryStore) BatchUpdate(ids []interface{}, data []interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	if len(ids) != len(data) {
		return fmt.Errorf("number of IDs and data must match")
	}

	for i, id := range ids {
		key, ok := id.(string)
		if !ok {
			return fmt.Errorf("invalid ID type: %T", id)
		}

		// Serialize the data into a map[string]interface{}
		data, err := marshalData(data[i])
		if err != nil {
			return err
		}
		s.stores[store][key] = data
	}

	return nil
}

// BatchFilterDelete removes multiple documents matching a list of filters.
func (s *MemoryStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	// Currently, this method does not perform filtering. It simply deletes all documents.
	for key := range s.stores[store] {
		delete(s.stores[store], key)
	}
	return nil
}

// BatchInsert inserts multiple documents into the store.
func (s *MemoryStore) BatchInsert(data []interface{}, store string, opts ObjectStoreOptions) (keys []string, err error) {
	s.Lock()
	defer s.Unlock()

	for _, datum := range data {
		key, err := s.Save("", store, datum)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// Close does nothing, as the in-memory store does not require any cleanup.
func (s *MemoryStore) Close() {
	// No cleanup needed for in-memory store.
}

// helper function to serialize data into map[string]interface{}
func marshalData(src interface{}) (map[string]interface{}, error) {
	// This is a basic implementation.
	// You might need to use a more robust serialization library
	// like JSON or YAML depending on your data structure.
	// This example assumes src is a struct.

	data, ok := src.(map[string]interface{})
	if ok {
		return data, nil
	}

	return nil, fmt.Errorf("cannot marshal data of type %T", src)
}

// helper function to unmarshal data from map[string]interface{} into a struct
func unmarshalData(data interface{}, dst interface{}) error {
	// This is a basic implementation.
	// You might need to use a more robust deserialization library
	// depending on your data structure.
	// This example assumes dst is a pointer to a struct.

	// Example assuming data is a map[string]interface{} and dst is a pointer to a struct.
	if data, ok := data.(map[string]interface{}); ok {
		// You need to implement the unmarshaling logic for your specific data structure.
		// This is a placeholder.
		// You might use reflection or other techniques.

		// Example:
		// if dst, ok := dst.(*YourStruct); ok {
		//    dst.Field1 = data["Field1"]
		//    dst.Field2 = data["Field2"]
		// }
		// destination := dst.(map[string]interface{})
		// return mergo.Map(&dst, data)
		switch v := dst.(type) {
		case *interface{}:
			dst = &data
			return nil
		case map[string]interface{}:
			return mergo.Map(&v, data)
		}
	}

	return fmt.Errorf("cannot unmarshal data of type %T", data)
}
