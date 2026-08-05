package memory

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"
	"sync"

	. "github.com/osiloke/gostore/common"
	common "github.com/osiloke/gostore/common"
)

func NewMemoryStore(opts ...func(s *MemoryStore)) *MemoryStore {
	s := &MemoryStore{stores: make(map[string]map[string]interface{})}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
		if i >= skip {
			rows = append(rows, v)
		}
		i++
	}
	if len(rows) > count {
		rows = rows[:count]
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

	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			if i >= skip {
				rows = append(rows, v)
			}
			i++
		}
	}
	if len(rows) > count {
		rows = rows[:count]
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// Since retrieves all documents, as the in-memory store has no ordering.
func (s *MemoryStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if i >= skip {
			rows = append(rows, v)
		}
		i++
	}
	if len(rows) > count {
		rows = rows[:count]
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// Before retrieves all documents, as the in-memory store has no ordering.
func (s *MemoryStore) Before(id string, count int, skip int, store string) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if i >= skip {
			rows = append(rows, v)
		}
		i++
	}
	if len(rows) > count {
		rows = rows[:count]
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// FilterSince retrieves all documents, as the in-memory store has no ordering.
func (s *MemoryStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return s.Since(id, count, skip, store)
}

// FilterBefore retrieves all documents, as the in-memory store has no ordering.
func (s *MemoryStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return s.Before(id, count, skip, store)
}

// FilterBeforeCount counts all documents, as the in-memory store has no ordering.
func (s *MemoryStore) FilterBeforeCount(id string, filter map[string]interface{}, size int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	rows, err := s.FilterBefore(id, filter, size, skip, store, opts)
	if err != nil {
		return 0, err
	}
	return int64(len(rows.(*TransactionRows).entries)), nil
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
	return common.ErrNotFound
}

// Save inserts a new document into the store.
func (s *MemoryStore) Save(key, store string, src interface{}) (string, error) {
	s.Lock()
	defer s.Unlock()
	return s.save(key, store, src)
}

func (s *MemoryStore) save(key, store string, src interface{}) (string, error) {
	if _, ok := s.stores[store]; !ok {
		s.stores[store] = make(map[string]interface{})
	}

	// Serialize the data into a map[string]interface{}
	data, err := marshalData(src)
	if err != nil {
		return "", err
	}

	if key == "" {
		if idVal, ok := data[common.IDField]; ok && idVal != "" {
			key = fmt.Sprintf("%v", idVal)
		} else {
			key = common.NewObjectId().String()
		}
		data[common.IDField] = key
	}

	s.stores[store][key] = data
	return key, nil
}

// SaveAll inserts multiple documents into the store.
func (s *MemoryStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	s.Lock()
	defer s.Unlock()

	for _, data := range src {
		key, err := s.save("", store, data)
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
	return s.update(key, store, src)
}

func (s *MemoryStore) update(key string, store string, src interface{}) error {
	if _, ok := s.stores[store][key]; !ok {
		return common.ErrNotFound
	}

	updateData, err := marshalData(src)
	if err != nil {
		return err
	}

	if existingData, ok := s.stores[store][key].(map[string]interface{}); ok {
		for k, v := range updateData {
			if strings.EqualFold(k, "id") {
				continue
			}
			found := false
			for ek := range existingData {
				if strings.EqualFold(ek, k) {
					existingData[ek] = v
					found = true
					break
				}
			}
			if !found {
				existingData[strings.ToLower(k)] = v
			}
		}
		s.stores[store][key] = existingData
	} else {
		s.stores[store][key] = updateData
	}
	return nil
}

// Replace replaces an existing document with a new one.
func (s *MemoryStore) Replace(key string, store string, src interface{}) error {
	s.Lock()
	defer s.Unlock()
	return s.replace(key, store, src)
}

func (s *MemoryStore) replace(key string, store string, src interface{}) error {
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

	for key, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			err := s.update(key, store, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// FilterReplace replaces multiple documents matching a filter.
func (s *MemoryStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	for key, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			err := s.replace(key, store, src)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func matchesFilter(item interface{}, filter map[string]interface{}) bool {
	if len(filter) == 0 {
		return true
	}

	itemMap, ok := item.(map[string]interface{})
	if !ok {
		dataBytes, err := json.Marshal(item)
		if err != nil {
			return false
		}
		if err := json.Unmarshal(dataBytes, &itemMap); err != nil {
			return false
		}
	}

	var query map[string]interface{}
	if q, ok := filter["q"].(map[string]interface{}); ok {
		query = q
	} else {
		query = filter
	}

	if len(query) == 0 {
		return true
	}

	for k, v := range query {
		cleanKey := k
		if strings.HasPrefix(k, "data.") {
			cleanKey = strings.TrimPrefix(k, "data.")
		}

		val, exists := itemMap[cleanKey]
		if !exists {
			lowerClean := strings.ToLower(cleanKey)
			val, exists = itemMap[lowerClean]
			if !exists {
				val, exists = getValueAtPath(itemMap, cleanKey)
				if !exists {
					val, exists = itemMap[k]
					if !exists {
						lowerK := strings.ToLower(k)
						val, exists = itemMap[lowerK]
						if !exists {
							val, exists = getValueAtPath(itemMap, k)
							if !exists {
								return false
							}
						}
					}
				}
			}
		}

		if vSlice, ok := v.([]string); ok {
			matchedAny := false
			for _, item := range vSlice {
				if compareValues(val, item) {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false
			}
		} else if vSliceInterface, ok := v.([]interface{}); ok {
			matchedAny := false
			for _, item := range vSliceInterface {
				if compareValues(val, item) {
					matchedAny = true
					break
				}
			}
			if !matchedAny {
				return false
			}
		} else {
			if !compareValues(val, v) {
				return false
			}
		}
	}
	return true
}

func getValueAtPath(data map[string]interface{}, path string) (interface{}, bool) {
	parts := strings.Split(path, ".")
	var current interface{} = data
	for _, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			var exists bool
			current, exists = m[part]
			if !exists {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return current, true
}

func compareValues(actual, expected interface{}) bool {
	if expMap, ok := expected.(map[string]interface{}); ok {
		for op, val := range expMap {
			switch op {
			case "$gt":
				return compareNumeric(actual, val) > 0
			case "$gte":
				return compareNumeric(actual, val) >= 0
			case "$lt":
				return compareNumeric(actual, val) < 0
			case "$lte":
				return compareNumeric(actual, val) <= 0
			case "$eq":
				return reflect.DeepEqual(actual, val) || fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", val)
			case "$ne":
				return !reflect.DeepEqual(actual, val) && fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", val)
			}
		}
		return false
	}

	if expectedStr, ok := expected.(string); ok {
		return evaluateQueryStringSyntax(actual, expectedStr)
	}

	return reflect.DeepEqual(actual, expected) || fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

func evaluateQueryStringSyntax(actual interface{}, queryVal string) bool {
	if len(queryVal) == 0 {
		return true
	}

	negate := false
	valRune := []rune(queryVal)
	switch valRune[0] {
	case '!':
		negate = true
		valRune = valRune[1:]
	case '?', '+':
		valRune = valRune[1:]
	}

	if len(valRune) == 0 {
		return !negate
	}

	first := valRune[0]
	var matched bool

	switch first {
	case '^':
		pattern := string(valRune[1:])
		patternRegex := "^" + pattern
		matched, _ = regexp.MatchString(patternRegex, fmt.Sprintf("%v", actual))
	case '<':
		var compVal string
		if len(valRune) > 1 && valRune[1] == ':' {
			compVal = string(valRune[3:])
		} else {
			compVal = string(valRune[1:])
		}
		matched = compareNumeric(actual, compVal) <= 0
	case '>':
		var compVal string
		if len(valRune) > 1 && valRune[1] == ':' {
			compVal = string(valRune[3:])
		} else {
			compVal = string(valRune[1:])
		}
		matched = compareNumeric(actual, compVal) >= 0
	default:
		expectedStr := string(valRune)
		actualStr := fmt.Sprintf("%v", actual)
		if strings.Contains(expectedStr, "*") {
			pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(expectedStr), "\\*", ".*") + "$"
			matched, _ = regexp.MatchString(pattern, actualStr)
		} else {
			matched = (actualStr == expectedStr)
		}
	}

	if negate {
		return !matched
	}
	return matched
}

func compareNumeric(actual, expected interface{}) int {
	actFloat, ok1 := toFloat64(actual)
	expFloat, ok2 := toFloat64(expected)
	if !ok1 || !ok2 {
		actStr := fmt.Sprintf("%v", actual)
		expStr := fmt.Sprintf("%v", expected)
		if actStr < expStr {
			return -1
		} else if actStr > expStr {
			return 1
		}
		return 0
	}
	if actFloat < expFloat {
		return -1
	} else if actFloat > expFloat {
		return 1
	}
	return 0
}

func toFloat64(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		var f float64
		if _, err := fmt.Sscanf(v, "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}

// FilterGet retrieves a document matching a filter.
func (s *MemoryStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	logger.Info("FilterGet", "filter", filter, "store", store)

	for _, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			return unmarshalData(v, dst)
		}
	}
	return common.ErrNotFound
}

// FilterGetAll retrieves multiple documents matching a filter.
func (s *MemoryStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			if i >= skip {
				rows = append(rows, v)
			}
			i++
		}
	}
	if len(rows) > count {
		rows = rows[:count]
	}

	return &TransactionRows{
		entries: rows,
	}, nil
}

// Query retrieves documents matching a filter and calculates aggregations.
func (s *MemoryStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, AggregateResult, error) {
	s.Lock()
	defer s.Unlock()

	rows := make([]interface{}, 0)
	i := 0
	for _, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			if i >= skip {
				rows = append(rows, v)
			}
			i++
		}
	}
	if len(rows) > count {
		rows = rows[:count]
	}
	logger.Info("query", "filter", filter, "rows", len(rows))
	// Currently, this method does not perform aggregation.
	return &TransactionRows{
		entries: rows,
	}, AggregateResult{}, nil
}

// FilterDelete removes multiple documents matching a filter.
func (s *MemoryStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.stores[store]; !ok {
		return common.ErrNotFound
	}

	for key, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			delete(s.stores[store], key)
		}
	}
	return nil
}

// FilterCount counts documents matching a filter.
func (s *MemoryStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	s.Lock()
	defer s.Unlock()

	count := int64(0)
	for _, v := range s.stores[store] {
		if matchesFilter(v, filter) {
			count++
		}
	}
	return count, nil
}

// GetByField retrieves the first document that has a specific field.
func (s *MemoryStore) GetByField(name, val, store string, dst interface{}) error {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.stores[store]; !ok {
		return common.ErrNotFound
	}

	for _, v := range s.stores[store] {
		if data, ok := v.(map[string]interface{}); ok {
			if _, found := getFieldCaseInsensitive(data, name); found {
				return unmarshalData(v, dst)
			}
		}
	}

	return common.ErrNotFound
}

// GetByFieldsByField retrieves all documents that have a specific field, selecting specific fields.
func (s *MemoryStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	s.Lock()
	defer s.Unlock()

	if _, ok := s.stores[store]; !ok {
		return nil
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return fmt.Errorf("dst must be a pointer")
	}

	sliceType := dstVal.Elem().Type()
	isSlice := sliceType.Kind() == reflect.Slice

	var matchedMaps []map[string]interface{}
	for _, v := range s.stores[store] {
		if data, ok := v.(map[string]interface{}); ok {
			if _, found := getFieldCaseInsensitive(data, name); found {
				if len(fields) > 0 {
					filtered := make(map[string]interface{})
					for _, field := range fields {
						if fv, ok := getFieldCaseInsensitive(data, field); ok {
							filtered[field] = fv
						}
					}
					matchedMaps = append(matchedMaps, filtered)
				} else {
					matchedMaps = append(matchedMaps, data)
				}
			}
		}
	}

	if isSlice {
		sliceVal := reflect.MakeSlice(sliceType, len(matchedMaps), len(matchedMaps))
		for i, m := range matchedMaps {
			elemPtr := reflect.New(sliceType.Elem())
			b, err := json.Marshal(m)
			if err != nil {
				return err
			}
			if err := json.Unmarshal(b, elemPtr.Interface()); err != nil {
				return err
			}
			sliceVal.Index(i).Set(elemPtr.Elem())
		}
		dstVal.Elem().Set(sliceVal)
	} else {
		if len(matchedMaps) == 0 {
			return common.ErrNotFound
		}
		b, err := json.Marshal(matchedMaps[0])
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, dst); err != nil {
			return err
		}
	}

	return nil
}

func getFieldCaseInsensitive(m map[string]interface{}, name string) (interface{}, bool) {
	for k, v := range m {
		if strings.EqualFold(k, name) {
			return v, true
		}
	}
	return nil, false
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
		if _, ok := s.stores[store][key]; !ok {
			return common.ErrNotFound
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
		key, err := s.save("", store, datum)
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
	log.Printf("Marshalling data of type %T: %+v", src, src)
	// This is a basic implementation.
	// You might need to use a more robust serialization library
	// like JSON or YAML depending on your data structure.
	// This example assumes src is a struct.

	data, ok := src.(map[string]interface{})
	if ok {
		return data, nil
	}

	// If not a map, try to marshal it to JSON and then unmarshal it to a map
	b, err := json.Marshal(src)
	if err != nil {
		log.Printf("Error marshalling data: %v", err)
		return nil, fmt.Errorf("cannot marshal data: %v", src)
	}
	log.Printf("Marshalled data: %s", b)
	var dst map[string]interface{}
	err = json.Unmarshal(b, &dst)
	if err != nil {
		log.Printf("Error unmarshalling data: %v", err)
		return nil, fmt.Errorf("cannot unmarshal data: %v", src)
	}

	return dst, nil
}

// helper function to unmarshal data from map[string]interface{} into a struct
func unmarshalData(data interface{}, dst interface{}) error {
	// This is a basic implementation.
	// You might need to use a more robust deserialization library
	// depending on your data structure.
	// This example assumes dst is a pointer to a struct.

	// Example assuming data is a map[string]interface{} and dst is a pointer to a struct.
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
