package gostore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mgutz/logxi/v1"
	"github.com/nanobox-io/golang-scribble"
)

var logger = log.New("gostore.scribble")

// ErrKeyNotValid represents an error when a record key was not generated
var ErrKeyNotValid = errors.New("record key was not generated")

// ErrNotFound represents an error when a record does not exist
var ErrNotFound = errors.New("does not exist")

// ErrNotAllDeleted represents an error when not all rows were deleted
var ErrNotAllDeleted = errors.New("not all rows were deleted")

// ErrDuplicatePk represents an error when a duplicate primary key exists
var ErrDuplicatePk = errors.New("duplicate primary key exists")

// ErrNotImplemented represents an error when a method is not implemented yet
var ErrNotImplemented = errors.New("not implemented yet")

// ErrEOF represents an EOF error
var ErrEOF = errors.New("eof")

// ScribbleStore represents a store backed by Scribble
type ScribbleStore struct {
	db   *scribble.Driver
	path string
}

// NewScribbleStore creates a new ScribbleStore
func NewScribbleStore(path string) *ScribbleStore {
	if db, err := scribble.New(path, nil); err == nil {
		return &ScribbleStore{db, path}
	} else {
		logger.Warn("cannot create scribble database", "err", err)
	}
	return nil
}

// ScribbleRows represents rows from a Scribble query
type ScribbleRows struct {
	rows []string
	i    int
	len  int
}

// LastError returns the last error
func (s ScribbleRows) LastError() error {
	return nil
}

// Next moves to the next row and populates the destination
func (s ScribbleRows) Next(dst interface{}) (bool, error) {
	if s.i >= s.len {
		return false, nil
	}
	if err := json.Unmarshal([]byte(s.rows[s.i]), dst); err != nil {
		return false, err
	}
	s.i++
	return true, nil
}

// NextRaw returns the raw data for the next row
func (s ScribbleRows) NextRaw() ([]byte, bool) {
	if s.i >= s.len {
		return nil, false
	}
	data := []byte(s.rows[s.i])
	s.i++
	return data, true
}

// Close closes the rows
func (s ScribbleRows) Close() {
	s.rows = nil
	s.i = -1
	s.len = -1
}

// Management API

// CreateDatabase creates the database
func (s ScribbleStore) CreateDatabase() error {
	// Scribble creates the database directory when it's initialized
	return nil
}

// CreateTable creates a table
func (s ScribbleStore) CreateTable(table string, sample interface{}) error {
	// Scribble creates collections (tables) on-demand
	return nil
}

// Misc API

// GetStore returns the underlying store
func (s ScribbleStore) GetStore() interface{} {
	return s.db
}

// Stats returns statistics for the store
func (s ScribbleStore) Stats(store string) (map[string]interface{}, error) {
	files, err := filepath.Glob(filepath.Join(s.path, store, "*.json"))
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"count": len(files),
	}, nil
}

// New API

// All returns all records in the store
func (s ScribbleStore) All(count int, skip int, store string) (ObjectRows, error) {
	_rows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Apply skip and count
	start := skip
	end := len(_rows)
	if count > 0 && start+count < end {
		end = start + count
	}
	
	if start >= len(_rows) {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{_rows[start:end], 0, end - start}, nil
}

// AllCursor returns a cursor for all records in the store
func (s ScribbleStore) AllCursor(store string) (ObjectRows, error) {
	return s.All(0, 0, store)
}

// AllWithinRange returns all records within a range
func (s ScribbleStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Filter rows based on the filter
	var filteredRows []string
	for _, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			filteredRows = append(filteredRows, row)
		}
	}
	
	// Apply skip and count
	start := skip
	end := len(filteredRows)
	if count > 0 && start+count < end {
		end = start + count
	}
	
	if start >= len(filteredRows) {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{filteredRows[start:end], 0, end - start}, nil
}

// Since returns all records since a specific ID
func (s ScribbleStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Find the index of the specified ID
	idIndex := -1
	for i, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		if itemID, ok := item["id"].(string); ok && itemID == id {
			idIndex = i
			break
		}
	}
	
	if idIndex == -1 {
		return nil, ErrNotFound
	}
	
	// Get records after the specified ID
	start := idIndex + 1 + skip
	end := len(allRows)
	if count > 0 && start+count < end {
		end = start + count
	}
	
	if start >= len(allRows) {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{allRows[start:end], 0, end - start}, nil
}

// Before returns all records before a specific ID
func (s ScribbleStore) Before(id string, count int, skip int, store string) (ObjectRows, error) {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Find the index of the specified ID
	idIndex := -1
	for i, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		if itemID, ok := item["id"].(string); ok && itemID == id {
			idIndex = i
			break
		}
	}
	
	if idIndex == -1 {
		return nil, ErrNotFound
	}
	
	// Get records before the specified ID
	end := idIndex - skip
	start := 0
	if count > 0 && end-count > start {
		start = end - count
	}
	
	if end <= 0 || start >= end {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{allRows[start:end], 0, end - start}, nil
}

// FilterSince returns filtered records since a specific ID
func (s ScribbleStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Find the index of the specified ID
	idIndex := -1
	for i, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		if itemID, ok := item["id"].(string); ok && itemID == id {
			idIndex = i
			break
		}
	}
	
	if idIndex == -1 {
		return nil, ErrNotFound
	}
	
	// Filter rows after the specified ID
	var filteredRows []string
	for i := idIndex + 1; i < len(allRows); i++ {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(allRows[i]), &item); err != nil {
			continue
		}
		
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			filteredRows = append(filteredRows, allRows[i])
		}
	}
	
	// Apply skip and count
	start := skip
	end := len(filteredRows)
	if count > 0 && start+count < end {
		end = start + count
	}
	
	if start >= len(filteredRows) {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{filteredRows[start:end], 0, end - start}, nil
}

// FilterBefore returns filtered records before a specific ID
func (s ScribbleStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	
	// Find the index of the specified ID
	idIndex := -1
	for i, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		if itemID, ok := item["id"].(string); ok && itemID == id {
			idIndex = i
			break
		}
	}
	
	if idIndex == -1 {
		return nil, ErrNotFound
	}
	
	// Filter rows before the specified ID
	var filteredRows []string
	for i := 0; i < idIndex; i++ {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(allRows[i]), &item); err != nil {
			continue
		}
		
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			filteredRows = append(filteredRows, allRows[i])
		}
	}
	
	// Apply skip and count
	end := len(filteredRows) - skip
	start := 0
	if count > 0 && end-count > start {
		start = end - count
	}
	
	if end <= 0 || start >= end {
		return ScribbleRows{[]string{}, 0, 0}, nil
	}
	
	return ScribbleRows{filteredRows[start:end], 0, end - start}, nil
}

// FilterBeforeCount returns the count of filtered records before a specific ID
func (s ScribbleStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	rows, err := s.FilterBefore(id, filter, 0, 0, store, opts)
	if err != nil {
		return 0, err
	}
	
	scribbleRows, ok := rows.(ScribbleRows)
	if !ok {
		return 0, errors.New("unexpected rows type")
	}
	
	return int64(scribbleRows.len), nil
}

// Get retrieves a record by key
func (s ScribbleStore) Get(key string, store string, dst interface{}) error {
	err := s.db.Read(store, key, dst)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	return nil
}

// Save saves a record
func (s ScribbleStore) Save(key, store string, src interface{}) (string, error) {
	if err := s.db.Write(store, key, src); err != nil {
		return "", err
	}
	return key, nil
}

// SaveAll saves multiple records
func (s ScribbleStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	keys = make([]string, len(src))
	for i, item := range src {
		// Extract the key from the item
		var key string
		val := reflect.ValueOf(item)
		if val.Kind() == reflect.Map {
			idVal := val.MapIndex(reflect.ValueOf("id"))
			if idVal.IsValid() {
				key = fmt.Sprintf("%v", idVal.Interface())
			} else {
				key = strconv.FormatInt(time.Now().UnixNano(), 10)
			}
		} else if val.Kind() == reflect.Ptr && val.Elem().Kind() == reflect.Struct {
			idField := val.Elem().FieldByName("ID")
			if idField.IsValid() {
				key = fmt.Sprintf("%v", idField.Interface())
			} else {
				key = strconv.FormatInt(time.Now().UnixNano(), 10)
			}
		} else {
			key = strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		
		if err := s.db.Write(store, key, item); err != nil {
			return keys, err
		}
		keys[i] = key
	}
	return keys, nil
}

// Update updates a record
func (s ScribbleStore) Update(key string, store string, src interface{}) error {
	var existing map[string]interface{}
	if err := s.db.Read(store, key, &existing); err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	// Convert src to map
	var updates map[string]interface{}
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &updates); err != nil {
		return err
	}
	
	// Apply updates
	for k, v := range updates {
		existing[k] = v
	}
	
	return s.db.Write(store, key, existing)
}

// Replace replaces a record
func (s ScribbleStore) Replace(key string, store string, src interface{}) error {
	// Check if the record exists
	var existing map[string]interface{}
	if err := s.db.Read(store, key, &existing); err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	// Replace the record
	return s.db.Write(store, key, src)
}

// Delete deletes a record
func (s ScribbleStore) Delete(key string, store string) error {
	return s.db.Delete(store, key)
}

// Filter API

// FilterUpdate updates records based on a filter
func (s ScribbleStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	// Convert src to map
	var updates map[string]interface{}
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &updates); err != nil {
		return err
	}
	
	for _, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		// Check if the item matches the filter
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			// Apply updates
			for k, v := range updates {
				item[k] = v
			}
			
			// Save the updated item
			if id, ok := item["id"].(string); ok {
				if err := s.db.Write(store, id, item); err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// FilterReplace replaces records based on a filter
func (s ScribbleStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	// Convert src to map
	var replacement map[string]interface{}
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, &replacement); err != nil {
		return err
	}
	
	for _, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		// Check if the item matches the filter
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			// Preserve the ID
			if id, ok := item["id"].(string); ok {
				replacement["id"] = id
				
				// Replace the item
				if err := s.db.Write(store, id, replacement); err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// FilterGet retrieves a record based on a filter
func (s ScribbleStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	for _, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		// Check if the item matches the filter
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			// Found a match, unmarshal into dst
			return json.Unmarshal([]byte(row), dst)
		}
	}
	
	return ErrNotFound
}

// FilterGetAll retrieves all records based on a filter
func (s ScribbleStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return s.AllWithinRange(filter, count, skip, store, opts)
}

// Query performs a query with aggregates
func (s ScribbleStore) Query(filter, aggregates map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, AggregateResult, error) {
	rows, err := s.FilterGetAll(filter, count, skip, store, opts)
	if err != nil {
		return nil, nil, err
	}
	
	// Simple implementation of aggregates
	aggResult := make(AggregateResult)
	
	// Count aggregate
	if _, ok := aggregates["count"]; ok {
		scribbleRows, ok := rows.(ScribbleRows)
		if ok {
			aggResult["count"] = scribbleRows.len
		}
	}
	
	return rows, aggResult, nil
}

// FilterDelete deletes records based on a filter
func (s ScribbleStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	allRows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return ErrNotFound
		}
		return err
	}
	
	for _, row := range allRows {
		var item map[string]interface{}
		if err := json.Unmarshal([]byte(row), &item); err != nil {
			continue
		}
		
		// Check if the item matches the filter
		match := true
		for k, v := range filter {
			if itemVal, ok := item[k]; !ok || !reflect.DeepEqual(itemVal, v) {
				match = false
				break
			}
		}
		
		if match {
			// Delete the item
			if id, ok := item["id"].(string); ok {
				if err := s.db.Delete(store, id); err != nil {
					return err
				}
			}
		}
	}
	
	return nil
}

// BatchDelete deletes multiple records by ID
func (s ScribbleStore) BatchDelete(ids []interface{}, store string, opts ObjectStoreOptions) (err error) {
	for _, id := range ids {
		idStr := fmt.Sprintf("%v", id)
		if err := s.db.Delete(store, idStr); err != nil {
			return err
		}
	}
	return nil
}

// BatchUpdate updates multiple records by ID
func (s ScribbleStore) BatchUpdate(ids []interface{}, data []interface{}, store string, opts ObjectStoreOptions) error {
	if len(ids) != len(data) {
		return errors.New("ids and data length mismatch")
	}
	
	for i, id := range ids {
		idStr := fmt.Sprintf("%v", id)
		if err := s.Update(idStr, store, data[i]); err != nil {
			return err
		}
	}
	
	return nil
}

// BatchFilterDelete deletes records based on multiple filters
func (s ScribbleStore) BatchFilterDelete(filters []map[string]interface{}, store string, opts ObjectStoreOptions) error {
	for _, filter := range filters {
		if err := s.FilterDelete(filter, store, opts); err != nil {
			return err
		}
	}
	return nil
}

// FilterCount returns the count of records based on a filter
func (s ScribbleStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	rows, err := s.FilterGetAll(filter, 0, 0, store, opts)
	if err != nil {
		return 0, err
	}
	
	scribbleRows, ok := rows.(ScribbleRows)
	if !ok {
		return 0, errors.New("unexpected rows type")
	}
	
	return int64(scribbleRows.len), nil
}

// Misc gets

// GetByField retrieves a record by field
func (s ScribbleStore) GetByField(name, val, store string, dst interface{}) error {
	filter := map[string]interface{}{name: val}
	return s.FilterGet(filter, store, dst, nil)
}

// GetByFieldsByField retrieves specific fields of a record by field
func (s ScribbleStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	// Get the full record
	var fullRecord map[string]interface{}
	if err := s.GetByField(name, val, store, &fullRecord); err != nil {
		return err
	}
	
	// Extract only the requested fields
	result := make(map[string]interface{})
	for _, field := range fields {
		if value, ok := fullRecord[field]; ok {
			result[field] = value
		}
	}
	
	// Marshal and unmarshal to convert to the destination type
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}
	
	return json.Unmarshal(data, dst)
}

// BatchInsert inserts multiple records
func (s ScribbleStore) BatchInsert(data []interface{}, store string, opts ObjectStoreOptions) (keys []string, err error) {
	return s.SaveAll(store, data...)
}

// Close closes the store
func (s ScribbleStore) Close() {
	// Nothing to do for scribble
}