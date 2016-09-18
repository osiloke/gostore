package gostore

import (
	"encoding/json"
	"github.com/mgutz/logxi/v1"
	"github.com/nanobox-io/golang-scribble"
	"os"
)

type ScribbleStore struct {
	db   *scribble.Driver
	path string
}

func NewScribbleStore(path string) *ScribbleStore {
	if db, err := scribble.New(path, nil); err == nil {
		return &ScribbleStore{db, path}
	} else {
		log.Warn("cannot create scribble database", "err", err)
	}
	return nil
}

type ScribbleRows struct {
	rows []string
	i    int
	len  int
}

func (s ScribbleRows) LastError() error {
	return nil
}
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

func (s ScribbleRows) Close() {
	s.rows = nil
	s.i = -1
	s.len = -1
}

//Management Api
func (s ScribbleStore) CreateDatabase() error {
	return nil
}
func (s ScribbleStore) CreateTable(table string, sample interface{}) error {
	return nil
}

//Misc api
func (s ScribbleStore) GetStore() interface{} {
	return s.db
}
func (s ScribbleStore) Stats(store string) (map[string]interface{}, error) {
	return nil, nil
}

//New Api
func (s ScribbleStore) All(count int, skip int, store string) (ObjectRows, error) {
	_rows, err := s.db.ReadAll(store)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ScribbleRows{_rows, 0, len(_rows)}, nil
}
func (s ScribbleStore) AllCursor(store string) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s ScribbleStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
}

func (s ScribbleStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s ScribbleStore) Before(id string, count int, skip int, store string) (ObjectRows, error) {
	return nil, ErrNotImplemented
}

func (s ScribbleStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s ScribbleStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {

	return nil, ErrNotImplemented
}
func (s ScribbleStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {

	return 0, ErrNotImplemented
}

func (s ScribbleStore) Get(key string, store string, dst interface{}) error {
	err := s.db.Read(store, key, &dst)
	if _, ok := err.(*os.PathError); ok {
		return ErrNotFound
	} else {
		return err
	}
}

func (s ScribbleStore) Save(store string, src interface{}) (string, error) {
	var key = NewObjectId().String()
	if _v, ok := src.(map[string]interface{}); ok {
		if k, ok := _v["id"].(string); ok {
			key = k
		}
	}
	log.Debug("saving " + key + " to " + store)
	if err := s.db.Write(store, key, src); err != nil {
		return "", err
	}
	return key, nil
}
func (s ScribbleStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, ErrNotImplemented
}
func (s ScribbleStore) Update(key string, store string, src interface{}) error {
	return ErrNotImplemented
}
func (s ScribbleStore) Replace(key string, store string, src interface{}) error {
	return ErrNotImplemented
}
func (s ScribbleStore) Delete(key string, store string) error {
	return s.db.Delete(store, key)
}

//Filter
func (s ScribbleStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s ScribbleStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s ScribbleStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s ScribbleStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s ScribbleStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s ScribbleStore) BatchFilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s ScribbleStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	return 0, ErrNotImplemented
}

//Misc gets
func (s ScribbleStore) GetByField(name, val, store string, dst interface{}) error {
	return ErrNotImplemented
}
func (s ScribbleStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return ErrNotImplemented
}

func (s ScribbleStore) Close() {
}
