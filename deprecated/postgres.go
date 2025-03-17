package gostore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jinzhu/gorm"
	_ "github.com/lib/pq"
)

type Storage struct {
	//	store string
	Id  string `sql:"type:varchar(255) PRIMARY KEY"`
	Raw string `sql:"jsonb"`
}

//func (c Storage) TableName() string {
//	return c.store
//}

type PostgresObjectStore struct {
	db       *gorm.DB
	database string
}

func NewPostgresObjectStore(db *gorm.DB, database string) PostgresObjectStore {
	s := PostgresObjectStore{db, database}
	s.CreateDatabase()
	return s
}

type PostgresRows struct {
	cursor *sql.Rows
}

func (s PostgresRows) LastError() error {
	return nil
}

func (s PostgresRows) Next(dst interface{}) (bool, error) {
	if ok := s.cursor.Next(); ok {
		var data []byte
		s.cursor.Scan(&data)
		err := json.Unmarshal(data, &dst)
		return true, err
	}
	return false, nil
}
func (s PostgresRows) NextRaw() ([]byte, bool) {
	return nil, false
}

func (s PostgresRows) Close() {
	s.cursor.Close()
}

func (s PostgresObjectStore) CreateDatabase() (err error) {
	return nil
}

func safeStoreName(name string) string {
	switch name {
	case "user":
		return "_user"
	case "group":
		return "_group"
	}
	return name
}
func (s PostgresObjectStore) CreateTable(store string, sample interface{}) (err error) {
	//http://stackoverflow.com/questions/21302520/golang-iterating-through-map-in-template
	//	sql := `
	//	CREATE TABLE {{ .name }}
	//	(
	//	{{ range $key, $value := .fields }}
	//	{{.column_name}} {{.type}}({{.size}}),
	//	{{ end }}
	//	....
	//	);
	//	`
	//	s.db.Table(store).CreateTable(&sample)
	//	s.db.Table(store).CreateTable(&Storage{})
	//	s.db.Table(store).AutoMigrate(&Storage{})
	err = s.db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
                id TEXT PRIMARY KEY,
                raw JSONB NOT NULL DEFAULT '{}'::JSONB
                )`, safeStoreName(store))).Error
	return nil
}

func (s PostgresObjectStore) All(count int, skip int, store string) (prows ObjectRows, err error) {
	rows, err := s.db.Table(safeStoreName(store)).Select("raw").Limit(count).Offset(skip).Rows()
	if err != nil {
		return
	}
	prows = PostgresRows{rows}
	return
}

func (s PostgresObjectStore) AllCursor(store string) (ObjectRows, error) {
	//	result, err := r.DB(s.Database).Table(store).Run(s.Session)
	//	if err != nil{
	//		return nil, err
	//	}
	//	defer result.Close()
	//	return RethinkRows{result}, nil
	return nil, errors.New("Not implemented")
}

func (s PostgresObjectStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, errors.New("Not implemented")
}

func (s PostgresObjectStore) Get(id, store string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select("raw").Where("id = ?", id)
	if result.Error != nil {
		return result.Error
	}
	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	json.Unmarshal(row, dst)
	return nil
}

//This will retrieve all old rows that were created before the row with id was created
// [1, 2, 3, 4], before 2 will return [3, 4]
func (s PostgresObjectStore) Before(id string, count int, skip int, store string) (prows ObjectRows, err error) {
	rows, err := s.db.Table(safeStoreName(store)).Select("raw").Where("id < ?", id).Limit(count).Offset(skip).Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return
	}
	prows = PostgresRows{rows}
	return
}

//This will retrieve all new rows that were created since the row with id was created
// [1, 2, 3, 4], since 2 will return [1]
func (s PostgresObjectStore) Since(id string, count, skip int, store string) (prows ObjectRows, err error) {
	rows, err := s.db.Table(safeStoreName(store)).Select("raw").Where("id > ?", id).Limit(count).Offset(skip).Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return
	}
	prows = PostgresRows{rows}
	return
}

func (s PostgresObjectStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	return nil, errors.New("Not Implemented")
}

func (s PostgresObjectStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	return 0, errors.New("Not Implemented")
}

func (s PostgresObjectStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (rows ObjectRows, err error) {
	return nil, errors.New("Not Implemented")
}

func (s PostgresObjectStore) Save(key, store string, src interface{}) (string, error) {
	data, err := json.Marshal(src)
	if err == nil {
		item := Storage{key, string(data)}
		result := s.db.Table(safeStoreName(store)).Create(&item)
		if result.Error != nil {
			err = result.Error
			if err == sql.ErrNoRows {
				return "", ErrNotFound
			}
		}
	}
	if err != nil {
		logger.Debug("Error saving doc", "Err", err)
	}
	return key, err
}

// SaveAll is deprecated: use BatchInsert instead
func (s PostgresObjectStore) SaveAll(store string, srcArray ...interface{}) (keys []string, err error) {
	// Convert variadic parameters to slice
	data := make([]interface{}, len(srcArray))
	for i, item := range srcArray {
		data[i] = item
	}
	return s.BatchInsert(data, store, nil)
}

func (s PostgresObjectStore) Update(id string, store string, src interface{}) (err error) {
	//TODO:perform update by retrieving existing data and merging data
	if data, err := json.Marshal(src); err == nil {
		err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Updates(map[string]interface{}{"raw": data}).Error
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
	}

	return

}

func (s PostgresObjectStore) Replace(id string, store string, src interface{}) (err error) {
	if data, err := json.Marshal(src); err == nil {
		err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Updates(map[string]interface{}{"raw": data}).Error
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
	}

	return

}

func (s PostgresObjectStore) Delete(id string, store string) (err error) {
	err = s.db.Table(safeStoreName(store)).Where("id = ?", id).Delete(&Storage{}).Error

	return
}

func (s PostgresObjectStore) GetStore() interface{} {
	return s.db
}

func (s PostgresObjectStore) Stats(store string) (map[string]interface{}, error) {
	var cnt int64
	result := s.db.Table(safeStoreName(store)).Count(&cnt)
	if result.Error != nil {
		if result.Error == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return nil, result.Error
	}
	return map[string]interface{}{"count": cnt}, nil
}

type QueryField struct {
	Name string      `json:"name"`
	Val  interface{} `json:"val"`
}

//func (f QueryField) MarshalJSON() ([]byte, error) {
//	return json.Marshal(struct{
//		Name string `json:"name"`
//		Val interface{} `json:"val"`
//	}{
//		Name: f.Name,
//		Val: f.Val,
//	})
//}

func (f QueryField) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		f.Name: f.Val,
	})
}

func (s PostgresObjectStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) (err error) {
	return errors.New("Not Implemented")
}
func (s PostgresObjectStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) (err error) {
	return errors.New("Not Implemented")
}

func (s PostgresObjectStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) (err error) {
	sfilter, _ := json.Marshal(filter)
	result := s.db.Table(safeStoreName(store)).Select("raw").Where("raw @>  ?", string(sfilter)).Limit(1)
	if result.Error != nil {
		return result.Error
	}

	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	json.Unmarshal(row, dst)
	return nil
	return errors.New("Not Implemented")
}

func (s PostgresObjectStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (prows ObjectRows, err error) {
	sfilter, _ := json.Marshal(filter)
	rows, err := s.db.Table(safeStoreName(store)).Select("raw").Where("raw @>  ?", string(sfilter)).Rows()
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNotFound
		}
		return
	}
	prows = PostgresRows{rows}
	return
}
func (s PostgresObjectStore) BatchDelete(ids []interface{}, store string, opts ObjectStoreOptions) (err error) {
	return ErrNotImplemented
}
func (s PostgresObjectStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts ObjectStoreOptions) (err error) {
	return ErrNotImplemented
}
func (s PostgresObjectStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) (err error) {
	return errors.New("Not Implemented")
}

func (s PostgresObjectStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s PostgresObjectStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	return 0, errors.New("Not Implemented")
}

func (s PostgresObjectStore) GetByField(name, val, store string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select("raw").Where("raw @>  ?", fmt.Sprintf(`{"`+name+`": "%s"}`, val)).Limit(1)
	if result.Error != nil {
		return result.Error
	}

	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	logger.Debug("Err if any", "err", err)
	json.Unmarshal(row, dst)
	return nil
}

func (s PostgresObjectStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	result := s.db.Table(safeStoreName(store)).Select(fields).Where("raw @>  ?", fmt.Sprintf(`{"`+name+`": "%s"}`, val)).Limit(1)
	if result.Error != nil {
		return result.Error
	}
	var row []byte
	err = result.Row().Scan(&row)
	if err == sql.ErrNoRows {
		return ErrNotFound
	}
	json.Unmarshal(row, dst)
	return nil
}
func (s PostgresObjectStore) BatchInsert(data []interface{}, store string, opts ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, 0, len(data))
	for _, src := range data {
		if jsonData, err := json.Marshal(src); err == nil {
			var id string
			if m, ok := src.(map[string]interface{}); ok {
				if idVal, hasID := m["id"]; hasID {
					if idStr, isStr := idVal.(string); isStr {
						id = idStr
					} else {
						id = NewObjectId().Hex()
						m["id"] = id
					}
				} else {
					id = NewObjectId().Hex()
					m["id"] = id
				}
			} else {
				id = NewObjectId().Hex()
			}
			
			item := Storage{id, string(jsonData)}
			result := s.db.Table(safeStoreName(store)).Create(&item)
			if result.Error != nil {
				err = result.Error
				return keys, err
			}
			keys = append(keys, id)
		} else {
			logger.Debug("Error marshaling doc", "Err", err)
			return keys, err
		}
	}
	return keys, nil
}
func (s PostgresObjectStore) Close() {
	s.db.Close()
}