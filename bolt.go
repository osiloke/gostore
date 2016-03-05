package gostore

//TODO: Extract methods into functions
import (
	"bytes"
	"github.com/boltdb/bolt"
	"github.com/dustin/gojson"
	// "github.com/fatih/structs"
	// "github.com/ventu-io/go-shortid"
	"log"
	"sync"
	"time"
)

type HasID interface {
	GetId() string
}
type BoltStore struct {
	Bucket []byte
	Db     *bolt.DB
}

func NewBoltStore(bucket string, db *bolt.DB) BoltStore {
	e := BoltStore{[]byte(bucket), db}
	//	e.CreateBucket(bucket)
	return e
}

func NewBoltObjectStore(path string) (s BoltStore, err error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return
	}
	s = BoltStore{[]byte("_default"), db}
	//	e.CreateBucket(bucket)
	return
}

func (s BoltStore) CreateDatabase() error {
	return nil
}

func (s BoltStore) CreateTable(table string, sample interface{}) error {
	return nil
}

func (s BoltStore) GetStore() interface{} {
	return s.Db
}

func (s BoltStore) CreateBucket(bucket string) {
	s.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			log.Fatalf("create bucket: %s", err)
		}
		return nil
	})
}

func Get(key []byte, bucket []byte, db *bolt.DB) (v []byte, err error) {
	defer timeTrack(time.Now(), "Bolt Store::Get "+string(key)+" from "+string(bucket))
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v = b.Get(key)
		if v == nil {
			return ErrNotFound
		}
		return nil
	})
	return
}

func PrefixGet(prefix []byte, bucket []byte, db *bolt.DB) (k, v []byte, err error) {
	defer timeTrack(time.Now(), "Bolt Store::PrefixGet "+string(prefix)+" from "+string(bucket))
	err = db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		k, v = c.Seek(prefix)
		if v == nil {
			return ErrNotFound
		}
		return nil
	})
	return
}

func (s BoltStore) _Get(key, resource string) (v [][]byte, err error) {
	s.CreateBucket(resource)
	_key := []byte(key)
	vv, err := Get(_key, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{_key, vv}
	}
	return
}

func (s BoltStore) _PrefixGet(prefix []byte, resource string) (v [][]byte, err error) {
	s.CreateBucket(resource)
	kk, vv, err := PrefixGet(prefix, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{kk, vv}
	}
	return
}

func (s BoltStore) _Save(key []byte, data []byte, resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	})
	return err
}

func (s BoltStore) _Delete(key string, resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete([]byte(key))
		return err
	})
	return err
}

func (s BoltStore) _DeleteAll(resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			b.Delete(k)
		}
		return nil
	})
	return err
}

func newBoltRows(rows [][][]byte) BoltRows {
	total := len(rows)
	closed := make(chan bool)
	retrieved := make(chan string)
	nextItem := make(chan interface{})
	ci := 0
	b := BoltRows{nextItem: nextItem, closed: closed, retrieved: retrieved}
	go func() {
	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newBoltRows closed")
				break OUTER
				return
			case item := <-nextItem:
				// logger.Info("current index", "ci", ci, "total", total)
				if ci == total {
					b.lastError = ErrEOF
					// logger.Info("break bolt rows loop")
					break OUTER
					return
				} else {
					current := rows[ci]
					if err := json.Unmarshal(current[1], item); err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER
						return
					} else {
						retrieved <- string(current[0])
						ci++
					}
				}
			}
		}
		b.Close()
	}()
	return b
}

//New Api
type BoltRows struct {
	rows      [][][]byte
	i         int
	length    int
	retrieved chan string
	closed    chan bool
	nextItem  chan interface{}
	lastError error
	isClosed  bool
	sync.RWMutex
}

func (s BoltRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}
	//NOTE: Consider saving id in bolt data
	var _dst map[string]interface{}
	s.nextItem <- &_dst
	key := <-s.retrieved
	if key == "" {
		return false, nil
	}
	_dst["id"] = key
	_data, _ := json.Marshal(&_dst)
	json.Unmarshal(_data, dst)
	return true, nil
}
func (s BoltRows) LastError() error {
	return s.lastError
}
func (s BoltRows) Close() {
	// s.rows = nil
	// s.closed <- true
	logger.Info("close bolt rows")
	close(s.closed)
	close(s.retrieved)
	close(s.nextItem)
	// s.isClosed = true
}

func (s BoltStore) All(count int, skip int, store string) (ObjectRows, error) {
	_rows, err := s._GetAll(count, skip, store)
	// logger.Info("retrieved rows", "rows", _rows)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
}

func (s BoltStore) _GetAll(count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var skip_lim int = 1

		var lim int = 0
		//Skip a certain amount
		if skip > 0 {
			//make sure we hit the database once
			var target_count int = skip - 1
			for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Last()
			if k == nil {
				return err
			}
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				// logger.Info("count reached", "lim", lim, "count", count)
				return nil
			}
		}
		//Get next items after skipping or getting first item
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				// logger.Info("count reached", "lim", lim, "count", count)
				break
			}
		}
		return err
	})
	logger.Info("_GetAll done")
	return
}

func (s BoltStore) _GetAllAfter(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var lim int = 0
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(key); k != nil; k, _ = c.Next() {
				log.Println("Skipped ", string(k), "Current lim is ", skip_lim, " target count is ", target_count)
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(key)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
				lim++
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}
		for k, v := c.Next(); k != nil; k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return err
	})
	return
}

func (s BoltStore) _GetAllBefore(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var lim int = 0
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(key); k != nil; k, _ = c.Prev() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(key)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
				lim++
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return err
	})
	return
}

func (s BoltStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	b_prefix := []byte(prefix)
	err = s.Db.View(func(tx *bolt.Tx) error {
		var lim int = 1
		c := tx.Bucket([]byte(resource)).Cursor()
		if skip > 0 {
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.Seek(b_prefix); k != nil; k, _ = c.Next() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.Seek(b_prefix)
			if k != nil {
				objs = append(objs, [][]byte{k, v})
			} else {
				return err
			}
			if lim == count {
				return nil
			}
		}

		for k, v := c.Next(); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			lim++
			if lim == count {
				break
			}
		}
		return nil
	})
	return
}

func (s BoltStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
	s.CreateBucket(resource)
	b_prefix := []byte(suffix)
	err = s.Db.View(func(tx *bolt.Tx) error {
		var lim int = 1
		c := tx.Bucket([]byte(resource)).Cursor()
		for k, v := c.Seek(b_prefix); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, v)
			if lim == count {
				break
			}
			lim++
		}
		return nil
	})
	return
}

func (s BoltStore) StreamFilter(key []byte, count int, resource string) chan []byte {

	s.CreateBucket(resource)
	//Uses channels to stream filtered keys
	ch := make(chan []byte)
	go func() {
		b_prefix := []byte(key)
		s.Db.View(func(tx *bolt.Tx) error {
			var lim int = 1
			c := tx.Bucket([]byte(resource)).Cursor()
			for k, v := c.Seek(b_prefix); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
				ch <- v
				if lim == count {
					break
				}
				lim++
			}
			return nil
		})
		close(ch)
	}()
	return ch
}

func (s BoltStore) StreamAll(count int, resource string) chan [][]byte {

	s.CreateBucket(resource)
	//Uses channels to stream filtered keys
	ch := make(chan [][]byte)
	go func() {
		s.Db.View(func(tx *bolt.Tx) error {
			var lim int = 1
			c := tx.Bucket([]byte(resource)).Cursor()
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				ch <- [][]byte{k, v}
				if lim == count {
					break
				}
				lim++
			}
			close(ch)
			return nil
		})
	}()
	return ch
}

func (s BoltStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})
	err = s.Db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(bucket)).Stats()
		data["total_count"] = v.KeyN
		return nil
	})
	return
}

func (s BoltStore) AllCursor(store string) (ObjectRows, error) { return nil, ErrNotImplemented }

func (s BoltStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s BoltStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {
	_rows, err := s._GetAllAfter([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
} //Get all recent items from a key
func (s BoltStore) Before(id string, count int, skip int, store string) (ObjectRows, error) {
	_rows, err := s._GetAllBefore([]byte(id), count, skip, store)
	if err != nil {
		return nil, err
	}
	return newBoltRows(_rows), nil
} //Get all existing items before a key

func (s BoltStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
} //Get all recent items from a key
func (s BoltStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
} //Get all existing items before a key
func (s BoltStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) {
	return 0, ErrNotImplemented
} //Get all existing items before a key

func (s BoltStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}
func (s BoltStore) Save(store string, src interface{}) (string, error) {
	var key string
	if _v, ok := src.(map[string]interface{}); ok {
		if k, ok := _v["id"].(string); ok {
			key = k
		} else {
			key = NewObjectId().String()
		}
	} else if _v, ok := src.(HasID); ok {
		key = _v.GetId()
	} else {
		// if _key, err := shortid.Generate(); err == nil {
		// 	key = _key
		// } else {
		// 	logger.Error(ErrKeyNotValid.Error(), "err", err)
		// 	return ErrKeyNotValid
		// }
		key = NewObjectId().String()
	}
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	if err := s._Save([]byte(key), data, store); err != nil {
		return "", err
	}
	return key, nil
}
func (s BoltStore) Update(key string, store string, src interface{}) error  { return ErrNotImplemented }
func (s BoltStore) Replace(key string, store string, src interface{}) error { return ErrNotImplemented }
func (s BoltStore) Delete(key string, store string) error {
	return s._Delete(key, store)
}

//Filter
func (s BoltStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s BoltStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s BoltStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s BoltStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) {
	return nil, ErrNotImplemented
}
func (s BoltStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error {
	return ErrNotImplemented
}
func (s BoltStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error) {
	// if data, err := s.Stats(store); err != nil {
	// 	return 0, err
	// } else {
	// 	logger.Info("FilterCount", "data", data)
	// 	return data["count"].(int64), nil
	// }
	return 0, ErrNotImplemented
}

//Misc gets
func (s BoltStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s BoltStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return ErrNotImplemented
}
func (s BoltStore) Close() {}
