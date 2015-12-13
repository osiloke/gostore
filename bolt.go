package gostore

//TODO: Extract methods into functions
import (
	"bytes"
	"github.com/boltdb/bolt"
	"github.com/fatih/structs"
	"log"
	"time"
	"github.com/dustin/gojson"
)


type BoltStore struct {
	Bucket []byte
	Db     *bolt.DB
}

func NewBoltStore(bucket string, db *bolt.DB) BoltStore {
	e := BoltStore{[]byte(bucket), db}
	//	e.CreateBucket(bucket)
	return e
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

func (s BoltStore) _Get(key []byte, resource string) (v [][]byte, err error) {
	s.CreateBucket(resource)
	vv, err := Get(key, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{key, vv}
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

func (s BoltStore) _Delete(key []byte, resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete(key)
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

func (s BoltStore) _GetAll(count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		//Skip a certain amount
		if skip > 0 {
			//make sure we hit the database once
			var skip_lim int = 1
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
			if k != nil {
				objs = append(objs, [][]byte{k, v})
			} else {
				return err
			}
		}

		//Get next items after skipping or getting first item
		var lim int = 2
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			if lim == count {
				break
			}
			lim++
		}
		return err
	})
	return
}

func (s BoltStore) _GetAllAfter(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
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
			} else {
				return err
			}
		}
		var lim int = 2
		for k, v := c.Next(); k != nil; k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			if lim == count {
				break
			}
			lim++
		}
		return err
	})
	return
}

func (s BoltStore) _GetAllBefore(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
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
			} else {
				return err
			}
		}
		var lim int = 2
		for k, v := c.Prev(); k != nil; k, v = c.Prev() {
			objs = append(objs, [][]byte{k, v})
			if lim == count {
				break
			}
			lim++
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
		}

		for k, v := c.Next(); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, [][]byte{k, v})
			if lim == count {
				break
			}
			lim++
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
	err = s.Db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(bucket)).Stats()
		data = structs.Map(v)
		return nil
	})
	return
}

func (s BoltStore) GetStoreObject() interface{}{
	return s.Db
}

//New Api
type BoltRows struct{
	rows [][][]byte
	i int
	len int
}
func (s BoltRows) Next(dst interface{}) (bool, error) {
	if s.i >= s.len{
		return false, nil
	}
	if err := json.Unmarshal(s.rows[s.i][1], dst); err != nil{
		return false, err
	}
	s.i++
	return true, nil
}

func (s BoltRows) Close() {
	s.rows = nil
}

func (s BoltStore) All(count int, skip int, store string) (ObjectRows, error){
	_rows, err := s._GetAll(count, skip, store)
	if err != nil{
		return nil, err
	}
	return BoltRows{_rows, 0, len(_rows)}, nil
}
func (s BoltStore) AllCursor(store string) (ObjectRows, error){return nil, nil}

func (s BoltStore) Since(id string, count int, skip int, store string) (ObjectRows, error) {return nil, nil} //Get all recent items from a key
func (s BoltStore) Before(id string, count int, skip int, store string) (ObjectRows, error){return nil, nil} //Get all existing items before a key

func (s BoltStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error){return nil, nil}  //Get all recent items from a key
func (s BoltStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error){return nil, nil} //Get all existing items before a key
func (s BoltStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error){return 0, nil} //Get all existing items before a key

func (s BoltStore) Get(key string, store string, dst interface{}) error{return nil}
func (s BoltStore) Save(store string, src interface{}) (string, error){return "", nil}
func (s BoltStore) Update(key string, store string, src interface{}) error{return nil}
func (s BoltStore) Replace(key string, store string, src interface{}) error{return nil}
func (s BoltStore) Delete(key string, store string) error{return nil}

//Filter
func (s BoltStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error {return nil}
func (s BoltStore) FilterReplace(filter map[string]interface{}, src interface{}, store string,  opts ObjectStoreOptions) error{return nil}
func (s BoltStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error {return nil}
func (s BoltStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error){return nil, nil}
func (s BoltStore) FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error{return nil}
func (s BoltStore) FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error){return 0, nil}

//Misc gets
func (s BoltStore) GetByField(name, val, store string, dst interface{}) error{return nil}
func (s BoltStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error){return nil}
func (s BoltStore) Close(){}
func NewBoltObjectStore(db *bolt.DB, database string) BoltObjectStore {
	e := BoltObjectStore{db}
	//	e.CreateBucket(bucket)
	return e
}



type BoltObjectStore struct{
	Db     *bolt.DB
}