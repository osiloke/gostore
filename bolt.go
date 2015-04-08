package gostore

//TODO: Extract methods into functions
import (
	"bytes"
	"errors"
	"github.com/boltdb/bolt"
	"github.com/fatih/structs"
	"log"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %d", name, elapsed)
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
			return errors.New("Does not exist")
		}
		return nil
	})
	return
}

func (s BoltStore) Get(key []byte, resource string) (v [][]byte, err error) {
	s.CreateBucket(resource)
	vv, err := Get(key, []byte(resource), s.Db)
	if vv != nil {
		v = [][]byte{key, vv}
	}
	return
}

func (s BoltStore) Save(key []byte, data []byte, resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Put(key, data)
		return err
	})
	return err
}

func (s BoltStore) Delete(key []byte, resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(resource))
		err := b.Delete(key)
		return err
	})
	return err
}

func (s BoltStore) DeleteAll(resource string) error {
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

func (s BoltStore) GetAll(count int, skip int, resource string) (objs [][][]byte, err error) {
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		//Skip a certain amount
		if skip > 0 {
			//make sure we hit the database once
			var skip_lim int = 1
			var target_count int = skip - 1
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if skip_lim >= target_count {
					break
				}
				skip_lim++
			}
		} else {
			//no skip needed. Get first item
			k, v := c.First()
			if k != nil {
				objs = append(objs, [][]byte{k, v})
			} else {
				return err
			}
		}

		//Get next items after skipping or getting first item
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

func (s BoltStore) GetAllAfter(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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

func (s BoltStore) GetAllBefore(key []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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

func (s BoltStore) Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
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
			for k, v := c.First(); k != nil; k, v = c.Next() {
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
