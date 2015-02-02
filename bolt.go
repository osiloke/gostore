package gostore
//TODO: Extract methods into functions
import (
	"github.com/boltdb/bolt"
	"log"
	"errors"
	"bytes"
)

type BoltStore struct {
	Bucket []byte
	Db *bolt.DB
}


func NewBoltStore(bucket string, db *bolt.DB) BoltStore{
	e := BoltStore{[]byte(bucket), db}
	e.CreateBucket(bucket)
	return e
}

func (s BoltStore) CreateBucket(bucket string){
	s.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			log.Fatalf("create bucket: %s", err)
		}
		return nil
	})
}

func Get(key []byte, bucket []byte, db *bolt.DB) (v []byte, err error){
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v = b.Get(key)
		if v == nil{
			return errors.New("Does not exist")
		}
		return nil
	})
	return
}

func (s BoltStore)	Get(key []byte, resource string) (v [][]byte, err error){
	s.CreateBucket(resource)
	vv, err := Get(key, []byte(resource), s.Db)
	if vv != nil{
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
	err := s.Db.Update(func(tx *bolt.Tx) error{
		b := tx.Bucket([]byte(resource))
		err := b.Delete(key)
		return err
	})
	return err
}

func (s BoltStore) DeleteAll(resource string) error {
	s.CreateBucket(resource)
	err := s.Db.Update(func(tx *bolt.Tx) error{
		b := tx.Bucket([]byte(resource))
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
 			b.Delete(k)
		}
		return nil
	})
	return err
}

func (s BoltStore) GetAll(count int, resource string) (objs [][][]byte, err error){
	s.CreateBucket(resource)
	err = s.Db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(resource)).Cursor()
		var lim int = 1
		for k, v := c.First(); k != nil; k, v = c.Next(){
			objs = append(objs,[][]byte{k, v})
			if lim == count{
				break
			}
			lim++
		}
		return err
	})
	return
}

func (s BoltStore) Filter(key []byte, count int, resource string) (objs [][]byte, err error){
	s.CreateBucket(resource)
	b_prefix := []byte(key)
	err = s.Db.View(func(tx *bolt.Tx) error {
		var lim int = 1
		c := tx.Bucket([]byte(resource)).Cursor()
		for k, v := c.Seek(b_prefix); bytes.HasPrefix(k, b_prefix); k, v = c.Next() {
			objs = append(objs, v)
			if lim == count{
				break
			}
			lim++
		}
		return nil
	})
	return
}

func (s BoltStore) StreamFilter(key []byte, count int, resource string) chan []byte{

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
				if lim == count{
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
