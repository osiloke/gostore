package gostore

import (
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

// var fs fileSystem = osFS{}

// type fileSystem interface {
// 	Open(name string) (file, error)
// 	Stat(name string) (os.FileInfo, error)
// }

// type file interface {
// 	io.Closer
// 	io.Reader
// 	io.ReaderAt
// 	io.Seeker
// 	Stat() (os.FileInfo, error)
// }

// // osFS implements fileSystem using the local disk.
// type osFS struct{}

// func (osFS) Open(name string) (file, error)        { return os.Open(name) }
// func (osFS) Stat(name string) (os.FileInfo, error) { return os.Stat(name) }

func TestScribbleSave(t *testing.T) {

	// Only pass t into top-level Convey calls
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, _ := store.Save(item["id"].(string), collection, &item)
			Convey("The stored data is retrieved", func() {
				var storedItem map[string]interface{}
				store.Get(key, collection, &storedItem)
				Convey("This should have the same id as the saved key", func() {
					So(storedItem["name"].(string), ShouldEqual, item["name"].(string))
				})
			})
		})
		os.RemoveAll(path)
	})

}

func TestScribbleDelete(t *testing.T) {

	// Only pass t into top-level Convey calls
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, _ := store.Save(item["id"].(string), collection, &item)
			Convey("After deleting the item", func() {
				store.Delete(key, collection)
				Convey("Trying to get the deleted key should fail", func() {
					var dst interface{}
					err := store.Get(key, collection, &dst)
					So(err, ShouldNotBeNil)
				})
			})
		})
		os.RemoveAll(path)
	})

}
