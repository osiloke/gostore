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

func TestScribbleAll(t *testing.T) {
	// Only pass t into top-level Convey calls
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		Convey("After inserting multiple test data items", func() {
			items := []map[string]interface{}{
				{"id": "1", "name": "orange", "qty": 10, "price": 4.99},
				{"id": "2", "name": "apple", "qty": 15, "price": 3.99},
				{"id": "3", "name": "banana", "qty": 8, "price": 2.99},
			}
			
			// Save each item
			for _, item := range items {
				_, err := store.Save(item["id"].(string), collection, &item)
				So(err, ShouldBeNil)
			}
			
			Convey("All items should be retrieved", func() {
				rows, err := store.All(0, 0, collection)
				So(err, ShouldBeNil)
				So(rows, ShouldNotBeNil)
				
				// Convert rows to array of items
				var retrievedItems []map[string]interface{}
				var ok bool = true
				for ok {
					var item map[string]interface{}
					ok, err = rows.Next(&item)
					if !ok {
						continue
					}
					retrievedItems = append(retrievedItems, item)
				}
				
				// Verify that we got all items
				So(len(retrievedItems), ShouldEqual, len(items))
				
				// Verify that each item is in the retrieved items
				for _, item := range items {
					found := false
					for _, retrievedItem := range retrievedItems {
						if retrievedItem["id"] == item["id"] {
							found = true
							So(retrievedItem["name"], ShouldEqual, item["name"])
							So(retrievedItem["qty"], ShouldEqual, item["qty"])
							So(retrievedItem["price"], ShouldEqual, item["price"])
							break
						}
					}
					So(found, ShouldBeTrue)
				}
			})
		})
		os.RemoveAll(path)
	})
}

func TestScribbleCreateDatabaseAndTable(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "products"
		
		Convey("Creating a database should not return an error", func() {
			err := store.CreateDatabase()
			So(err, ShouldBeNil)
			
			Convey("Creating a table should not return an error", func() {
				err := store.CreateTable(collection, nil)
				So(err, ShouldBeNil)
				
				Convey("After creating a table, we should be able to save and retrieve data", func() {
					item := map[string]interface{}{"id": "1", "name": "product1", "price": 9.99}
					key, err := store.Save(item["id"].(string), collection, &item)
					So(err, ShouldBeNil)
					
					var retrievedItem map[string]interface{}
					err = store.Get(key, collection, &retrievedItem)
					So(err, ShouldBeNil)
					So(retrievedItem["name"], ShouldEqual, item["name"])
				})
			})
		})
		os.RemoveAll(path)
	})
}

func TestScribbleGetStore(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		
		Convey("GetStore should return the underlying scribble.Driver", func() {
			driver := store.GetStore()
			So(driver, ShouldNotBeNil)
			// Verify that the returned object is of the correct type
			_, ok := driver.(*scribble.Driver)
			So(ok, ShouldBeTrue)
		})
		os.RemoveAll(path)
	})
}

func TestScribbleStats(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "stats_test"
		
		Convey("Stats should return nil for both result and error", func() {
			stats, err := store.Stats(collection)
			So(err, ShouldBeNil)
			So(stats, ShouldBeNil)
		})
		os.RemoveAll(path)
	})
}

func TestScribbleGetNotFound(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "nonexistent"
		
		Convey("Getting a non-existent key should return ErrNotFound", func() {
			var item map[string]interface{}
			err := store.Get("nonexistent", collection, &item)
			So(err, ShouldEqual, ErrNotFound)
		})
		os.RemoveAll(path)
	})
}

func TestScribbleRows(t *testing.T) {
	Convey("Given a ScribbleRows instance", t, func() {
		rows := []string{
			`{"id":"1","name":"item1"}`,
			`{"id":"2","name":"item2"}`,
		}
		scribbleRows := ScribbleRows{rows, 0, len(rows)}
		
		Convey("LastError should return nil", func() {
			err := scribbleRows.LastError()
			So(err, ShouldBeNil)
		})
		
		Convey("Next should unmarshal JSON correctly", func() {
			var item map[string]interface{}
			ok, err := scribbleRows.Next(&item)
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			So(item["id"], ShouldEqual, "1")
			So(item["name"], ShouldEqual, "item1")
			
			// Get the second item
			ok, err = scribbleRows.Next(&item)
			So(err, ShouldBeNil)
			So(ok, ShouldBeTrue)
			So(item["id"], ShouldEqual, "2")
			So(item["name"], ShouldEqual, "item2")
			
			// No more items
			ok, err = scribbleRows.Next(&item)
			So(err, ShouldBeNil)
			So(ok, ShouldBeFalse)
		})
		
		Convey("NextRaw should return false", func() {
			_, ok := scribbleRows.NextRaw()
			So(ok, ShouldBeFalse)
		})
		
		Convey("Close should reset the rows", func() {
			scribbleRows.Close()
			So(scribbleRows.rows, ShouldBeNil)
			So(scribbleRows.i, ShouldEqual, -1)
			So(scribbleRows.len, ShouldEqual, -1)
		})
	})
}

func TestScribbleAllEmpty(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "empty_collection"
		
		Convey("All on an empty collection should return ErrNotFound", func() {
			rows, err := store.All(0, 0, collection)
			So(err, ShouldEqual, ErrNotFound)
			So(rows, ShouldBeNil)
		})
		os.RemoveAll(path)
	})
}

func TestScribbleSaveInvalidJSON(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "invalid_json"
		
		Convey("Saving an item that can't be marshaled to JSON should return an error", func() {
			// Create a circular reference that can't be marshaled to JSON
			item := make(map[string]interface{})
			item["self"] = item
			
			_, err := store.Save("1", collection, item)
			So(err, ShouldNotBeNil)
		})
		os.RemoveAll(path)
	})
}

func TestScribbleClose(t *testing.T) {
	Convey("Giving a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		
		Convey("Close should not panic", func() {
			So(func() { store.Close() }, ShouldNotPanic)
		})
		os.RemoveAll(path)
	})
}