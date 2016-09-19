package gostore

import (
	r "github.com/dancannon/gorethink"
	// . "github.com/osiloke/gostore/testing"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"testing"
)

// var session *gorethink.Session
var url, url1, url2, url3, db, authKey string
var store RethinkStore
var session *r.Session

var collection string = "things"

func init() {
	// If the test is being run by wercker look for the rethink url
	url = os.Getenv("RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}

	url2 = os.Getenv("RETHINKDB_URL_1")
	if url2 == "" {
		url2 = "localhost:28016"
	}

	url2 = os.Getenv("RETHINKDB_URL_2")
	if url2 == "" {
		url2 = "localhost:28017"
	}

	url3 = os.Getenv("RETHINKDB_URL_3")
	if url3 == "" {
		url3 = "localhost:28018"
	}

	db = os.Getenv("RETHINKDB_DB")
	if db == "" {
		db = "test"
	}

	// Needed for running tests for RethinkDB with a non-empty authkey
	authKey = os.Getenv("RETHINKDB_AUTHKEY")
	session, err := r.Connect(r.ConnectOpts{
		Address: url,
		// MaxIdle: 10,
		// MaxOpen: 10,
		// Timeout: time.Second * 100,
	})
	//			r.SetVerbose(true)
	if err != nil {
		println("Error while opening rethinkdb database", err.Error())
		return
	}
	//Make global store
	store = RethinkStore{session, "gostore_test"}
	//Clear test database
	r.DBDrop("gostore_test").Exec(session)
	store.CreateDatabase()
	store.CreateTable(collection, nil)
}
func TestRethinkSaveAndGet(t *testing.T) {
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {

		store.CreateTable(collection, nil)
		Convey("After creating a things table", func() {
			Convey("After inserting one row", func() {
				item := map[string]interface{}{"name": "First Thing", "kind": "thing", "rating": 4.99}
				key, _ := store.Save(collection, &item)
				Convey("The stored data is retrieved", func() {
					var storedItem map[string]interface{}
					store.Get(key, collection, &storedItem)
					Convey("This should have the same id as the saved key", func() {
						So(storedItem["name"].(string), ShouldEqual, item["name"].(string))
					})
				})
			})
		})
		// ConveySaveAndGet(collection, store)
	})

}
func TestFilterGet(t *testing.T) {
	store.DeleteAll(collection)
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		Convey("After creating a things table", func() {
			Convey("After inserting a couple of rows", func() {
				items := []interface{}{
					map[string]interface{}{"name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"name": "Second Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"name": "First Something", "kind": "something", "rating": 4.99},
					map[string]interface{}{"name": "Second Something", "kind": "something", "rating": 4.99},
				}
				keys, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("The stored data is retrieved", func() {
					var row map[string]interface{}
					err := store.FilterGet(map[string]interface{}{"name": "First Something"}, collection, &row, nil)
					if err != nil {
						panic(err.Error())
					}
					Convey("This should return the filtered row", func() {

						So(row["id"].(string), ShouldEqual, keys[2])
					})
				})
			})
		})
	})

}
func TestFilterBefore(t *testing.T) {
	store.DeleteAll(collection)
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		Convey("After creating a things table", func() {
			Convey("After inserting 4 things and 3 somethings", func() {
				items := []interface{}{
					map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "3", "name": "First Something", "kind": "something", "rating": 4.99},
					map[string]interface{}{"id": "4", "name": "Second Something", "kind": "something", "rating": 4.99},
					map[string]interface{}{"id": "5", "name": "Third Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "6", "name": "Third Something", "kind": "something", "rating": 4.99},

					map[string]interface{}{"id": "7", "name": "Forth Thing", "kind": "thing", "rating": 4.99},
				}
				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("Then retrieve all rows before a particular key filtered by kind", func() {
					rows, err := store.FilterBefore("5", map[string]interface{}{
						"kind": "thing",
					}, 3, 0, collection, nil)
					if err != nil {
						panic(err.Error())
					}
					Convey("This will return the key and 2 kinds of thing's", func() {
						var ok bool = true
						resultIds := make([]string, 3)
						// Println(keys)
						ix := 0
						for ok {
							var row map[string]interface{}
							ok, err = rows.Next(&row)
							if !ok {
								continue
							}
							resultIds[ix] = row["id"].(string)
							ix++
						}
						So(resultIds, ShouldResemble, []string{"5", "2", "1"})
					})
				})
			})
		})
	})

}
func TestFilterGetAll(t *testing.T) {
	// collection := "rethink_getall"
	// store.CreateTable(collection, nil)
	store.DeleteAll(collection)
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		Convey("After creating a things table", func() {
			Convey("After inserting a couple of rows", func() {
				items := []interface{}{
					map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "3", "name": "First Something", "kind": "something", "rating": 4.99},
					map[string]interface{}{"id": "4", "name": "Second Something", "kind": "something", "rating": 4.99},
				}
				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("The stored data is retrieved", func() {
					rows, err := store.FilterGetAll(map[string]interface{}{
						"kind": "something",
					}, 2, 0, collection, nil)
					if err != nil {
						panic(err)
					}
					Convey("This should return 2 kinds of something's", func() {
						var ok bool = true
						results := []string{}
						for ok {
							var row map[string]interface{}
							ok, err = rows.Next(&row)
							if !ok {
								continue
							}
							results = append(results, row["id"].(string))
						}
						So(results, ShouldResemble, []string{"4", "3"})
					})
				})
			})
		})
	})

}
func TestRethinkSaveAll(t *testing.T) {

	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		store.DeleteAll(collection)
		Convey("After creating a things table", func() {
			Convey("After inserting two rows", func() {
				items := []interface{}{
					map[string]interface{}{"name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"name": "Second Thing", "kind": "thing", "rating": 4.99},
				}
				keys, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("The stored data is retrieved", func() {
					storedItems := []map[string]interface{}{}
					for _, key := range keys {
						var storedItem map[string]interface{}
						store.Get(key, collection, &storedItem)
						storedItems = append(storedItems, storedItem)
					}
					Convey("This should have the same id as the saved key", func() {
						So(storedItems[1]["name"].(string), ShouldEqual, items[1].(map[string]interface{})["name"].(string))
					})
				})
			})
		})
	})

}

func TestBatchFilterDelete(t *testing.T) {
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		store.DeleteAll(collection)
		Convey("After creating a things table", func() {
			Convey("After inserting two rows", func() {
				items := []interface{}{
					map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "3", "name": "First Something", "kind": "something", "rating": 4.99},
				}
				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("Both rows can be deleted using a batch delete filter", func() {
					err = store.BatchFilterDelete(map[string]interface{}{"kind": "thing"}, collection, nil)
					if err != nil {
						panic(err)
					}
					Convey("Now the store should contain only entries that dont match the filter", func() {
						count, err := store.FilterCount(nil, collection, nil)
						if err != nil {
							panic(err)
						}
						So(count, ShouldEqual, int64(1))
					})
				})
			})
		})
	})
}

func TestOrFilterBatchFilterDelete(t *testing.T) {
	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		var collection string = "things"
		store.DeleteAll(collection)
		Convey("After creating a things table", func() {
			Convey("After inserting two rows of different kinds", func() {
				items := []interface{}{
					map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
					map[string]interface{}{"id": "2", "name": "First Something", "kind": "something", "rating": 4.99},
				}
				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("Both rows can be deleted using a batch delete filter kind: =firstkind|secondkind ", func() {
					store.BatchFilterDelete(map[string]interface{}{"kind": "=thing|something"}, collection, nil)
					Convey("Now the store should be empty", func() {
						count, err := store.FilterCount(nil, collection, nil)
						if err != nil {
							panic(err)
						}
						So(count, ShouldEqual, int64(0))
					})
				})
			})
		})
	})
}
