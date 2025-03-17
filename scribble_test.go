package gostore

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestScribbleSave(t *testing.T) {
	Convey("Given a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, err := store.Save(item["id"].(string), collection, &item)
			
			So(err, ShouldBeNil)
			So(key, ShouldEqual, "1")
			
			Convey("The stored data is retrieved", func() {
				var storedItem map[string]interface{}
				err := store.Get(key, collection, &storedItem)
				
				So(err, ShouldBeNil)
				So(storedItem["name"].(string), ShouldEqual, item["name"].(string))
				So(storedItem["qty"].(float64), ShouldEqual, 10)
				So(storedItem["price"].(float64), ShouldEqual, 4.99)
			})
		})
	})
}

func TestScribbleDelete(t *testing.T) {
	Convey("Given a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, err := store.Save(item["id"].(string), collection, &item)
			
			So(err, ShouldBeNil)
			
			Convey("After deleting the item", func() {
				err := store.Delete(key, collection)
				
				So(err, ShouldBeNil)
				
				Convey("Trying to get the deleted key should fail", func() {
					var dst interface{}
					err := store.Get(key, collection, &dst)
					
					So(err, ShouldNotBeNil)
					So(err, ShouldEqual, ErrNotFound)
				})
			})
		})
	})
}

func TestScribbleAll(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "qty": 20, "price": 2.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("All should return all items", func() {
			rows, err := store.All(0, 0, collection)
			
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 3)
		})
		
		Convey("All with count should limit results", func() {
			rows, err := store.All(2, 0, collection)
			
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 2)
		})
		
		Convey("All with skip should skip results", func() {
			rows, err := store.All(0, 1, collection)
			
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 2)
		})
	})
}

func TestScribbleUpdate(t *testing.T) {
	Convey("Given a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, err := store.Save(item["id"].(string), collection, &item)
			
			So(err, ShouldBeNil)
			
			Convey("After updating the item", func() {
				update := map[string]interface{}{"qty": 20, "price": 5.99}
				err := store.Update(key, collection, update)
				
				So(err, ShouldBeNil)
				
				Convey("The item should be updated", func() {
					var updatedItem map[string]interface{}
					err := store.Get(key, collection, &updatedItem)
					
					So(err, ShouldBeNil)
					So(updatedItem["name"].(string), ShouldEqual, "orange")
					So(updatedItem["qty"].(float64), ShouldEqual, 20)
					So(updatedItem["price"].(float64), ShouldEqual, 5.99)
				})
			})
		})
	})
}

func TestScribbleReplace(t *testing.T) {
	Convey("Given a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		Convey("After inserting some test data", func() {
			item := map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99}
			key, err := store.Save(item["id"].(string), collection, &item)
			
			So(err, ShouldBeNil)
			
			Convey("After replacing the item", func() {
				replacement := map[string]interface{}{"id": "1", "name": "apple", "qty": 20, "price": 3.99}
				err := store.Replace(key, collection, replacement)
				
				So(err, ShouldBeNil)
				
				Convey("The item should be replaced", func() {
					var replacedItem map[string]interface{}
					err := store.Get(key, collection, &replacedItem)
					
					So(err, ShouldBeNil)
					So(replacedItem["name"].(string), ShouldEqual, "apple")
					So(replacedItem["qty"].(float64), ShouldEqual, 20)
					So(replacedItem["price"].(float64), ShouldEqual, 3.99)
				})
			})
		})
	})
}

func TestScribbleFilterGet(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("FilterGet should return a matching item", func() {
			var result map[string]interface{}
			err := store.FilterGet(map[string]interface{}{"name": "apple"}, collection, &result, nil)
			
			So(err, ShouldBeNil)
			So(result["id"].(string), ShouldEqual, "2")
			So(result["name"].(string), ShouldEqual, "apple")
		})
		
		Convey("FilterGet should return error for non-matching filter", func() {
			var result map[string]interface{}
			err := store.FilterGet(map[string]interface{}{"name": "grape"}, collection, &result, nil)
			
			So(err, ShouldEqual, ErrNotFound)
		})
	})
}

func TestScribbleFilterGetAll(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("FilterGetAll should return all matching items", func() {
			rows, err := store.FilterGetAll(map[string]interface{}{"category": "fruit"}, 0, 0, collection, nil)
			
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 3)
			
			for _, result := range results {
				So(result["category"].(string), ShouldEqual, "fruit")
			}
		})
		
		Convey("FilterGetAll with count should limit results", func() {
			rows, err := store.FilterGetAll(map[string]interface{}{"category": "fruit"}, 2, 0, collection, nil)
			
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 2)
		})
	})
}

func TestScribbleFilterUpdate(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("FilterUpdate should update all matching items", func() {
			err := store.FilterUpdate(map[string]interface{}{"category": "fruit"}, map[string]interface{}{"organic": true}, collection, nil)
			
			So(err, ShouldBeNil)
			
			rows, err := store.FilterGetAll(map[string]interface{}{"category": "fruit"}, 0, 0, collection, nil)
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 3)
			
			for _, result := range results {
				So(result["organic"].(bool), ShouldBeTrue)
			}
			
			// Verify non-matching items were not updated
			var vegetable map[string]interface{}
			err = store.Get("4", collection, &vegetable)
			So(err, ShouldBeNil)
			_, hasOrganic := vegetable["organic"]
			So(hasOrganic, ShouldBeFalse)
		})
	})
}

func TestScribbleFilterDelete(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("FilterDelete should delete all matching items", func() {
			err := store.FilterDelete(map[string]interface{}{"category": "fruit"}, collection, nil)
			
			So(err, ShouldBeNil)
			
			rows, err := store.All(0, 0, collection)
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 1)
			So(results[0]["category"].(string), ShouldEqual, "vegetable")
		})
	})
}

func TestScribbleSaveAll(t *testing.T) {
	Convey("Given a scribble store", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		Convey("SaveAll should save multiple items", func() {
			items := []interface{}{
				map[string]interface{}{"id": "1", "name": "orange", "qty": 10, "price": 4.99},
				map[string]interface{}{"id": "2", "name": "apple", "qty": 15, "price": 3.99},
				map[string]interface{}{"id": "3", "name": "banana", "qty": 20, "price": 2.99},
			}
			
			keys, err := store.SaveAll(collection, items...)
			
			So(err, ShouldBeNil)
			So(len(keys), ShouldEqual, 3)
			So(keys[0], ShouldEqual, "1")
			So(keys[1], ShouldEqual, "2")
			So(keys[2], ShouldEqual, "3")
			
			// Verify items were saved
			rows, err := store.All(0, 0, collection)
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 3)
		})
	})
}

func TestScribbleBatchOperations(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("BatchDelete should delete multiple items by ID", func() {
			err := store.BatchDelete([]interface{}{"1", "3"}, collection, nil)
			
			So(err, ShouldBeNil)
			
			rows, err := store.All(0, 0, collection)
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 2)
			
			// Verify the correct items were deleted
			var item map[string]interface{}
			err = store.Get("1", collection, &item)
			So(err, ShouldEqual, ErrNotFound)
			
			err = store.Get("3", collection, &item)
			So(err, ShouldEqual, ErrNotFound)
			
			err = store.Get("2", collection, &item)
			So(err, ShouldBeNil)
			
			err = store.Get("4", collection, &item)
			So(err, ShouldBeNil)
		})
		
		Convey("BatchUpdate should update multiple items by ID", func() {
			ids := []interface{}{"1", "3"}
			updates := []interface{}{
				map[string]interface{}{"qty": 5, "sale": true},
				map[string]interface{}{"qty": 25, "sale": true},
			}
			
			err := store.BatchUpdate(ids, updates, collection, nil)
			
			So(err, ShouldBeNil)
			
			// Verify updates
			var item1 map[string]interface{}
			err = store.Get("1", collection, &item1)
			So(err, ShouldBeNil)
			So(item1["qty"].(float64), ShouldEqual, 5)
			So(item1["sale"].(bool), ShouldBeTrue)
			
			var item3 map[string]interface{}
			err = store.Get("3", collection, &item3)
			So(err, ShouldBeNil)
			So(item3["qty"].(float64), ShouldEqual, 25)
			So(item3["sale"].(bool), ShouldBeTrue)
			
			// Verify other items were not updated
			var item2 map[string]interface{}
			err = store.Get("2", collection, &item2)
			So(err, ShouldBeNil)
			So(item2["qty"].(float64), ShouldEqual, 15)
			_, hasSale := item2["sale"]
			So(hasSale, ShouldBeFalse)
		})
		
		Convey("BatchFilterDelete should delete items matching multiple filters", func() {
			filters := []map[string]interface{}{
				{"name": "orange"},
				{"name": "carrot"},
			}
			
			err := store.BatchFilterDelete(filters, collection, nil)
			
			So(err, ShouldBeNil)
			
			rows, err := store.All(0, 0, collection)
			So(err, ShouldBeNil)
			
			var results []map[string]interface{}
			for {
				var item map[string]interface{}
				next, err := rows.Next(&item)
				
				So(err, ShouldBeNil)
				
				if !next {
					break
				}
				
				results = append(results, item)
			}
			
			So(len(results), ShouldEqual, 2)
			
			// Verify the correct items were deleted
			var item map[string]interface{}
			err = store.Get("1", collection, &item)
			So(err, ShouldEqual, ErrNotFound)
			
			err = store.Get("4", collection, &item)
			So(err, ShouldEqual, ErrNotFound)
			
			err = store.Get("2", collection, &item)
			So(err, ShouldBeNil)
			
			err = store.Get("3", collection, &item)
			So(err, ShouldBeNil)
		})
	})
}

func TestScribbleGetByField(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("GetByField should return an item matching the field", func() {
			var result map[string]interface{}
			err := store.GetByField("name", "apple", collection, &result)
			
			So(err, ShouldBeNil)
			So(result["id"].(string), ShouldEqual, "2")
			So(result["name"].(string), ShouldEqual, "apple")
		})
		
		Convey("GetByFieldsByField should return specific fields of an item", func() {
			var result map[string]interface{}
			err := store.GetByFieldsByField("name", "apple", collection, []string{"id", "price"}, &result)
			
			So(err, ShouldBeNil)
			So(result["id"].(string), ShouldEqual, "2")
			So(result["price"].(float64), ShouldEqual, 3.99)
			_, hasName := result["name"]
			So(hasName, ShouldBeFalse)
		})
	})
}

func TestScribbleFilterCount(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("FilterCount should return the count of matching items", func() {
			count, err := store.FilterCount(map[string]interface{}{"category": "fruit"}, collection, nil)
			
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 3)
			
			count, err = store.FilterCount(map[string]interface{}{"category": "vegetable"}, collection, nil)
			
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 1)
			
			count, err = store.FilterCount(map[string]interface{}{"category": "dairy"}, collection, nil)
			
			So(err, ShouldBeNil)
			So(count, ShouldEqual, 0)
		})
	})
}

func TestScribbleStats(t *testing.T) {
	Convey("Given a scribble store with multiple items", t, func() {
		path := "/tmp/scribble.store.test.json"
		store := NewScribbleStore(path)
		collection := "shopping"
		
		Reset(func() {
			os.RemoveAll(path)
		})
		
		items := []map[string]interface{}{
			{"id": "1", "name": "orange", "category": "fruit", "qty": 10, "price": 4.99},
			{"id": "2", "name": "apple", "category": "fruit", "qty": 15, "price": 3.99},
			{"id": "3", "name": "banana", "category": "fruit", "qty": 20, "price": 2.99},
			{"id": "4", "name": "carrot", "category": "vegetable", "qty": 8, "price": 1.99},
		}
		
		for _, item := range items {
			_, err := store.Save(item["id"].(string), collection, item)
			So(err, ShouldBeNil)
		}
		
		Convey("Stats should return store statistics", func() {
			stats, err := store.Stats(collection)
			
			So(err, ShouldBeNil)
			So(stats["count"], ShouldEqual, 4)
		})
	})
}