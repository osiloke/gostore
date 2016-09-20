package gostore

import (
	r "github.com/dancannon/gorethink"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

// var session *gorethink.Session
var collection = "things"
var mock r.Mock

var names = map[int]string{
	1: "First",
	2: "Second",
	3: "Third",
	4: "Fourth",
	5: "Fifth",
}

func init() {
}
func TestRethinkSaveAndGet(t *testing.T) {
	id := "1"
	entry := map[string]interface{}{"id": id, "name": "First Thing",
		"kind": "thing", "rating": 4.99}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things")).Return([]interface{}{}, nil)
	mock.On(r.DB("gostore_test").Table("things").Delete(r.DeleteOpts{Durability: "hard"})).Return(nil, nil)
	mock.On(r.DB("gostore_test").Table("things").Insert(entry, r.InsertOpts{Durability: "soft"})).Return(id, nil)
	mock.On(r.DB("gostore_test").Table("things").Get("1")).Return(entry, nil)
	//Make global store
	store := RethinkStore{mock, "gostore_test"}
	// store.DeleteAll(collection)

	Convey("Giving a rethink store", t, func() {
		Convey("After creating a things table", func() {
			Convey("After inserting one row", func() {
				store.Save(collection, &entry)
				Convey("The stored data is retrieved", func() {
					var storedItem map[string]interface{}
					store.Get(id, collection, &storedItem)
					Convey("This should have the same id as the saved key", func() {
						So(storedItem["name"].(string), ShouldEqual, entry["name"].(string))
					})
				})
			})
		})
	})

}

// func genData(length, pattern int) ([]string, []map[string]interface{}){}
func TestFilterGet(t *testing.T) {
	// store.DeleteAll(collection)

	ikeys := []int{1, 2, 3, 4}
	expectedKeys := []string{"1", "2", "3", "4"}
	pattern := 2
	entries := make([]interface{}, 4)
	kind := "thing"
	for i, k := range ikeys {
		if i > pattern {
			kind = "something"
		}
		entries[i] = map[string]interface{}{"id": expectedKeys[i], "name": names[k], "kind": kind}
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Delete(r.DeleteOpts{Durability: "hard"})).Return(nil, nil)
	mock.On(r.DB("gostore_test").Table("things").Insert(entries, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things")).Return(entries, nil)
	mock.On(r.DB("gostore_test").Table("things").Filter(r.Row.Field("name").Eq("Fourth")).Limit(1)).Return(entries[3], nil)
	//Make global store
	Convey("Giving a rethink store", t, func() {
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting a couple of rows", func() {
				_, err := store.SaveAll(collection, entries...)
				if err != nil {
					panic(err)
				}
				Convey("The stored data is retrieved", func() {
					var row map[string]interface{}
					err := store.FilterGet(map[string]interface{}{"name": "Fourth"}, collection, &row, nil)
					if err != nil {
						panic(err)
					}
					Convey("This should return the filtered row", func() {

						So(row["id"].(string), ShouldEqual, expectedKeys[3])
					})
				})
			})
		})
	})

}

func TestFilterBefore(t *testing.T) {
	expectedKeys := []string{"1", "2", "3", "4", "5", "6"}
	items := []interface{}{
		map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing"},
		map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing"},
		map[string]interface{}{"id": "3", "name": "First Something", "kind": "something"},
		map[string]interface{}{"id": "4", "name": "Second Something", "kind": "something"},
		map[string]interface{}{"id": "5", "name": "Third Thing", "kind": "thing"},
		map[string]interface{}{"id": "6", "name": "Third Something", "kind": "something"},
		map[string]interface{}{"id": "7", "name": "Forth Thing", "kind": "thing"},
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Insert(items, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things")).Return(items, nil)
	mock.On(r.DB("gostore_test").Table("things").Between(r.MinVal, "5", r.BetweenOpts{RightBound: "closed"}).
		OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Filter(r.Row.Field("kind").Eq("thing")).
		Limit(3)).Return([]interface{}{items[4], items[3], items[2]}, nil)

	Convey("Giving a rethink store", t, func() {
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting 4 things and 3 somethings", func() {

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
						So(resultIds, ShouldResemble, []string{"5", "4", "3"})
					})
				})
			})
		})
	})

}

func TestFilterGetAll(t *testing.T) {
	expectedKeys := []string{"1", "2", "3", "4"}
	items := []interface{}{
		map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing"},
		map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing"},
		map[string]interface{}{"id": "3", "name": "First Something", "kind": "something"},
		map[string]interface{}{"id": "4", "name": "Second Something", "kind": "something"},
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Insert(items, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things").OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Filter(r.Row.Field("kind").Eq("something")).Slice(0, 2)).Return([]interface{}{items[3], items[2]}, nil)

	Convey("Giving a rethink store", t, func() {
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting a couple of rows", func() {

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
	expectedKeys := []string{"1", "2"}
	items := []interface{}{
		map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing"},
		map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing"},
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Insert(items, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things").Get("1")).Return(items[0], nil)
	mock.On(r.DB("gostore_test").Table("things").Get("2")).Return(items[1], nil)

	// Only pass t into top-level Convey calls
	Convey("Giving a rethink store", t, func() {
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting two rows", func() {
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

func TestFilterDelete(t *testing.T) {
	expectedKeys := []string{"1", "2", "3"}
	items := []interface{}{
		map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing"},
		map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing"},
		map[string]interface{}{"id": "3", "name": "First Something", "kind": "something"},
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Insert(items, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things").OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Filter(r.Row.Field("kind").Eq("thing")).Delete(r.DeleteOpts{Durability: "hard"})).Return(r.WriteResponse{Deleted: 1}, nil)
	mock.On(r.DB("gostore_test").Table("things").OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Count()).Return(1, nil)

	Convey("Giving a rethink store", t, func() {
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting two rows", func() {

				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("Both rows can be deleted using a batch delete filter", func() {
					err = store.FilterDelete(map[string]interface{}{"kind": "thing"}, collection, nil)
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

func TestOrFilterDelete(t *testing.T) {
	expectedKeys := []string{"1", "2"}
	items := []interface{}{
		map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
		map[string]interface{}{"id": "2", "name": "First Something", "kind": "something", "rating": 4.99},
	}
	mock := r.NewMock()
	mock.On(r.DB("gostore_test").Table("things").Insert(items, r.InsertOpts{Durability: "hard"})).Return(r.WriteResponse{GeneratedKeys: expectedKeys}, nil)
	mock.On(r.DB("gostore_test").Table("things").OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Filter(r.Row.Field("kind").Eq("thing").Or(r.Row.Field("kind").Eq("something"))).Delete(r.DeleteOpts{Durability: "hard"})).Return(r.WriteResponse{Deleted: 1}, nil)
	mock.On(r.DB("gostore_test").Table("things").OrderBy(r.OrderByOpts{Index: r.Desc("id")}).Count()).Return(0, nil)

	Convey("Giving a rethink store", t, func() {
		var collection string = "things"
		store := RethinkStore{mock, "gostore_test"}
		Convey("After creating a things table", func() {
			Convey("After inserting two rows of different kinds", func() {

				_, err := store.SaveAll(collection, items...)
				if err != nil {
					panic(err)
				}
				Convey("Both rows can be deleted using a batch delete filter kind: =firstkind|secondkind ", func() {
					store.FilterDelete(map[string]interface{}{"kind": "=thing|something"}, collection, nil)
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

func TestParseFilterOpsOrEqTerm(t *testing.T) {
	mock := r.NewMock()
	store := RethinkStore{mock, "gostore_test"}
	Convey("Given a filter value", t, func() {
		key := "kind"
		val := "=thing|fish"
		Convey("Determine what conditions to perform on the val", func() {
			t := store.parseFilterOpsTerm(key, val)
			So(t.String(), ShouldEqual, `r.Row.Field("kind").Eq("thing").Or(r.Row.Field("kind").Eq("fish"))`)

		})

	})

}

func TestParseFilterOpsOrMatchTerm(t *testing.T) {
	mock := r.NewMock()
	store := RethinkStore{mock, "gostore_test"}
	Convey("Given a filter value", t, func() {
		key := "kind"
		val := "~thing|fish"
		Convey("Determine what conditions to perform on the val", func() {
			t := store.parseFilterOpsTerm(key, val)
			So(t.String(), ShouldEqual, `r.Row.Field("kind").Match("thing").Or(r.Row.Field("kind").Match("fish"))`)

		})

	})

}

func TestTransformFilter(t *testing.T) {
	mock := r.NewMock()
	store := RethinkStore{mock, "gostore_test"}
	Convey("Given a filter", t, func() {
		/*
			food is either amala or ewedu and place is lagos
			or
			beverage is coke and server is olu
		*/

		filter := map[string]interface{}{
			"or": []interface{}{
				map[string]interface{}{
					"food":  "~amala|ewedu",
					"place": "lagos",
				},
				map[string]interface{}{
					"beverage": "coke",
					"server":   "olu",
				},
			},
		}
		Convey("figure out rethink conditions", func() {
			term := store.transformFilter(nil, filter)
			So(term.String(), ShouldBeIn, []string{
				`r.Or(r.Row.Field("place").Eq("lagos").And(r.Row.Field("food").Match("amala").Or(r.Row.Field("food").Match("ewedu"))), r.Row.Field("server").Eq("olu").And(r.Row.Field("beverage").Eq("coke")))`,
				`r.Or(r.Row.Field("place").Eq("lagos").And(r.Row.Field("food").Match("amala").Or(r.Row.Field("food").Match("ewedu"))), r.Row.Field("beverage").Eq("coke").And(r.Row.Field("server").Eq("olu")))`,
				`r.Or(r.Row.Field("food").Match("amala").Or(r.Row.Field("food").Match("ewedu")).And(r.Row.Field("place").Eq("lagos")), r.Row.Field("server").Eq("olu").And(r.Row.Field("beverage").Eq("coke")))`,
				`r.Or(r.Row.Field("food").Match("amala").Or(r.Row.Field("food").Match("ewedu")).And(r.Row.Field("place").Eq("lagos")), r.Row.Field("beverage").Eq("coke").And(r.Row.Field("server").Eq("olu")))`,
			})
		})
	})
}
func TestGetRootTermWithoutIndexes(t *testing.T) {
	Convey("Giving a store", t, func() {
		mock := r.NewMock()
		store := RethinkStore{mock, "gostore_test"}
		Convey("and a filter", func() {
			filter := map[string]interface{}{"id": "1", "kind": "thing"}
			Convey("generating a root term without indexes should give a slow term without any indexing", func() {
				term := store.getRootTerm("things", filter, nil)

				So(term.String(), ShouldBeIn, []string{
					`r.DB("gostore_test").Table("things").OrderBy(index=r.Desc("id")).Filter(func(var_11 r.Term) r.Term { return r.Row.Field("kind").Eq("thing").And(r.Row.Field("id").Eq("1")) })`,
					`r.DB("gostore_test").Table("things").OrderBy(index=r.Desc("id")).Filter(func(var_11 r.Term) r.Term { return r.Row.Field("id").Eq("1").And(r.Row.Field("kind").Eq("thing")) })`,
				})
			})
		})
	})
}

func TestGetRootTermWithIndexes(t *testing.T) {
	Convey("Giving a store", t, func() {
		mock := r.NewMock()
		store := RethinkStore{mock, "gostore_test"}
		Convey("and a filter", func() {
			filter := map[string]interface{}{"id": "1", "kind": "thing"}
			//Indexes should be inserted in order of preference
			opts := DefaultObjectStoreOptions{map[string][]string{
				"kind_id": {"kind", "id"},
				"kind":    {},
				"id":      {},
			},
			}
			Convey("generating a root term with indexes should give an optimized term", func() {
				term := store.getRootTerm("things", filter, opts)

				So(term.String(), ShouldBeIn, []string{
					`r.DB("gostore_test").Table("things").Between(["thing", r.MinVal()], ["thing", r.MaxVal()], index="kind_id", right_bound="closed").OrderBy(index=r.Desc("kind_id")).Filter(func(var_12 r.Term) r.Term { return r.Row.Field("kind").Eq("thing").And(r.Row.Field("id").Eq("1")) })`,
					`r.DB("gostore_test").Table("things").Between(["thing", r.MinVal()], ["thing", r.MaxVal()], right_bound="closed", index="kind_id").OrderBy(index=r.Desc("kind_id")).Filter(func(var_12 r.Term) r.Term { return r.Row.Field("id").Eq("1").And(r.Row.Field("kind").Eq("thing")) })`,
					`r.DB("gostore_test").Table("things").Between(["thing", r.MinVal()], ["thing", r.MaxVal()], index="kind_id", right_bound="closed").OrderBy(index=r.Desc("kind_id")).Filter(func(var_12 r.Term) r.Term { return r.Row.Field("id").Eq("1").And(r.Row.Field("kind").Eq("thing")) })`,
					`r.DB("gostore_test").Table("things").GetAll("1", index="id").Filter(func(var_12 r.Term) r.Term { return r.Row.Field("id").Eq("1").And(r.Row.Field("kind").Eq("thing")) })`,
				})
			})
		})
	})
}

// func TestBatchFilterDelete(t *testing.T) {
// 	// Only pass t into top-level Convey calls
// 	Convey("Giving a rethink store", t, func() {
// 		store.DeleteAll(collection)
// 		Convey("After creating a things table", func() {
// 			Convey("After inserting two rows", func() {
// 				items := []interface{}{
// 					map[string]interface{}{"id": "1", "name": "First Thing", "kind": "thing", "rating": 4.99},
// 					map[string]interface{}{"id": "2", "name": "Second Thing", "kind": "thing", "rating": 4.99},
// 					map[string]interface{}{"id": "3", "name": "First Something", "kind": "something", "rating": 4.99},
// 				}
// 				_, err := store.SaveAll(collection, items...)
// 				if err != nil {
// 					panic(err)
// 				}
// 				Convey("We can delete two rows using two filters", func() {
// 					// (kind=thing&id=1)&()
// 					err = store.BatchFilterDelete([]map[string]interface{}{
// 						{
// 							"kind": "thing",
// 							"id":   "1",
// 						},
// 						{
// 							"or": map[string]interface{}{
// 								"kind": "something",
// 								"id":   "2",
// 							},
// 						},
// 					}, collection, nil)
// 					if err != nil {
// 						panic(err)
// 					}
// 					Convey("Now the store should contain only entries that dont match the filter", func() {
// 						count, err := store.FilterCount(nil, collection, nil)
// 						if err != nil {
// 							panic(err)
// 						}
// 						So(count, ShouldEqual, int64(1))
// 					})
// 				})
// 			})
// 		})
// 	})
// }
