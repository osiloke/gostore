package testing

import (
	"github.com/osiloke/gostore"
	. "github.com/smartystreets/goconvey/convey"
)

func ConveySaveAndGet(collection string, store gostore.ObjectStore) {
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
}
