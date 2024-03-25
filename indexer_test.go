package indexer

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestIndexCreation(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
	})
}

// func TestAddMapping(t *testing.T) {
// 	indexPath := "./test.index"

// 	Convey("Create a new index at "+indexPath, t, func() {
// 		index := NewDefaultIndexer(indexPath)
// 		defer index.Close()
// 		defer os.RemoveAll(indexPath)
// 		Convey("Add mapping", func() {
// 			index.AddStructMapping("food")
// 		})
// 	})
// }

func TestIndexDocument(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}
				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}
			})
		})
	})
}

func TestIndexQuery(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				yam := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy called yam. It is also slimy",
				}
				potato := struct {
					Name        string
					Description string
				}{
					Name:        "potato",
					Description: "A yellow vegetable thats actually starchy called potato.",
				}

				err := index.IndexDocument(yam.Name, yam)
				if err != nil {
					panic(err)
				}
				err = index.IndexDocument(potato.Name, potato)
				if err != nil {
					panic(err)
				}
				Convey("Querying for yam", func() {
					res, err := index.Query("yam white slimy vegetable")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 2)
					So(res.Hits[0].ID, ShouldEqual, "yam")
				})
				Convey("Querying for potato", func() {
					res, err := index.Query("yellow starchy vegetable")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 2)
					So(res.Hits[0].ID, ShouldEqual, "potato")
				})
			})
		})
	})
}

func TestIndexQueryField(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				yam := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy called yam. It is also slimy",
				}
				potato := struct {
					Name        string
					Description string
				}{
					Name:        "potato",
					Description: "A yellow vegetable thats actually starchy called potato.",
				}

				err := index.IndexDocument(yam.Name, yam)
				if err != nil {
					panic(err)
				}
				err = index.IndexDocument(potato.Name, potato)
				if err != nil {
					panic(err)
				}
				Convey("Query document1", func() {
					res, err := index.MatchQuery("A  white vegetable", "Description")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 2)
					So(res.Hits[0].ID, ShouldEqual, "yam")
				})
				Convey("Query document2", func() {
					res, err := index.MatchQuery("A yellow white vegetable", "Description")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 2)
					So(res.Hits[0].ID, ShouldEqual, "potato")
				})
				Convey("Query document3", func() {
					res, err := index.MatchQuery("slimy", "Description")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "yam")
				})
				Convey("Query document4", func() {
					res, err := index.MatchQuery("potato", "Description")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "potato")
				})
			})
		})
	})
}

func TestIndexQueryFieldMaxScore(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Index document", func() {
			questions := []map[string]interface{}{
				{
					"ID":       "1",
					"question": "How are you",
					"answer":   "💃💃💃 I'm feeling great?",
					"action":   "",
					"type":     "root",
				}, {
					"ID":       "2",
					"question": "I want to reset my email password",
					"answer":   "Lets get that done!",
					"action":   "",
					"type":     "root",
				}, {
					"ID":       "3",
					"question": "Setup outlook, android, iOS mail client",
					"answer":   "What is your email address",
					"action":   "",
					"type":     "root",
				}, {
					"ID":       "4",
					"question": "Type your new password",
					"action":   "",
					"next":     "",
				}, {
					"ID":       "5",
					"question": "Confirm your new password",
					"action":   "update_password",
					"next":     "",
				},
			}

			for _, question := range questions {
				err := index.IndexDocument(question["ID"].(string), question)
				if err != nil {
					panic(err)
				}
			}
			Convey("Query document", func() {
				res, err := index.MatchQuery("How are you", "question", ExplainRequest(true))
				if err != nil {
					panic(err)
				}
				So(res.Total, ShouldEqual, 1)
				So(res.Hits[0].ID, ShouldEqual, "1")
			})
		})
	})
}

func TestIndexMultipleObjects(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				food := struct {
					Name        string
					Description string
				}{
					Name:        "yam",
					Description: "A white vegetable thats actually starchy.",
				}

				err := index.IndexDocument(food.Name, food)
				if err != nil {
					panic(err)
				}

				user := struct {
					Name  string
					Email string
				}{
					Name:  "osiloke",
					Email: "osi@emoekpere.org",
				}

				err = index.IndexDocument(user.Name, user)
				if err != nil {
					panic(err)
				}
				Convey("Query document", func() {
					res, err := index.MatchQuery("osi@emoekpere.org", "Email")
					if err != nil {
						panic(err)
					}
					So(res.Total, ShouldEqual, 1)
					So(res.Hits[0].ID, ShouldEqual, "osiloke")
				})
			})
		})
	})
}

func TestIndexer_FacetedSearch(t *testing.T) {
	indexPath := "./test.index"
	os.RemoveAll(indexPath)

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				foods := []struct {
					Name        string `json:"name"`
					Description string `json:"description"`
					Type        string `json:"type"`
				}{
					{
						Name:        "egg",
						Type:        "protein",
						Description: "An egg.",
					},
					{
						Name:        "apple",
						Type:        "fruit",
						Description: "An egg.",
					},
					{
						Name:        "broccoli",
						Type:        "protein",
						Description: "A green edible protein",
					},
					{
						Name:        "plantain",
						Type:        "carb",
						Description: "A starchy stuff.",
					},
					{
						Name:        "yam",
						Type:        "carb",
						Description: "A white vegetable thats actually starchy.",
					},
					{
						Name:        "potato",
						Type:        "carb",
						Description: "A white vegetable thats actually starchy.",
					},
				}
				for _, food := range foods {
					err := index.IndexDocument(food.Name, food)
					if err != nil {
						panic(err)
					}
				}
				Convey("Faceted query search", func() {
					res, err := index.FacetedQuery("*", &Facets{
						Top: map[string]TopFacet{
							"TopTypes": {"TopTypes", "type", 2},
						},
					}, 1, 0, true, []string{"*"})
					if err != nil {
						panic(err)
					}

					So(res.Facets["TopTypes"].Terms.Terms()[0].Term, ShouldEqual, "carb")
					So(res.Facets["TopTypes"].Terms.Terms()[1].Term, ShouldEqual, "protein")
				})
			})
		})
	})
}

type Ratings struct {
	Distance int `json:"distance"`
	Security int `json:"security"`
}

func TestIndexer_FacetedSearchRange(t *testing.T) {
	indexPath := "./test.index"
	os.RemoveAll(indexPath)
	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				reviews := []struct {
					User   string  `json:"user"`
					Place  string  `json:"place"`
					Rating Ratings `json:"ratings"`
				}{
					{
						User:   "kemi",
						Place:  "briggs",
						Rating: Ratings{2, 1},
					},
					{
						User:   "yemi",
						Place:  "briggs",
						Rating: Ratings{3, 3},
					},
					{
						User:   "femi",
						Place:  "briggs",
						Rating: Ratings{1, 3},
					},
					{
						User:   "sean",
						Place:  "briggs",
						Rating: Ratings{3, 3},
					},
					{
						User:   "sean",
						Place:  "tonder",
						Rating: Ratings{2, 2},
					},
					{
						User:   "femi",
						Place:  "tonder",
						Rating: Ratings{2, 2},
					},
				}
				for _, review := range reviews {
					err := index.IndexDocument(review.Place+review.User, review)
					if err != nil {
						panic(err)
					}
				}
				Convey("Faceted query search", func() {
					res, err := index.FacetedQuery("+place=briggs", &Facets{
						Range: map[string]RangeFacet{
							"totalDistanceRating": {
								Field: "ratings.distance",
								Ranges: []interface{}{
									map[string]interface{}{"name": "Low", "min": 1, "max": 2},
									map[string]interface{}{"name": "Mid", "min": 2, "max": 3},
									map[string]interface{}{"name": "High", "min": 3, "max": 4},
								},
							},
						},
					}, 1, 0, true, []string{"*"})
					if err != nil {
						panic(err)
					}
					So(res.Facets["totalDistanceRating"].NumericRanges[0].Count, ShouldEqual, 2)
					So(res.Facets["totalDistanceRating"].NumericRanges[1].Count, ShouldEqual, 1)
					So(res.Facets["totalDistanceRating"].NumericRanges[2].Count, ShouldEqual, 1)
				})
			})
		})
	})
}

func TestIndexer_GeoDistance(t *testing.T) {
	indexPath := "./test.index"
	os.RemoveAll(indexPath)
	Convey("Create a new index at "+indexPath, t, func() {
		index := GeoIndexer{"geo", NewIndexer(indexPath, NewGeoEnabledIndexMapping("geo", "people", "bucket"))}
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				reviews := []struct {
					User   string                 `json:"user"`
					Place  string                 `json:"place"`
					Bucket string                 `json:"bucket"`
					Geo    map[string]interface{} `json:"geo"`
				}{
					{
						User:   "kemi",
						Place:  "briggs",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "APPROXIMATE",
							"lat":      37.5483,
							"lon":      -121.989,
						},
					},
					{
						User:   "yemi",
						Place:  "briggs",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      38.8999,
							"lon":      -77.0272,
						},
					},
					{
						User:   "femi",
						Place:  "briggs",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      37.3775,
							"lon":      -122.03,
						},
					},
					{
						User:   "sean",
						Place:  "briggs",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      38.9911,
							"lon":      -121.988,
						},
					},
					{
						User:   "sean",
						Place:  "tonder",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      37.5441,
							"lon":      -121.988,
						},
					},
					{
						User:   "femi",
						Place:  "tonder",
						Bucket: "people",
						Geo: map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      39.0324,
							"lon":      -77.4097,
						},
					},
				}
				for _, review := range reviews {
					m := map[string]interface{}{
						"user":   review.User,
						"place":  review.Place,
						"bucket": review.Bucket,
						"geo":    review.Geo,
					}
					err := index.IndexDocument(review.Place+review.User, m)
					if err != nil {
						panic(err)
					}
				}
				Convey("Geo Distance search", func() {
					lon, lat := -77.4097, 39.0324
					res, err := index.GeoDistance(lon, lat, "100mi", OrderRequest([]string{"-_score", "-_id"}))
					if err != nil {
						panic(err)
					}
					So(res.Hits.Len(), ShouldEqual, 2)
				})
			})
		})
	})
}

type GeoLocation struct {
	Location map[string]interface{} `json:"location"`
}

func TestIndexer_GeoDistanceQuery(t *testing.T) {
	indexPath := "./test.index"
	// os.RemoveAll(indexPath)
	Convey("Create a new index at "+indexPath, t, func() {
		index := GeoIndexer{"location", NewIndexer(indexPath, NewGeoEnabledIndexMapping("location", "people", "bucket"))}
		// index := NewDefaultIndexer(indexPath)
		defer index.Close()
		// defer os.RemoveAll(indexPath)
		Convey("Add mapping", func() {
			// index.AddStructMapping("food")
			Convey("Index document", func() {
				reviews := []struct {
					User   string      `json:"user"`
					Place  string      `json:"place"`
					Bucket string      `json:"bucket"`
					Geo    GeoLocation `json:"geo"`
				}{
					{
						User:   "kemi",
						Place:  "briggs",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "APPROXIMATE",
							"lat":      37.5483,
							"lon":      -121.989,
						}},
					},
					{
						User:   "yemi",
						Place:  "briggs",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      38.8999,
							"lon":      -77.0272,
						}},
					},
					{
						User:   "femi",
						Place:  "briggs",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      37.3775,
							"lon":      -122.03,
						}},
					},
					{
						User:   "sean",
						Place:  "briggs",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      38.9911,
							"lon":      -121.988,
						}},
					},
					{
						User:   "sean",
						Place:  "tonder",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "ROOFTOP",
							"lat":      37.5441,
							"lon":      -121.988,
						}},
					},
					{
						User:   "osi",
						Place:  "tonder",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      39.0324,
							"lon":      -77.4097,
						}},
					},
					{
						User:   "yemi",
						Place:  "tonder",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      39.0324,
							"lon":      -77.4097,
						}},
					},

					{
						User:   "kemi",
						Place:  "tonder",
						Bucket: "people",
						Geo: GeoLocation{map[string]interface{}{
							"accuracy": "RANGE_INTERPOLATED",
							"lat":      38.9911,
							"lon":      -121.988,
						}},
					},
				}
				for _, review := range reviews {
					m := map[string]interface{}{
						"user":     review.User,
						"place":    map[string]interface{}{"name": review.Place},
						"bucket":   review.Bucket,
						"location": review.Geo.Location,
					}
					err := index.IndexDocument(review.Place+review.User, m)
					if err != nil {
						panic(err)
					}
				}
				Convey("Geo Distance search", func() {
					lon, lat := -77.4097, 39.0324

					// res, err := index.Query("tonder")
					res, err := index.GeoDistanceQuery("+place.name:tonder", lon, lat, "1mi", 20, 0, true, []string{}, OrderRequest([]string{"-_score", "-_id"}))
					if err != nil {
						panic(err)
					}
					So(res.Hits.Len(), ShouldEqual, 2)
				})
			})
		})
	})
}
