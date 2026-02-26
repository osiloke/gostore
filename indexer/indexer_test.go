package indexer

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	common "github.com/osiloke/gostore/common"
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
				res, err := index.MatchQuery("confirm\\ password", "question")
				if err != nil {
					panic(err)
				}
				So(res.Total, ShouldEqual, 3)
				if res.Total > 0 {
					So(res.Hits[0].ID, ShouldEqual, "5")
				}
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

func TestIndexRegexQuery(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Index document", func() {
			doc := struct {
				Name        string
				Description string
			}{
				Name:        "gostore",
				Description: "A fast storage engine",
			}

			err := index.IndexDocument("1", doc)
			if err != nil {
				panic(err)
			}

			// Add random documents
			for i := 0; i < 10; i++ {
				randomDoc := struct {
					Name        string
					Description string
				}{
					Name:        fmt.Sprintf("item-%d", i),
					Description: fmt.Sprintf("A random item number %d", i),
				}
				err := index.IndexDocument(fmt.Sprintf("random-%d", i), randomDoc)
				if err != nil {
					panic(err)
				}
			}

			Convey("Query with regex", func() {
				res, err := index.Query("Name:/go.*ore/")
				if err != nil {
					panic(err)
				}
				So(res.Total, ShouldEqual, 1)
				So(res.Hits[0].ID, ShouldEqual, "1")
			})
		})
	})
}

func TestIndexRegexEmptyOrValue(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Index documents with different status", func() {
			docs := []struct {
				ID     string
				Status string
			}{
				{ID: "1", Status: "active"},
				{ID: "2", Status: ""},
				{ID: "3", Status: "inactive"},
			}

			for _, doc := range docs {
				err := index.IndexDocument(doc.ID, doc)
				if err != nil {
					panic(err)
				}
			}

			Convey("Query for active or empty status (Expected to fail/error)", func() {
				// Regex to match "active" or an empty string
				// In Bleve, empty strings are generally not indexed as terms.
				_, err := index.Query("Status:/active/ Status:/^$/")
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestIndexOptionalFieldsMatching(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Index documents with different optional fields", func() {
			docs := []struct {
				ID    string
				Type  string
				Color string
			}{
				{ID: "1", Type: "apple", Color: "red"},
				{ID: "2", Type: "banana", Color: "yellow"},
				{ID: "3", Type: "apple", Color: "green"},
			}

			for _, doc := range docs {
				err := index.IndexDocument(doc.ID, doc)
				if err != nil {
					panic(err)
				}
			}

			Convey("Query with two optional fields matching both", func() {
				// Query for Type:apple OR Color:red
				// Document 1 matches both.
				// Document 3 matches apple.
				res, err := index.Query("Type:apple Color:red")
				if err != nil {
					panic(err)
				}

				So(res.Total, ShouldEqual, 2)
				// verify doc 1 is first as it matches both and should have higher score
				So(res.Hits[0].ID, ShouldEqual, "1")
			})

			Convey("Query with two optional fields matching different documents", func() {
				// Query for Color:red OR Color:yellow
				res, err := index.Query("Color:red Color:yellow")
				if err != nil {
					panic(err)
				}

				So(res.Total, ShouldEqual, 2)
				ids := []string{res.Hits[0].ID, res.Hits[1].ID}
				So(ids, ShouldContain, "1")
				So(ids, ShouldContain, "2")
			})
		})
	})
}

func TestIndexOptionalDifferentFieldsMatching(t *testing.T) {
	indexPath := "./test.index"

	Convey("Create a new index at "+indexPath, t, func() {
		index := NewDefaultIndexer(indexPath)
		defer index.Close()
		defer os.RemoveAll(indexPath)
		Convey("Index documents with different fields", func() {
			docs := []struct {
				ID    string
				Name  string
				Color string
			}{
				{ID: "1", Name: "pink panther", Color: "pink"},
				{ID: "2", Name: "red rose", Color: "red"},
				{ID: "3", Name: "pink floyd", Color: "black"},
				{ID: "4", Name: "random floyd", Color: "orange"},
			}

			for _, doc := range docs {
				err := index.IndexDocument(doc.ID, doc)
				if err != nil {
					panic(err)
				}
			}

			Convey("Query for Color:red OR Name:pink", func() {
				// Document 1 matches Name:pink (prefix/term) and Color:pink (not explicitly queried but shows up in text)
				// Wait, the query is "Color:red Name:pink"
				// Document 1 matches Name:pink
				// Document 2 matches Color:red
				// Document 3 matches Name:pink
				res, err := index.Query("Color:red Name:pink")
				if err != nil {
					panic(err)
				}

				So(res.Total, ShouldEqual, 3)

				ids := make([]string, 0)
				for _, hit := range res.Hits {
					ids = append(ids, hit.ID)
				}
				So(ids, ShouldContain, "1")
				So(ids, ShouldContain, "2")
				So(ids, ShouldContain, "3")
			})
		})
	})
}

// ---------------------------------------------------------------------------
// Helpers for TestReIndex
// ---------------------------------------------------------------------------

// mockIterator is a minimal common.Iterator backed by a static slice of
// raw key/value pairs in the "t$<table>|<id>" format used by BadgerStore.
type mockIterator struct {
	rows []mockRow
	pos  int
}

type mockRow struct {
	key []byte
	val []byte
}

func (m *mockIterator) Seek(key []byte) {}
func (m *mockIterator) Next()           { m.pos++ }
func (m *mockIterator) Current() ([]byte, []byte, bool) {
	if m.pos >= len(m.rows) {
		return nil, nil, false
	}
	r := m.rows[m.pos]
	return r.key, r.val, true
}
func (m *mockIterator) Key() []byte {
	if m.pos >= len(m.rows) {
		return nil
	}
	return m.rows[m.pos].key
}
func (m *mockIterator) Value() []byte {
	if m.pos >= len(m.rows) {
		return nil
	}
	return m.rows[m.pos].val
}
func (m *mockIterator) Valid() bool  { return m.pos < len(m.rows) }
func (m *mockIterator) Close() error { return nil }

// mockProvider wraps a mockIterator to satisfy ProviderStore.
type mockProvider struct{ iter *mockIterator }

func (p *mockProvider) Cursor() (common.Iterator, error) { return p.iter, nil }

// ---------------------------------------------------------------------------
// TestReIndex
// ---------------------------------------------------------------------------

func TestReIndex(t *testing.T) {
	Convey("ReIndex", t, func() {
		indexPath := "./test_reindex.index"
		initFile := "./test_reindex.init"
		os.RemoveAll(indexPath)
		os.Remove(initFile)
		defer os.RemoveAll(indexPath)
		defer os.Remove(initFile)

		Convey("indexes all records with a plain Indexer", func() {
			// Two records in two different tables that share the same record ID ("1")
			// to prove document IDs are globally unique (full key, not just "1").
			rows := []mockRow{
				{
					key: []byte("t$users|1"),
					val: mustJSON(map[string]interface{}{"name": "alice", "age": 30}),
				},
				{
					key: []byte("t$orders|1"),
					val: mustJSON(map[string]interface{}{"item": "book", "qty": 2}),
				},
			}
			index := NewDefaultIndexer(indexPath)
			defer index.Close()

			err := ReIndex("badger", initFile, &mockProvider{&mockIterator{rows: rows}}, index)
			So(err, ShouldBeNil)

			Convey("the init file is written with the correct name prefix and count", func() {
				data, readErr := os.ReadFile(initFile)
				So(readErr, ShouldBeNil)
				// Format: "<name>|<UTC timestamp>|<count>"
				So(string(data), ShouldStartWith, "badger|")
				So(string(data), ShouldEndWith, "|2")
			})

			Convey("the users record is findable and its document ID is the full storage key", func() {
				res, qErr := index.Query("alice")
				So(qErr, ShouldBeNil)
				So(res.Total, ShouldBeGreaterThanOrEqualTo, 1)
				So(res.Hits[0].ID, ShouldEqual, "users|1")
			})

			Convey("cross-table records with the same ID do not overwrite each other", func() {
				resUsers, _ := index.Query("alice")
				resOrders, _ := index.Query("book")
				So(resUsers.Total, ShouldBeGreaterThanOrEqualTo, 1)
				So(resOrders.Total, ShouldBeGreaterThanOrEqualTo, 1)
				So(resUsers.Hits[0].ID, ShouldEqual, "users|1")
				So(resOrders.Hits[0].ID, ShouldEqual, "orders|1")
			})
		})

		Convey("promotes _<field> to the top-level field when using GeoIndexer", func() {
			// The raw record stores the geo data under "_location" (leading underscore).
			// ReIndex must strip the underscore and hoist it to "location" at the root
			// so Bleve's geopoint mapping can see it.
			rows := []mockRow{
				{
					key: []byte("t$places|london"),
					val: mustJSON(map[string]interface{}{
						"name":      "london",
						"_location": map[string]interface{}{"lat": 51.5074, "lon": -0.1278},
					}),
				},
			}
			geoIndex := &GeoIndexer{
				Field:   "location",
				Indexer: NewIndexer(indexPath, NewGeoEnabledIndexMapping("location", "places", "bucket")),
			}
			defer geoIndex.Close()

			err := ReIndex("geo-moss", initFile, &mockProvider{&mockIterator{rows: rows}}, geoIndex)
			So(err, ShouldBeNil)

			Convey("init file records a count of 1", func() {
				data, readErr := os.ReadFile(initFile)
				So(readErr, ShouldBeNil)
				So(string(data), ShouldStartWith, "geo-moss|")
				So(string(data), ShouldEndWith, "|1")
			})

			Convey("the document is searchable after geo indexing", func() {
				res, qErr := geoIndex.Query("london")
				So(qErr, ShouldBeNil)
				So(res.Total, ShouldBeGreaterThanOrEqualTo, 1)
				So(res.Hits[0].ID, ShouldEqual, "places|london")
			})
		})

		Convey("skips non-JSON records and does not count them in the init file", func() {
			rows := []mockRow{
				{
					key: []byte("t$users|valid"),
					val: mustJSON(map[string]interface{}{"name": "bob"}),
				},
				{
					key: []byte("t$users|bad"),
					val: []byte("not-json-at-all"),
				},
			}
			index := NewDefaultIndexer(indexPath)
			defer index.Close()

			err := ReIndex("badger", initFile, &mockProvider{&mockIterator{rows: rows}}, index)
			So(err, ShouldBeNil)

			Convey("count in the init file is 1, not 2", func() {
				data, readErr := os.ReadFile(initFile)
				So(readErr, ShouldBeNil)
				So(string(data), ShouldEndWith, "|1")
			})

			Convey("the valid record is still indexed correctly", func() {
				res, qErr := index.Query("bob")
				So(qErr, ShouldBeNil)
				So(res.Total, ShouldBeGreaterThanOrEqualTo, 1)
			})
		})
	})
}

// mustJSON marshals v to JSON and panics on error — test helper only.
func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
