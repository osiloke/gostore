package indexer

import (
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/stretchr/testify/assert"
)

func TestAddFacets(t *testing.T) {
	type args struct {
		searchRequest *bleve.SearchRequest
		facets        *Facets
	}
	q := bleve.NewQueryStringQuery("")
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		expected bleve.FacetsRequest
	}{
		{
			"TopFacet",
			args{
				bleve.NewSearchRequest(q),
				&Facets{
					Top: map[string]TopFacet{
						"topActiveCars": {Name: "topActiveCars", Field: "car", Count: 5},
					},
				},
			},
			false,
			bleve.FacetsRequest{"topActiveCars": &bleve.FacetRequest{Field: "car", Size: 5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := AddFacets(tt.args.searchRequest, tt.args.facets); (err != nil) != tt.wantErr {
				t.Errorf("AddFacets() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				assert.Equal(t, tt.expected, tt.args.searchRequest.Facets, "does not contain added facet")
			}
		})
	}
}

// func Test_addRangeFacets(t *testing.T) {
// 	type args struct {
// 		searchRequest *bleve.SearchRequest
// 		facets        *Facets
// 	}
// 	q := bleve.NewQueryStringQuery("")
// 	tests := []struct {
// 		name     string
// 		args     args
// 		wantErr  bool
// 		expected bleve.FacetsRequest
// 	}{
// 		{
// 			"OneRangeFacet",
// 			args{
// 				bleve.NewSearchRequest(q),
// 				&Facets{
// 					Range: map[string]RangeFacet{
// 						"waterRating": {
// 							Field: "ratings.water",
// 							Ranges: []interface{}{
// 								map[string]interface{}{"name": "1", "min": 1, "max": 1},
// 								map[string]interface{}{"name": "2", "min": 2, "max": 2},
// 								map[string]interface{}{"name": "3", "min": 3, "max": 3},
// 							},
// 						},
// 					},
// 				},
// 			},
// 			false,
// 			bleve.FacetsRequest{"waterRating": &bleve.FacetRequest{
// 				Field: "ratings.water",
// 				Size:  3,
// 			}},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			if err := addRangeFacets(tt.args.searchRequest, tt.args.facets); (err != nil) != tt.wantErr {
// 				t.Errorf("addRangeFacets() error = %v, wantErr %v", err, tt.wantErr)
// 			} else {
// 				assert.Equal(t, tt.expected, tt.args.searchRequest.Facets, "does not contain added facet")
// 			}
// 		})
// 	}
// }

func TestGetQueryString(t *testing.T) {
	type args struct {
		store  string
		filter map[string]interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"name": "yall", "empty": ""},
			},
			`+bucket:store +data.name:"yall"`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"empty": ""},
			},
			`+bucket:store`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"empty": []string{""}},
			},
			`+bucket:store`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"empty": []string{"", "no"}},
			},
			`+bucket:store +data.empty:"no"`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"day": "<:d2019-06-11T12:13:43.523888755Z"},
			},
			`+bucket:store +data.day:<"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"testDateLess",
			args{
				"store",
				map[string]interface{}{"day": ">:d2019-06-11T12:13:43.523888755Z"},
			},
			`+bucket:store +data.day:>"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"test date less",
			args{
				"store",
				map[string]interface{}{"name": []string{"^fifty.*", "^.*cent", "!dollarcent"}},
			},
			`+bucket:store +data.name:/fifty.*/ +data.name:/.*cent/ -data.name:"dollarcent"`,
		},
		{
			"test date less",
			args{
				"store",
				map[string]interface{}{"name": []string{"^fifty.*", "^.*cent", "!dollarcent"}, "another": "another"},
			},
			`+bucket:store +data.another:"another" +data.name:/fifty.*/ +data.name:/.*cent/ -data.name:"dollarcent"`,
		},
		{
			"test date less",
			args{
				"store",
				map[string]interface{}{"another": "another", "name": []string{"^fifty.*", "^.*cent", "!dollarcent"}},
			},
			`+bucket:store +data.another:"another" +data.name:/fifty.*/ +data.name:/.*cent/ -data.name:"dollarcent"`,
		},
		{
			"testArrayRegex",
			args{
				"store",
				map[string]interface{}{"name": []string{"^fifty.*", "^.*cent"}},
			},
			`+bucket:store +data.name:/fifty.*/ +data.name:/.*cent/`,
		},
		// 1. Both anchors present (The exact scenario that caused the crash)
		{
			"testRegexBothAnchors",
			args{
				"store",
				map[string]interface{}{"domain": []string{"^.*osi.*$"}},
			},
			`+bucket:store +data.domain:/.*osi.*/`,
		},

		// 2. Start anchor only
		{
			"testRegexStartAnchorOnly",
			args{
				"store",
				map[string]interface{}{"domain": []string{"^.*osi.*"}},
			},
			`+bucket:store +data.domain:/.*osi.*/`,
		},

		// 3. End anchor only — no ^ prefix, so treated as plain text ($ is safe inside quotes)
		{
			"testRegexEndAnchorOnly",
			args{
				"store",
				map[string]interface{}{"domain": []string{".*osi.*$"}},
			},
			`+bucket:store +data.domain:".*osi.*$"`,
		},

		// 4. No anchors — no ^ prefix, treated as plain text
		{
			"testRegexNoAnchors",
			args{
				"store",
				map[string]interface{}{"domain": []string{".*osi.*"}},
			},
			`+bucket:store +data.domain:".*osi.*"`,
		},

		// 5. Array with a mix: only ^ entries are regex, plain $ entries are quoted
		{
			"testArrayMixedRegexAnchors",
			args{
				"store",
				map[string]interface{}{"name": []string{"^start.*", ".*middle.*", ".*end$"}},
			},
			`+bucket:store +data.name:/start.*/ +data.name:".*middle.*" +data.name:".*end$"`,
		},

		// 6. ^ followed immediately by $ — regex with empty body
		{
			"testRegexAnchorsOnly",
			args{
				"store",
				map[string]interface{}{"domain": []string{"^$"}},
			},
			`+bucket:store +data.domain://`,
		},

		// 7. Dollar sign used as currency — must NOT be treated as regex
		{
			"testCurrencyDollarSign",
			args{
				"store",
				map[string]interface{}{"price": "$100"},
			},
			`+bucket:store +data.price:"$100"`,
		},
		{
			"testCurrencyDollarSignInMiddle",
			args{
				"store",
				map[string]interface{}{"label": "USD$50"},
			},
			`+bucket:store +data.label:"USD$50"`,
		},
		{
			"testOptionalFields",
			args{
				"store",
				map[string]interface{}{"color": "?red", "name": "?pink"},
			},
			`+bucket:store data.color:"red" data.name:"pink"`,
		},
		{
			"testORQuerySliceStrings",
			args{
				"store",
				map[string]interface{}{"field": []string{"value1", "value2", "value3"}},
			},
			`+bucket:store +data.field:(value1 OR value2 OR value3)`,
		},
		{
			"testORQuerySliceWithSpaces",
			args{
				"store",
				map[string]interface{}{"field": []string{"value 1", "value 2"}},
			},
			`+bucket:store +data.field:("value 1" OR "value 2")`,
		},
		{
			"testORQuerySliceInterfaces",
			args{
				"store",
				map[string]interface{}{"field": []interface{}{"val1", "val2"}},
			},
			`+bucket:store +data.field:(val1 OR val2)`,
		},
		{
			"testORQuerySliceInts",
			args{
				"store",
				map[string]interface{}{"status": []int{1, 2, 3}},
			},
			`+bucket:store +data.status:(1 OR 2 OR 3)`,
		},
		{
			"testORQueryPrefixOptional",
			args{
				"store",
				map[string]interface{}{"?field": []string{"val1", "val2"}},
			},
			`+bucket:store data.field:(val1 OR val2)`,
		},
		{
			"testORQueryPrefixNegation",
			args{
				"store",
				map[string]interface{}{"!field": []string{"val1", "val2"}},
			},
			`+bucket:store -data.field:(val1 OR val2)`,
		},
		{
			"testORQueryStringDirect",
			args{
				"store",
				map[string]interface{}{"field": "(value1 OR value2 OR value3)"},
			},
			`+bucket:store +data.field:(value1 OR value2 OR value3)`,
		},
		{
			"testORQueryStringWithoutParens",
			args{
				"store",
				map[string]interface{}{"field": "value1 OR value2 OR value3"},
			},
			`+bucket:store +data.field:(value1 OR value2 OR value3)`,
		},
		{
			"testORQueryMultipleFields",
			args{
				"store",
				map[string]interface{}{"category": "books", "field": []string{"v1", "v2"}},
			},
			`+bucket:store +data.category:"books" +data.field:(v1 OR v2)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetQueryString(tt.args.store, tt.args.filter)
			assert.Equal(t, tt.want, got, "GetQueryString() = %v, want %v", got, tt.want)
		})
	}
}

func Test_getQueryValue(t *testing.T) {
	type args struct {
		store string
		k     string
		v     interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"test date less",
			args{
				"store",
				"day",
				"<:d2019-06-11T12:13:43.523888755Z",
			},
			`+data.day:<"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"test date less",
			args{
				"store",
				"day",
				"!<:d2019-06-11T12:13:43.523888755Z",
			},
			`-data.day:<"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"test date less",
			args{
				"store",
				"day",
				"?<:d2019-06-11T12:13:43.523888755Z",
			},
			`data.day:<"2019-06-11T12:13:43.523888755Z"`,
		},
		{
			"test date less",
			args{
				"store",
				"name",
				"^osi",
			},
			`+data.name:/osi/`,
		},
		{
			"test date less",
			args{
				"store",
				"name",
				"?^osi",
			},
			`data.name:/osi/`,
		},
		{
			"test date less",
			args{
				"store",
				"name",
				"!^osi",
			},
			`-data.name:/osi/`,
		},
		{
			"test optional string",
			args{
				"store",
				"color",
				"?red",
			},
			`data.color:"red"`,
		},
		{
			"test currency dollar sign",
			args{
				"store",
				"price",
				"$100",
			},
			`+data.price:"$100"`,
		},
		{
			"test trailing dollar sign plain text",
			args{
				"store",
				"label",
				".*end$",
			},
			`+data.label:".*end$"`,
		},
		{
			"test boolean true",
			args{
				"store",
				"is_active",
				true,
			},
			`+data.is_active:T*`,
		},
		{
			"test boolean false",
			args{
				"store",
				"is_active",
				false,
			},
			`+data.is_active:F*`,
		},
		{
			"test OR string with parens",
			args{
				"store",
				"field",
				"(val1 OR val2)",
			},
			`+data.field:(val1 OR val2)`,
		},
		{
			"test OR string without parens",
			args{
				"store",
				"field",
				"val1 OR val2",
			},
			`+data.field:(val1 OR val2)`,
		},
		{
			"test optional OR string",
			args{
				"store",
				"field",
				"?(val1 OR val2)",
			},
			`data.field:(val1 OR val2)`,
		},
		{
			"test negation OR string",
			args{
				"store",
				"field",
				"!(val1 OR val2)",
			},
			`-data.field:(val1 OR val2)`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getQueryValue(tt.args.store, tt.args.k, tt.args.v); got != tt.want {
				t.Errorf("getQueryValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseQueryAndSearch(t *testing.T) {
	mapping := bleve.NewIndexMapping()
	idx, err := bleve.NewMemOnly(mapping)
	assert.NoError(t, err)

	indexer := &DefaultIndexer{index: idx}

	type Doc struct {
		Bucket string `json:"bucket"`
		Data   struct {
			Field    string `json:"field"`
			Category string `json:"category"`
			Status   int    `json:"status"`
			City     string `json:"city"`
		} `json:"data"`
	}

	doc1 := Doc{Bucket: "products"}
	doc1.Data.Field = "value1"
	doc1.Data.Category = "books"
	doc1.Data.Status = 1
	doc1.Data.City = "New York"
	assert.NoError(t, indexer.IndexDocument("doc1", doc1))

	doc2 := Doc{Bucket: "products"}
	doc2.Data.Field = "value2"
	doc2.Data.Category = "books"
	doc2.Data.Status = 2
	doc2.Data.City = "San Francisco"
	assert.NoError(t, indexer.IndexDocument("doc2", doc2))

	doc3 := Doc{Bucket: "products"}
	doc3.Data.Field = "value3"
	doc3.Data.Category = "electronics"
	doc3.Data.Status = 1
	doc3.Data.City = "Chicago"
	assert.NoError(t, indexer.IndexDocument("doc3", doc3))

	doc4 := Doc{Bucket: "products"}
	doc4.Data.Field = "value4"
	doc4.Data.Category = "books"
	doc4.Data.Status = 3
	doc4.Data.City = "Seattle"
	assert.NoError(t, indexer.IndexDocument("doc4", doc4))

	t.Run("OR query generated from filter slice with colon syntax", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"field": []string{"value1", "value2"},
		})
		assert.Equal(t, "+bucket:products +data.field:(value1 OR value2)", qStr)

		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc1", "doc2"}, hits)
	})

	t.Run("OR query with equal syntax", func(t *testing.T) {
		qStr := "+bucket:products +data.field=(value1 OR value2 OR value3)"
		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc1", "doc2", "doc3"}, hits)
	})

	t.Run("OR query combined with additional criteria", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"field":    []string{"value2", "value3"},
			"category": "books",
		})
		assert.Equal(t, `+bucket:products +data.category:"books" +data.field:(value2 OR value3)`, qStr)

		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), res.Total)
		assert.Equal(t, "doc2", res.Hits[0].ID)
	})

	t.Run("OR query with numbers", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"status": []int{2, 3},
		})
		assert.Equal(t, "+bucket:products +data.status:(2 OR 3)", qStr)

		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc2", "doc4"}, hits)
	})

	t.Run("OR query with phrases containing spaces", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"city": []string{"New York", "San Francisco"},
		})
		assert.Equal(t, `+bucket:products +data.city:("New York" OR "San Francisco")`, qStr)

		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc1", "doc2"}, hits)
	})

	t.Run("QueryWithOptions returns results with OR query", func(t *testing.T) {
		qStr := "+bucket:products +data.field:(value1 OR value2 OR value3)"
		res, err := indexer.QueryWithOptions(qStr, 2, 0, false, []string{"data.field"})
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), res.Total)
		assert.Equal(t, 2, len(res.Hits))
	})

	t.Run("Standalone OR query without bucket prefix", func(t *testing.T) {
		qStr := "+data.field:(value1 OR value2)"
		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc1", "doc2"}, hits)
	})

	t.Run("OR query with negation", func(t *testing.T) {
		qStr := "+bucket:products -data.field:(value1 OR value2)"
		res, err := indexer.Query(qStr)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), res.Total)
		var hits []string
		for _, h := range res.Hits {
			hits = append(hits, h.ID)
		}
		assert.ElementsMatch(t, []string{"doc3", "doc4"}, hits)
	})

	t.Run("Preserves range conditions in slice", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"price": []string{"<:n10", ">:n5"},
		})
		assert.Equal(t, "+bucket:products +data.price:<=10 +data.price:>=5", qStr)
	})

	t.Run("Preserves regex conditions in slice", func(t *testing.T) {
		qStr := GetQueryString("products", map[string]interface{}{
			"name": []string{"^fifty.*", "^.*cent"},
		})
		assert.Equal(t, "+bucket:products +data.name:/fifty.*/ +data.name:/.*cent/", qStr)
	})
}
