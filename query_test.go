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
						"topActiveCars": TopFacet{Name: "topActiveCars", Field: "car", Count: 5},
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
			`+bucket:store +data.name:/fifty.*/ +data.name:/.*cent/ -data.name:"dollarcent" +data.another:"another"`,
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getQueryValue(tt.args.store, tt.args.k, tt.args.v); got != tt.want {
				t.Errorf("getQueryValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
