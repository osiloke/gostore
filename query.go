package indexer

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2"
)

func reduceValueLenght(v string) string {
	if len(v) > 100 {
		return v[0:100]
	}
	return v
}

func formatted(fieldPrefix, prefix, field string, valRune []rune) (queryString string) {
	v := strings.TrimSpace(string(valRune[1:]))
	v = strings.Replace(v, "\"", "", -1)
	switch strings.ToLower(string(valRune[0])) {
	case "d":
		logger.Debug("date format")
		queryString = fmt.Sprintf(`%sdata.%s:%s"%v"`, fieldPrefix, field, prefix, v)
	case "n":
		queryString = fmt.Sprintf("%sdata.%s:%s=%v", fieldPrefix, field, prefix, v)
	default:
		queryString = fmt.Sprintf("%sdata.%s:%s%v", fieldPrefix, field, prefix, v) //this should not be supported
	}
	return strings.TrimSpace(queryString)
}

func getQueryValue(store, k string, v interface{}) string {
	queryString := ""
	if v == nil {
		return queryString
	}
	if _v, ok := v.(int); ok {
		stringValue := strconv.Itoa(_v)
		queryString = fmt.Sprintf("+data.%s:>=%s", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
	} else if _v, ok := v.(int64); ok {
		stringValue := strconv.FormatInt(_v, 10)
		queryString = fmt.Sprintf("+data.%s:>=%s", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%s", queryString, k, stringValue)
	} else if _v, ok := v.(float64); ok {
		stringValue := strconv.Itoa(int(_v))
		queryString = fmt.Sprintf("+data.%s:>=%v", k, stringValue)
		queryString = fmt.Sprintf("%s +data.%s:<=%v", queryString, k, stringValue)
	} else if vv, ok := v.(string); ok {
		if len(vv) == 0 {
			return queryString
		}
		prefix := "+"
		valRune := []rune(vv)
		if valRune[0] == '\x21' {
			prefix = "-"
			valRune = valRune[1:]
		} else if valRune[0] == 63 {
			prefix = ""
			valRune = valRune[1:]
		} else if valRune[0] == 43 {
			prefix = "+"
			valRune = valRune[1:]
		}
		var first rune
		if len(valRune) > 0 {
			first = valRune[0]
		} else {
			first = 0
		}
		if string(first) == "^" { //match ^ regex
			queryString = fmt.Sprintf(`%sdata.%s:/%v/`, prefix, k, reduceValueLenght(string(valRune[1:])))
		} else if first == '\x3C' {
			if valRune[1] == '\x3A' {
				// something like <:d2016-12-12
				queryString = formatted(prefix, "<", k, valRune[2:])
			} else {
				// something like <1
				v = string(valRune)
				queryString = fmt.Sprintf("%sdata.%s:<=%v", prefix, k, v)
			}
		} else if first == '\x3E' {
			if valRune[1] == '\x3A' {
				// something like >:d2016-12-12
				queryString = formatted(prefix, ">", k, valRune[2:])
			} else {
				// something like >20
				v = string(valRune)
				queryString = fmt.Sprintf("%sdata.%s:>=%v", prefix, k, v)
			}
		} else {
			v = string(valRune)
			queryString = fmt.Sprintf(`%sdata.%s:"%v"`, prefix, k, reduceValueLenght(string(fmt.Sprintf("%v", v))))
		}
	} else if _v, ok := v.(bool); ok {
		queryString = fmt.Sprintf(`+data.%s:"%v"`, k, _v)
	} else {
		logger.Warn(store+" QueryString ["+k+"] was not parsed - defaulting to raw text", "value", v, "type", reflect.TypeOf(v))
		queryString = fmt.Sprintf(`+data.%s:"%v"`, k, v)
	}
	return queryString
}

func GetQueryString(store string, filter map[string]interface{}) string {
	queryString := ""
	for k, v := range filter {
		if _v, ok := v.([]string); ok {
			for _, vv := range _v {
				res := getQueryValue(store, k, vv)
				if len(res) > 0 {
					queryString = strings.TrimSpace(fmt.Sprintf("%s %s", queryString, res))
				}
			}
		} else if _v, ok := v.([]interface{}); ok {
			for _, vv := range _v {
				res := getQueryValue(store, k, vv)
				if len(res) > 0 {
					queryString = strings.TrimSpace(fmt.Sprintf("%s %s", queryString, res))
				}
			}
		} else {
			res := getQueryValue(store, k, v)
			if len(res) > 0 {
				queryString = strings.TrimSpace(fmt.Sprintf("%s %s", queryString, res))
			}
		}
	}
	return strings.TrimSpace(fmt.Sprintf("+bucket:%s %s", store, queryString))
}

func floatVal(v interface{}) float64 {
	if vv, ok := v.(float64); ok {
		return vv
	}
	return float64(v.(int))
}

// AddRangeFacets add range dacets to request
func addRangeFacets(searchRequest *bleve.SearchRequest, facets *Facets) error {
	for k, facet := range facets.Range {
		fieldFacet := bleve.NewFacetRequest(facet.Field, len(facet.Ranges))
		for _, v := range facet.Ranges {
			numericRange := v.(map[string]interface{})
			name := numericRange["name"].(string)
			min := floatVal(numericRange["min"])
			max := floatVal(numericRange["max"])
			fieldFacet.AddNumericRange(name, &min, &max)
		}
		searchRequest.AddFacet(k, fieldFacet)
	}
	return nil
}

// AddFacets facets to a request
func AddFacets(searchRequest *bleve.SearchRequest, facets *Facets) error {
	for _, facet := range facets.Top {
		fieldFacet := bleve.NewFacetRequest(facet.Field, facet.Count)
		searchRequest.AddFacet(facet.Name, fieldFacet)
	}
	addRangeFacets(searchRequest, facets)
	return nil
}
