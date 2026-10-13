package indexer

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"
)

func reduceValueLenght(v string) string {
	if len(v) > 100 {
		return v[0:100]
	}
	return v
}

// formatted builds a Bleve query string for a field comparison.
//
// fieldPrefix is the optional query prefix for the field, such as a
// boolean operator. prefix is the comparison operator text (for example
// "<" or ">"). field is the data field name. valRune is the value
// expression as runes, where the first rune indicates the comparison
// value type (for example "d" for date or "n" for numeric) and the
// remaining runes contain the actual value.
//
// Examples:
//
//	formatted("+", "<", "created", []rune("d2016-12-12"))
//	  -> "+data.created:<"2016-12-12""
//	formatted("", ">", "price", []rune("n100"))
//	  -> "data.price:>=100"
//
// The returned query string is trimmed of surrounding spaces.
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
		switch valRune[0] {
		case '\x21':
			prefix = "-"
			valRune = valRune[1:]
		case 63:
			prefix = ""
			valRune = valRune[1:]
		case 43:
			prefix = "+"
			valRune = valRune[1:]
		}
		var first rune
		if len(valRune) > 0 {
			first = valRune[0]
		} else {
			first = 0
		}
		strVal := strings.TrimSpace(string(valRune))
		if isORString(strVal) {
			queryString = fmt.Sprintf("%sdata.%s:(%s)", prefix, k, normalizeORString(strVal))
		} else if string(first) == "^" { // only strings starting with ^ are treated as regex
			// strip the leading ^ and any trailing $ anchor before building the bleve regex query
			queryString = fmt.Sprintf(`%sdata.%s:/%v/`, prefix, k, reduceValueLenght(strings.TrimSuffix(string(valRune[1:]), "$")))
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
		val := "F*"
		if _v {
			val = "T*"
		}
		queryString = fmt.Sprintf(`+data.%s:%s`, k, val)
	} else {
		logger.Warn(store+" QueryString ["+k+"] was not parsed - defaulting to raw text", "value", v, "type", reflect.TypeOf(v))
		queryString = fmt.Sprintf(`+data.%s:"%v"`, k, v)
	}
	return queryString
}

func isORString(s string) bool {
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		inner := strings.TrimSpace(s[1 : len(s)-1])
		return strings.Contains(inner, " OR ") || strings.Contains(inner, " || ")
	}
	return strings.Contains(s, " OR ") || strings.Contains(s, " || ")
}

func normalizeORString(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if strings.Contains(s, " || ") {
		s = strings.ReplaceAll(s, " || ", " OR ")
	}
	return s
}

// isPlainORSlice checks whether a slice contains multiple plain values suitable for an OR query.
// It returns true and the cleaned non-empty string values if all non-empty elements are plain
// (i.e. not starting with range operators '<', '>', regex '^', or prefix modifiers '!', '?').
func isPlainORSlice(v interface{}) (bool, []string) {
	var items []string
	switch s := v.(type) {
	case []string:
		for _, item := range s {
			item = strings.TrimSpace(item)
			if item != "" {
				items = append(items, item)
			}
		}
	case []interface{}:
		for _, item := range s {
			if item == nil {
				continue
			}
			str := strings.TrimSpace(fmt.Sprintf("%v", item))
			if str != "" {
				items = append(items, str)
			}
		}
	case []int:
		for _, item := range s {
			items = append(items, strconv.Itoa(item))
		}
	case []int64:
		for _, item := range s {
			items = append(items, strconv.FormatInt(item, 10))
		}
	default:
		return false, nil
	}

	if len(items) <= 1 {
		return false, items
	}

	for _, item := range items {
		if strings.HasPrefix(item, "<") || strings.HasPrefix(item, ">") ||
			strings.HasPrefix(item, "^") || strings.HasPrefix(item, "!") ||
			strings.HasPrefix(item, "?") {
			return false, items
		}
	}
	return true, items
}

func formatORQuery(k string, values []string) string {
	prefix := "+"
	field := k
	if len(k) > 0 {
		switch k[0] {
		case '!':
			prefix = "-"
			field = k[1:]
		case '?':
			prefix = ""
			field = k[1:]
		case '+':
			prefix = "+"
			field = k[1:]
		}
	}
	terms := make([]string, len(values))
	for i, val := range values {
		val = strings.TrimSpace(val)
		if strings.Contains(val, " ") && !strings.HasPrefix(val, "\"") {
			terms[i] = fmt.Sprintf(`"%s"`, val)
		} else {
			terms[i] = val
		}
	}
	return fmt.Sprintf("%sdata.%s:(%s)", prefix, field, strings.Join(terms, " OR "))
}

func GetQueryString(store string, filter map[string]interface{}) string {
	queryString := ""
	keys := make([]string, 0, len(filter))
	for k := range filter {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := filter[k]
		if ok, items := isPlainORSlice(v); ok {
			res := formatORQuery(k, items)
			if len(res) > 0 {
				queryString = strings.TrimSpace(fmt.Sprintf("%s %s", queryString, res))
			}
		} else if _v, ok := v.([]string); ok {
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

// splitQueryClauses splits a query string into individual clauses, respecting quotes and parentheses.
func splitQueryClauses(q string) []string {
	var clauses []string
	var current strings.Builder
	inQuotes := false
	parenDepth := 0

	for _, r := range q {
		switch {
		case r == '"':
			inQuotes = !inQuotes
			current.WriteRune(r)
		case r == '(' && !inQuotes:
			parenDepth++
			current.WriteRune(r)
		case r == ')' && !inQuotes:
			if parenDepth > 0 {
				parenDepth--
			}
			current.WriteRune(r)
		case unicode.IsSpace(r) && !inQuotes && parenDepth == 0:
			if current.Len() > 0 {
				clauses = append(clauses, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		clauses = append(clauses, current.String())
	}
	return clauses
}

// isGroupedClause checks if a clause is of the form [+-?]field:(...) or [+-?]field=(...)
func isGroupedClause(c string) bool {
	c = strings.TrimSpace(c)
	if strings.HasPrefix(c, "+") || strings.HasPrefix(c, "-") || strings.HasPrefix(c, "?") {
		c = c[1:]
	}
	idx := strings.IndexAny(c, ":=")
	if idx <= 0 {
		return false
	}
	rest := strings.TrimSpace(c[idx+1:])
	return strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")")
}

// parseGroupedClause parses a grouped clause into prefix, field, and the inner content of the parentheses.
func parseGroupedClause(c string) (prefix, field, groupContent string) {
	c = strings.TrimSpace(c)
	prefix = ""
	if strings.HasPrefix(c, "+") {
		prefix = "+"
		c = c[1:]
	} else if strings.HasPrefix(c, "-") {
		prefix = "-"
		c = c[1:]
	} else if strings.HasPrefix(c, "?") {
		prefix = ""
		c = c[1:]
	}
	idx := strings.IndexAny(c, ":=")
	field = c[:idx]
	rest := strings.TrimSpace(c[idx+1:])
	if strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")") {
		groupContent = rest[1 : len(rest)-1]
	}
	return prefix, field, groupContent
}

// parseGroupTokens tokenizes the content inside parentheses into individual values.
func parseGroupTokens(content string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false

	for _, r := range content {
		switch {
		case r == '"':
			inQuotes = !inQuotes
			current.WriteRune(r)
		case !inQuotes && (unicode.IsSpace(r) || r == '|'):
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	var values []string
	for _, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		upper := strings.ToUpper(tok)
		if upper == "OR" || upper == "||" || upper == "|" {
			continue
		}
		values = append(values, tok)
	}
	return values
}

// buildSubQueryForField builds a bleve query for a single value on a specific field.
func buildSubQueryForField(field, val string) query.Query {
	val = strings.TrimSpace(val)
	if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") && len(val) >= 2 {
		unquoted := val[1 : len(val)-1]
		q := bleve.NewMatchPhraseQuery(unquoted)
		q.SetField(field)
		return q
	}
	if f, err := strconv.ParseFloat(val, 64); err == nil {
		inclusive := true
		numQ := bleve.NewNumericRangeInclusiveQuery(&f, &f, &inclusive, &inclusive)
		numQ.SetField(field)
		matchQ := bleve.NewMatchQuery(val)
		matchQ.SetField(field)
		return bleve.NewDisjunctionQuery(matchQ, numQ)
	}
	if strings.HasPrefix(val, "/") && strings.HasSuffix(val, "/") && len(val) >= 2 {
		regQ := bleve.NewRegexpQuery(val[1 : len(val)-1])
		regQ.SetField(field)
		return regQ
	}
	if strings.ContainsAny(val, "*?") {
		wildQ := bleve.NewWildcardQuery(val)
		wildQ.SetField(field)
		return wildQ
	}
	matchQ := bleve.NewMatchQuery(val)
	matchQ.SetField(field)
	return matchQ
}

// ParseQuery parses a query string into a Bleve query, handling grouped OR clauses (e.g. +data.field:(val1 OR val2)).
func ParseQuery(q string) query.Query {
	q = strings.TrimSpace(q)
	if q == "" {
		return bleve.NewQueryStringQuery(q)
	}

	clauses := splitQueryClauses(q)
	hasGroup := false
	for _, c := range clauses {
		if isGroupedClause(c) {
			hasGroup = true
			break
		}
	}
	if !hasGroup {
		return bleve.NewQueryStringQuery(q)
	}

	boolQuery := bleve.NewBooleanQuery()
	var regularClauses []string

	for _, c := range clauses {
		if isGroupedClause(c) {
			prefix, field, groupContent := parseGroupedClause(c)
			tokens := parseGroupTokens(groupContent)
			if len(tokens) > 0 {
				subQueries := make([]query.Query, len(tokens))
				for i, tok := range tokens {
					subQueries[i] = buildSubQueryForField(field, tok)
				}
				disj := bleve.NewDisjunctionQuery(subQueries...)
				switch prefix {
				case "-":
					boolQuery.AddMustNot(disj)
				case "+":
					boolQuery.AddMust(disj)
				default:
					boolQuery.AddShould(disj)
				}
			}
		} else {
			regularClauses = append(regularClauses, c)
		}
	}

	if len(regularClauses) > 0 {
		remQ := bleve.NewQueryStringQuery(strings.Join(regularClauses, " "))
		boolQuery.AddMust(remQ)
	}

	return boolQuery
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
