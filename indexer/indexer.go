package indexer

import (
	"errors"
	"fmt"

	log "github.com/mgutz/logxi/v1"

	"encoding/json"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	// "github.com/blevesearch/blevex/regexp"
)

func ReIndex(provider ProviderStore, index Indexer) error {
	iter, _ := provider.Cursor()
	for iter.Valid() {
		key := iter.Key()
		val := iter.Value()
		var v map[string]interface{}
		if err := json.Unmarshal(val, &v); err == nil {
			k := string(key)
			u := strings.SplitN(k, "|", 2)
			ID := u[1]
			store := strings.TrimPrefix(u[0], "t$")
			// logger.Debug("reindexing", "ID", ID, "val", v)
			if ix, ok := index.(*GeoIndexer); ok {
				d := map[string]interface{}{"bucket": store, "data": v}
				if vv, ok := v["_"+ix.Field]; ok {
					d[ix.Field] = vv
				}
				ix.IndexDocument(ID, d)
			} else {
				index.IndexDocument(ID, IndexedData{store, v})
			}
		}
		iter.Next()
	}
	return nil
}

// IndexedData represents a stored row
type IndexedData struct {
	Bucket string      `json:"bucket"`
	Data   interface{} `json:"data"`
}

var logger = log.New("gostore-contrib.indexer")

type RequestOpt func(*bleve.SearchRequest) error

var OrderRequest = func(orderBy []string) RequestOpt {
	return func(req *bleve.SearchRequest) error {
		req.SortBy(orderBy)
		return nil
	}
}

var ExplainRequest = func(v bool) RequestOpt {
	return func(req *bleve.SearchRequest) error {
		req.Explain = v
		return nil
	}
}

type DefaultIndexer struct {
	index bleve.Index
}

func (i *DefaultIndexer) Index() bleve.Index {
	return i.index
}
func (i *DefaultIndexer) BatchIndex() *bleve.Batch {
	return i.index.NewBatch()
}
func (i *DefaultIndexer) Batch(b *bleve.Batch) error {
	return i.index.Batch(b)
}
func (i *DefaultIndexer) AddDocumentMapping(name string, dm *mapping.DocumentMapping) {
	// i.index.AddDocumentMapping(name, dm)
}

func (i *DefaultIndexer) IndexDocument(id string, data interface{}) error {
	if i.index == nil {
		return errors.New("no index")
	}
	// logger.Debug("Indexing document", "id", id, "data", data)
	return i.index.Index(id, data)
}

func (i *DefaultIndexer) UnIndexDocument(id string) error {
	if i.index == nil {
		return errors.New("no index")
	}
	// logger.Debug("UnIndexing document", "id", id)
	return i.index.Delete(id)
}

func (i *DefaultIndexer) QueryMap(q map[string]interface{}, opts ...RequestOpt) (*bleve.SearchResult, error) {
	queryString := ""
	for k, v := range q {
		queryString = fmt.Sprintf("%s %s:%v", queryString, k, v)
	}
	return i.Query(queryString, opts...)
}
func (i *DefaultIndexer) Query(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	// println(q)
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) QueryWithOptions(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) FacetedQuery(q string, facets *Facets, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	AddFacets(searchRequest, facets)
	return i.index.Search(searchRequest)
}
func (i *DefaultIndexer) QueryWithOptionsHighlighted(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	searchRequest := bleve.NewSearchRequestOptions(query, size, from, explain)
	searchRequest.Highlight = bleve.NewHighlightWithStyle("ansi")
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) MatchQuery(q, field string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewMatchQuery(q)
	query.SetField(field)
	query.SetFuzziness(0)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) TermQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewTermQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) MatchPhraseQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	if i.index == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewMatchPhraseQuery(q)
	searchRequest := bleve.NewSearchRequest(query)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return i.index.Search(searchRequest)
}

func (i *DefaultIndexer) Close() {

	if i.index == nil {
		return
	}
	err := i.index.Close()
	if err != nil {
		logger.Warn("error while closing index")
	}
}

func GetIndex(indexPath string) (bleve.Index, bool) {
	index, err := bleve.Open(indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		logger.Debug("Index path does not exist", "path", "indexPath")
		return nil, false
	}
	return index, true
}
func NewIndexerFromIndex(index bleve.Index) Indexer {
	return &DefaultIndexer{index: index}
}

// NewIndexer creates a new indexer
func NewDefaultIndexer(indexPath string) Indexer {
	indexMapping := bleve.NewIndexMapping()
	indexMapping.StoreDynamic = true
	indexMapping.IndexDynamic = true
	return NewIndexer(indexPath, indexMapping)
}

// NewGeoEnabledIndexMapping creates a new mapping with geo support
func NewGeoEnabledIndexMapping(geoField, documentName, typeField string) mapping.IndexMapping {
	geoMapping := bleve.NewDocumentMapping()
	locationMapping := bleve.NewGeoPointFieldMapping()
	locationMapping.IncludeTermVectors = true
	locationMapping.IncludeInAll = true
	locationMapping.Index = true
	locationMapping.Store = true
	locationMapping.Type = "geopoint"
	geoMapping.AddFieldMappingsAt(geoField, locationMapping)
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = true
	indexMapping.StoreDynamic = true
	indexMapping.AddDocumentMapping(documentName, geoMapping)
	indexMapping.TypeField = typeField
	logger.Debug(fmt.Sprintf("NewGeoEnabledIndexMapping(geoField - %s %s %s)", geoField, documentName, typeField))
	return indexMapping
}

// NewIndexer creates a new indexer
func NewIndexer(indexPath string, indexMapping mapping.IndexMapping) *DefaultIndexer {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"
			index, err = bleve.New(indexPath, indexMapping)
			if err != nil {
				logger.Warn("Index could not be created", "path", indexPath, "err", string(err.Error()))
				if err != bleve.ErrorIndexPathExists {
					panic(err)
				}
				return nil
			}
			return &DefaultIndexer{index: index}
		}
		panic(err)
	}
	return &DefaultIndexer{index: index}
}

// GeoIndexer an indexer that can handle geo queries
type GeoIndexer struct {
	Field string
	Indexer
}

func (g *GeoIndexer) SetField(field string) {
	g.Field = field
}

// GeoDistance get results within a distance from a lon lat
func (g *GeoIndexer) GeoDistance(lon, lat float64, distance string, opts ...RequestOpt) (*bleve.SearchResult, error) {

	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, distance)
	distanceQuery.SetField(g.Field)

	//execute request on index
	searchRequest := bleve.NewSearchRequest(distanceQuery)
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return g.Indexer.Index().Search(searchRequest)
}

// GeoDistanceQuery Geo
func (g *GeoIndexer) GeoDistanceQuery(q string, lon, lat float64, distance string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	//Search the index with GEO //https://github.com/blevesearch/bleve/issues/836
	//https://github.com/blevesearch/bleve/issues/599
	//term query
	if g.Index() == nil {
		return nil, errors.New("no index")
	}
	query := bleve.NewQueryStringQuery(q)
	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, distance)
	distanceQuery.SetField(g.Field)
	// fmt.Println("geofield", g.Field, "query", query, lon, lat, distance)

	//Conjonction of the term and distance queries
	conRequest := bleve.NewConjunctionQuery()
	conRequest.AddQuery(query)
	conRequest.AddQuery(distanceQuery)

	//execute request on index
	searchRequest := bleve.NewSearchRequestOptions(conRequest, size, from, explain)
	if len(fields) > 0 {
		searchRequest.Fields = fields
	}
	for _, opt := range opts {
		if err := opt(searchRequest); err != nil {
			logger.Warn("failed option passed")
		}
	}
	return g.Index().Search(searchRequest)
}
