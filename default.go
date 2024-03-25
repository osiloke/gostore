package indexer

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// Indexer ...
type Indexer interface {
	Index() bleve.Index
	BatchIndex() *bleve.Batch
	Batch(b *bleve.Batch) error
	AddDocumentMapping(name string, dm *mapping.DocumentMapping)
	IndexDocument(id string, data interface{}) error
	UnIndexDocument(id string) error
	QueryMap(q map[string]interface{}, opts ...RequestOpt) (*bleve.SearchResult, error)
	Query(q string, opts ...RequestOpt) (*bleve.SearchResult, error)
	QueryWithOptions(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error)
	FacetedQuery(q string, facets *Facets, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error)
	QueryWithOptionsHighlighted(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error)
	MatchQuery(q, field string, opts ...RequestOpt) (*bleve.SearchResult, error)
	TermQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error)
	MatchPhraseQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error)
	Close()
}

// GeoCapableIndexer an indexer that can makle geo queries
type GeoCapableIndexer interface {
	SetField(field string)
	GeoDistance(lon, lat float64, distance string, opts ...RequestOpt) (*bleve.SearchResult, error)
	GeoDistanceQuery(q string, lon, lat float64, distance string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error)
}

type IndexOptions func(Indexer)

func WithGeoField(field string) IndexOptions {
	return func(index Indexer) {
		index.(GeoCapableIndexer).SetField(field)
	}
}
