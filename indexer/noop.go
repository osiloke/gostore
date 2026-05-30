package indexer

import (
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

type NoOpIndexer struct{}

func (n *NoOpIndexer) Index() bleve.Index { return nil }
func (n *NoOpIndexer) BatchIndex() *bleve.Batch { return nil }
func (n *NoOpIndexer) Batch(b *bleve.Batch) error { return nil }
func (n *NoOpIndexer) AddDocumentMapping(name string, dm *mapping.DocumentMapping) {}
func (n *NoOpIndexer) IndexDocument(id string, data interface{}) error { return nil }
func (n *NoOpIndexer) UnIndexDocument(id string) error { return nil }
func (n *NoOpIndexer) QueryMap(q map[string]interface{}, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) Query(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) QueryWithOptions(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) FacetedQuery(q string, facets *Facets, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) QueryWithOptionsHighlighted(q string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) MatchQuery(q, field string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) TermQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) MatchPhraseQuery(q string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) Close() {}

// Implement GeoCapableIndexer as well
func (n *NoOpIndexer) SetField(field string) {}
func (n *NoOpIndexer) GeoDistance(lon, lat float64, distance string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
func (n *NoOpIndexer) GeoDistanceQuery(q string, lon, lat float64, distance string, size, from int, explain bool, fields []string, opts ...RequestOpt) (*bleve.SearchResult, error) {
	return &bleve.SearchResult{}, nil
}
