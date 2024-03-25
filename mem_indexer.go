package indexer

import (
	"github.com/blevesearch/bleve/v2"

	"github.com/blevesearch/bleve/v2/mapping"
)

// NewMemIndexer creates a new indexer
func NewMemIndexer(indexPath string) (Indexer, bool) {
	indexMapping := bleve.NewIndexMapping()
	return NewMemIndexerWithMapping(indexPath, indexMapping)
}

// NewMemIndexerWithMapping creates a new indexer
func NewMemIndexerWithMapping(indexPath string, indexMapping mapping.IndexMapping) (Indexer, bool) {
	// os.RemoveAll(indexPath)
	index, err := bleve.NewMemOnly(indexMapping)
	if err != nil {
		return nil, true
	}
	logger.Debug("opening memory index", "stats", index.Stats())
	return &DefaultIndexer{index: index}, false
}
