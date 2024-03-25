package indexer

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	badger "github.com/osiloke/gostore-indexer/badger"

	"github.com/blevesearch/bleve/v2/mapping"
)

// NewIndexer creates a new indexer
func NewBadgerIndexer(indexPath string) Indexer {
	indexMapping := bleve.NewIndexMapping()
	return NewBadgerIndexerWithMapping(indexPath, indexMapping)
}

// NewIndexer creates a new indexer
func NewBadgerIndexerWithMapping(indexPath string, indexMapping mapping.IndexMapping) Indexer {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new badger index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"
			kvconfig := map[string]interface{}{}

			index, err = bleve.NewUsing(indexPath, indexMapping, upsidedown.Name, badger.Name, kvconfig)

			if err != nil {
				logger.Warn("Index could not be created", "path", indexPath, "err", string(err.Error()))
				if err != bleve.ErrorIndexPathExists {
					panic(err)
				}
				return nil
			}

		} else {
			panic(err)
		}
	}
	logger.Debug("opening existing index", "path", indexPath, "stats", index.Stats())
	return &DefaultIndexer{index: index}
}
