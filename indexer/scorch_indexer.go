package indexer

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
)

// NewScorchIndexer creates a new indexer
func NewScorchIndexer(indexPath string) (Indexer, bool) {
	indexMapping := bleve.NewIndexMapping()
	return NewScorchIndexerWithMapping(indexPath, indexMapping)
}

// NewScorchIndexerWithMapping creates a new indexer
func NewScorchIndexerWithMapping(indexPath string, indexMapping mapping.IndexMapping) (Indexer, bool) {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening Scorch indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new Scorch index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"

			index, err = bleve.New(indexPath, indexMapping)

			if err != nil {
				logger.Warn("Scorch Index could not be created", "path", indexPath, "err", string(err.Error()))
				if err != bleve.ErrorIndexPathExists {
					panic(err)
				}
				return nil, false
			}
		} else {
			panic(err)
		}
		return &DefaultIndexer{index: index}, true
	}
	logger.Debug("opening existing Scorch index", "stats", index.Stats())
	return &DefaultIndexer{index: index}, false
}

// NewScorchIndexerWithGeoMapping create a geo capable scorch indexer
func NewScorchIndexerWithGeoMapping(indexPath, field string, indexMapping mapping.IndexMapping) (Indexer, bool) {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening Scorch indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new Scorch index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"

			index, err = bleve.New(indexPath, indexMapping)

			if err != nil {
				logger.Warn("Scorch Index could not be created", "path", indexPath, "err", string(err.Error()))
				if err != bleve.ErrorIndexPathExists {
					panic(err)
				}
				return nil, false
			}
		} else {
			panic(err)
		}
		return &GeoIndexer{Field: field, Indexer: &DefaultIndexer{index: index}}, true
	}
	logger.Debug("opening existing Scorch index", "stats", index.Stats())
	return &GeoIndexer{Field: field, Indexer: &DefaultIndexer{index: index}}, false
}
