package indexer

import (
	"fmt"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/moss"
	"github.com/blevesearch/bleve/v2/mapping"
)

// NewMossScorchIndexer creates a new indexer
func NewMossScorchIndexer(indexPath string) (Indexer, bool) {
	indexMapping := bleve.NewIndexMapping()
	return NewMossScorchIndexerWithMapping(indexPath, indexMapping)
}

// NewMossScorchIndexerWithMapping creates a new indexer
func NewMossScorchIndexerWithMapping(indexPath string, indexMapping mapping.IndexMapping) (Indexer, bool) {
	return NewMossScorchIndexerWithConfig(indexPath, indexMapping, nil)
}

// NewMossScorchIndexerWithConfig creates a new Moss-Scorch indexer with a specific KV config map
func NewMossScorchIndexerWithConfig(indexPath string, indexMapping mapping.IndexMapping, kvconfig map[string]interface{}) (Indexer, bool) {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening MossScorch indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new MossScorch index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"

			if kvconfig == nil {
				kvconfig = map[string]interface{}{}
			}
			if _, ok := kvconfig["mossLowerLevelStoreName"]; !ok {
				kvconfig["mossLowerLevelStoreName"] = "mossStore"
			}

			index, err = bleve.NewUsing(indexPath, indexMapping, scorch.Name, moss.Name, kvconfig)

			if err != nil {
				logger.Warn("MossScorch Index could not be created", "path", indexPath, "err", string(err.Error()))
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
	logger.Debug("opening existing MossScorch index", "stats", index.Stats())
	return &DefaultIndexer{index: index}, false
}

// NewMossScorchIndexerWithGeoMapping create a geo capable moss indexer
func NewMossScorchIndexerWithGeoMapping(indexPath, field string, indexMapping mapping.IndexMapping) (Indexer, bool) {
	return NewMossScorchIndexerWithGeoConfig(indexPath, field, indexMapping, nil)
}

// NewMossScorchIndexerWithGeoConfig create a geo capable moss indexer with a specific KV config map
func NewMossScorchIndexerWithGeoConfig(indexPath, field string, indexMapping mapping.IndexMapping, kvconfig map[string]interface{}) (Indexer, bool) {
	index, err := bleve.Open(indexPath)
	if err != nil {
		logger.Debug("Error opening MossScorch indexpath", "path", indexPath, "verbose", string(err.Error()))
		if err == bleve.ErrorIndexMetaMissing || err == bleve.ErrorIndexPathDoesNotExist {
			logger.Debug(fmt.Sprintf("Creating new MossScorch index at %s ...", indexPath))
			// indexMapping.DefaultAnalyzer = "keyword"
			if kvconfig == nil {
				kvconfig = map[string]interface{}{}
			}
			if _, ok := kvconfig["mossLowerLevelStoreName"]; !ok {
				kvconfig["mossLowerLevelStoreName"] = "mossStore"
			}

			index, err = bleve.NewUsing(indexPath, indexMapping, scorch.Name, moss.Name, kvconfig)

			if err != nil {
				logger.Warn("MossScorch Index could not be created", "path", indexPath, "err", string(err.Error()))
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
	logger.Debug("opening existing MossScorch index", "stats", index.Stats())
	return &GeoIndexer{Field: field, Indexer: &DefaultIndexer{index: index}}, false
}

