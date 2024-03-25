package indexer

import (
	common "github.com/osiloke/gostore-common"
)

// ProviderStore a store which provides data to an index store
type ProviderStore interface {
	Cursor() (common.Iterator, error)
}
