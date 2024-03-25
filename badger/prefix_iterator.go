package badger

import (
	"bytes"

	"github.com/dgraph-io/badger"
)

type PrefixIterator struct {
	iterator *badger.Iterator
	prefix   []byte
}

func (i *PrefixIterator) Seek(key []byte) {
	if bytes.Compare(key, i.prefix) < 0 {
		i.iterator.Seek(i.prefix)
		return
	}
	i.iterator.Seek(key)
}

func (i *PrefixIterator) Next() {
	i.iterator.Next()
}

func (i *PrefixIterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *PrefixIterator) Key() []byte {
	ks := i.iterator.Item().Key()
	k := make([]byte, len(ks))
	copy(k, ks)

	return k
}

func (i *PrefixIterator) Value() []byte {
	var valCopy []byte
	i.iterator.Item().Value(func(v []byte) error {
		valCopy = append([]byte{}, v...)
		return nil
	})
	return valCopy
}

func (i *PrefixIterator) Valid() bool {
	return i.iterator.ValidForPrefix(i.prefix)
}

func (i *PrefixIterator) Close() error {
	i.iterator.Close()
	return nil
}
