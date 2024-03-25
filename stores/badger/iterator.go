package badger

import (
	"github.com/dgraph-io/badger/v4"
)

type Iterator struct {
	iterator *badger.Iterator
}

func (i *Iterator) Seek(key []byte) {
	i.iterator.Seek(key)
}

func (i *Iterator) Next() {
	i.iterator.Next()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	ks := i.iterator.Item().Key()
	k := make([]byte, len(ks))
	copy(k, ks)

	return k
}

func (i *Iterator) Value() []byte {
	var val []byte
	i.iterator.Item().Value(func(v []byte) error {
		val = append([]byte{}, v...)
		return nil
	})
	return val
}

func (i *Iterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
