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
	return i.iterator.Item().KeyCopy(nil)
}

func (i *Iterator) Value() []byte {
	v, err := i.iterator.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}
	return v
}

func (i *Iterator) Valid() bool {
	return i.iterator.Valid()
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
