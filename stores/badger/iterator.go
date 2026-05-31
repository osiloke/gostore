package badger

import (
	"github.com/dgraph-io/badger/v4"
)

// Iterator wraps a badger.Iterator and the transaction that owns it,
// ensuring that both are properly cleaned up when Close is called.
type Iterator struct {
	iterator *badger.Iterator
	txn      *badger.Txn
	lastErr  error
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
		i.lastErr = err
		return nil
	}
	return v
}

// LastError returns the last error encountered during iteration,
// for example a ValueCopy failure that could not be returned inline.
func (i *Iterator) LastError() error {
	return i.lastErr
}

func (i *Iterator) Valid() bool {
	return i.iterator.Valid()
}

// Close releases the iterator and discards the underlying read transaction,
// freeing the LSM snapshot that was pinned when the cursor was opened.
func (i *Iterator) Close() error {
	i.iterator.Close()
	if i.txn != nil {
		i.txn.Discard()
	}
	return nil
}
