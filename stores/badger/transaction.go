package badger

import (
	"errors"

	badgerdb "github.com/dgraph-io/badger/v4"
)

type BadgerTransaction struct {
	db   *badgerdb.DB
	txn  *badgerdb.Txn
	mode string
}

func (t *BadgerTransaction) Restart() error {
	switch t.mode {
	case "update":
		t.txn = t.db.NewTransaction(true)
	default:
		return errors.New("unknown transaction mode")
	}
	return nil
}
func (t *BadgerTransaction) Commit() error {
	return t.txn.Commit()
}

func (t *BadgerTransaction) Discard() {
	t.txn.Discard()
}

func (t *BadgerTransaction) Set(key []byte, data []byte) error {
	return t.txn.Set(key, data)
}
func (t *BadgerTransaction) Get(key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	var valCopy []byte
	err = item.Value(func(val []byte) error {
		valCopy = append([]byte{}, val...)
		return nil
	})
	return valCopy, err

}
func (t *BadgerTransaction) Delete(key []byte) error {
	return t.txn.Delete(key)
}
