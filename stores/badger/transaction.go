package badger

import (
	"errors"
	"time"

	badgerdb "github.com/dgraph-io/badger/v4"
	common "github.com/osiloke/gostore/common"
)

type BadgerTransaction struct {
	db            *badgerdb.DB
	txn           *badgerdb.Txn
	mode          string
	createdTime   time.Time
	commitedTime  time.Time
	discardedTime time.Time
}

func (t *BadgerTransaction) Restart() error {
	switch t.mode {
	case "update":
		t.txn = t.db.NewTransaction(true)
		t.createdTime = time.Now()
	default:
		return errors.New("unknown transaction mode")
	}
	return nil
}
func (t *BadgerTransaction) Commit() error {
	if err := t.txn.Commit(); err != nil {
		return err
	}
	t.commitedTime = time.Now()
	return nil
}

func (t *BadgerTransaction) Discard() {
	t.txn.Discard()
	t.discardedTime = time.Now()
}

func (t *BadgerTransaction) Set(key []byte, data []byte) error {
	return t.txn.Set(key, data)
}
func (t *BadgerTransaction) Get(key []byte) ([]byte, error) {
	item, err := t.txn.Get(key)
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil, common.ErrNotFound
		}
		return nil, err
	}
	var valCopy []byte
	err = item.Value(func(val []byte) error {
		valCopy = append([]byte{}, val...)
		return nil
	})
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil, common.ErrNotFound
		}
		return nil, err
	}
	if len(valCopy) == 0 {
		return nil, common.ErrNotFound
	}
	return valCopy, nil
}
func (t *BadgerTransaction) Delete(key []byte) error {
	err := t.txn.Delete(key)
	if err != nil {
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return common.ErrNotFound
		}
		return err
	}
	return nil
}
