package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoTransaction struct {
	session mongo.Session
	ctx     context.Context
}

func (m *MongoTransaction) Commit() error {
	return m.session.CommitTransaction(m.ctx)
}

func (m *MongoTransaction) Discard() {
	m.session.AbortTransaction(m.ctx)
}

func (m *MongoTransaction) Set(key []byte, value []byte) error {
	return fmt.Errorf("not implemented")
}

func (m *MongoTransaction) Get(key []byte) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *MongoTransaction) Delete(key []byte) error {
	return fmt.Errorf("not implemented")
}

func (m *MongoTransaction) Restart() error {
	return fmt.Errorf("not implemented")
}
