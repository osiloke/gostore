package mongodb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type MongoRows struct {
	mongodb *MongoDBStore
	cursor  *mongo.Cursor
	ctx     context.Context
}

func (m *MongoRows) Next(dst interface{}) (bool, error) {
	var err error
	ok := m.cursor.Next(m.ctx)
	if ok {
		m.cursor.Decode(dst)
		m.mongodb.SetID(dst)
	}
	err = m.cursor.Err()
	return ok, err
}

func (m *MongoRows) NextRaw() ([]byte, bool) {
	ok := m.cursor.Next(m.ctx)
	if ok {
		data := m.cursor.Current
		m.mongodb.SetID(data)
		return data, ok
	}
	return nil, ok
}

func (m *MongoRows) Close() {
	m.cursor.Close(m.ctx)
}

func (m *MongoRows) LastError() error {
	return m.cursor.Err()
}
