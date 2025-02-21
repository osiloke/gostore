package mongodb_test

import (
	"context"
	"testing"

	"github.com/osiloke/gostore/common"
	"github.com/osiloke/gostore/stores/mongodb"
)

func TestSetIDMap(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	data := map[string]interface{}{
		store.IDField: "123",
		"name":        "test",
	}

	err = store.SetID(data)
	if err != nil {
		t.Error(err)
	}

	if data[common.IDField] != "123" {
		t.Errorf("Expected id to be 123, got %v", data[common.IDField])
	}
}
func TestSetIDMapPtr(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	data := map[string]interface{}{
		store.IDField: "123",
		"name":        "test",
	}

	err = store.SetID(&data)
	if err != nil {
		t.Error(err)
	}

	if data[common.IDField] != "123" {
		t.Errorf("Expected id to be 123, got %v", data[common.IDField])
	}
}

func TestSetIDStruct(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type TestStruct struct {
		ID       string `json:"id"`
		ObjectID string `json:"_id"`
		Name     string
	}

	data := TestStruct{
		ObjectID: "456",
		Name:     "test2",
	}

	err = store.SetID(&data)
	if err != nil {
		t.Error(err)
	}

	if data.ID != "456" {
		t.Errorf("Expected id to be 456, got %v", data.ID)
	}
}

func TestSetIDInvalidInput(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	err = store.SetID(123)
	if err == nil {
		t.Error("Expected error for invalid input type, got nil")
	}
}

func TestSetIDNoID(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	type TestStruct struct {
		Name string
	}

	data := TestStruct{
		Name: "test3",
	}

	err = store.SetID(data)
	if err == nil {
		t.Error("Expected error for missing ID field, got nil")
	}
}

func TestSetIDCustomIDKey(t *testing.T) {
	store, err := mongodb.New(context.Background(), "mongodb://localhost:27017", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	store.IDField = "customID"

	type TestStruct struct {
		ID       string `json:"id"`
		CustomID string `json:"customID"`
		Name     string
	}

	data := TestStruct{
		CustomID: "789",
		Name:     "test4",
	}

	err = store.SetID(&data)
	if err != nil {
		t.Error(err)
	}

	if data.ID != "789" {
		t.Errorf("Expected id to be 789, got %v", data.ID)
	}
	if data.CustomID != "" {
		t.Errorf("Expected CustomID to be empty, got %v", data.CustomID)
	}
}
