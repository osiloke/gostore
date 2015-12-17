package gostore

import (
	"os"
	// r "github.com/dancannon/gorethink"
)

// var session *gorethink.Session
var url, url1, url2, url3, db, authKey string

func init() {
	// If the test is being run by wercker look for the rethink url
	url = os.Getenv("RETHINKDB_URL")
	if url == "" {
		url = "localhost:28015"
	}

	url2 = os.Getenv("RETHINKDB_URL_1")
	if url2 == "" {
		url2 = "localhost:28016"
	}

	url2 = os.Getenv("RETHINKDB_URL_2")
	if url2 == "" {
		url2 = "localhost:28017"
	}

	url3 = os.Getenv("RETHINKDB_URL_3")
	if url3 == "" {
		url3 = "localhost:28018"
	}

	db = os.Getenv("RETHINKDB_DB")
	if db == "" {
		db = "test"
	}

	// Needed for running tests for RethinkDB with a non-empty authkey
	authKey = os.Getenv("RETHINKDB_AUTHKEY")
}
