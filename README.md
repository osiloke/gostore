GoStore is your applications data swiss knife
======

[![GoDoc](https://godoc.org/github.com/osiloke/gostore?status.svg)](http://godoc.org/github.com/osiloke/gostore)

GoStore is an application storage tool used to speed up your application development. It makes it easier to mix and match existing nosql/some rdbms* databases like RethinkDb, boltdb, leveldb, riak, mongodb, postgres* etc into your application.
It attempts to create a simple way to use different combinations of storage backends in an api.
The idea is to standardize high level database operations that are common to every application.

*RDBMS support is still experimental

**Features:**
- Generalized api for accessing multiple types of databases
- Uses best practices for manipulating data tailored to each type of database
- Application level filtering for databases that dont support filtering (BoltDB)
- Semantic easily understandable api to perform application store actions

NOTE:
Still being developed, use at own risk or when i update this note.

#### Supported Databases
* [RethinkDB](https://github.com/rethinkdb/rethinkdb) - Fully Supported

* [BoltDB](https://github.com/boltdb/bolt) - Partially Supported

* [Scribble](https://github.com/nanobox-io/golang-scribble) - Partially Supported

* [Postgres](https://github.com/postgres/postgres) - Experimental

#### Eventually Supported Databases
* CockroachDb
* MongoDB
* Aerospike
* Couch

## Usage

```
$ go get github.com/osiloke/gostore
```

Gostore tries to simplify your database needs by providing simple specific highlevel database actions which are common to most applications. These actions form the api and are listed below

Api
====

####Database Operations

*	CreateDatabase
* 	CreateTable
* 	GetStore
*	Stats

####Retrieval
*	All
*	AllCursor
*	AllWithinRange
*	Get

####Creation and Updates

*	Save
*	SaveAll
*	Update
*	Replace
*	Delete

####Filtering

*	Since
*	Before
*	FilterSince
*	FilterBefore
*	FilterBeforeCount
*	FilterUpdate
*	FilterReplace
*	FilterGet
*	FilterGetAll
*	FilterDeleter
*	BatchFilterDelete
*	FilterCounts
*	GetByField
*	GetByFieldsByField
*	Close()

## Contributors

### Contributors on GitHub
* [Contributors](https://github.com/osiloke/gostore/graphs/contributors)

## License 
* see [LICENSE](https://github.com/osiloke/gostore/blob/master/LICENSE.md) file

## Version 
* Version 0.1

## Contact
#### Developer/Company
* Homepage: [osiloke.com](http://osiloke.com "Osiloke Blogs Sometimes")
* e-mail: me@osiloke.com
* Twitter: [@osiloke](https://twitter.com/osiloke "osiloke on twitter") 
* Twitter (Again?): [@osilocks](https://twitter.com/osilocks "osiloke on twitter") 

[![Flattr this git repo](http://api.flattr.com/button/flattr-badge-large.png)](https://flattr.com/submit/auto?user_id=lgxkqk&url=https://github.com/osiloke/gostore&title=gostore&language=golang&tags=github&category=software) 
