package badger

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/char/regexp"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/analysis/token/lowercase"
	rtoken "github.com/blevesearch/bleve/v2/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/v2/search"
	common "github.com/osiloke/gostore-common"
	indexer "github.com/osiloke/gostore-indexer"
	"github.com/stretchr/testify/assert"
)

var rootPath = "./.testdata"

func init() {
	os.Mkdir(rootPath, 0777)
}

func createDB(name string) *BadgerStore {
	mode := int(0777)
	testDbPath := filepath.Join(rootPath, name)
	indexPath := ""
	dbPath := filepath.Join(testDbPath, "/db")
	os.Mkdir(testDbPath, os.FileMode(mode))
	os.RemoveAll(dbPath)
	os.RemoveAll(indexPath)
	// os.Mkdir(dbPath, os.FileMode(mode))
	// ix, _ := indexer.NewMossIndexer(indexPath)

	indexMapping := bleve.NewIndexMapping()
	rootMapping := bleve.NewDocumentStaticMapping()
	rootMapping.Dynamic = true
	rootMapping.Enabled = true
	dataFieldMapping := bleve.NewDocumentStaticMapping()
	dataFieldMapping.Dynamic = true
	dataFieldMapping.Enabled = true
	schemasFieldMapping := bleve.NewTextFieldMapping()
	schemasFieldMapping.Analyzer = "hyphenated"

	dataFieldMapping.AddFieldMappingsAt("schemas", schemasFieldMapping)
	rootMapping.AddSubDocumentMapping("data", dataFieldMapping)
	indexMapping.StoreDynamic = true
	indexMapping.IndexDynamic = true
	indexMapping.DefaultMapping = rootMapping
	err := indexMapping.AddCustomTokenizer("hyphenatedTokenization",
		map[string]interface{}{
			"type":   rtoken.Name,
			"regexp": `\w+(?:-\w+)+|\w+`,
		})
	if err != nil {
		panic(err)
	}
	err = indexMapping.AddCustomCharFilter("hyphenatedChars",
		map[string]interface{}{
			"type":    regexp.Name,
			"regexp":  `-`,
			"replace": "_",
		})
	if err != nil {
		panic(err)
	}
	err = indexMapping.AddCustomAnalyzer("hyphenated",
		map[string]interface{}{
			"type":      custom.Name,
			"tokenizer": "hyphenatedTokenization",
			"token_filters": []string{
				en.PossessiveName,
				lowercase.Name,
				en.StopName,
				// "edgeNgram325",
			},
			"char_filters": []string{
				"hyphenatedChars",
			},
		})
	if err != nil {
		panic(err)
	}
	_db, err := NewWithIndex(testDbPath, "memory", indexMapping)
	if err != nil {
		panic(err)
	}
	return _db
}

func TestBadgerStore_QueryReturnBestExactMatchFirst(t *testing.T) {

	db := createDB("filterGetAll")
	defer removeDB("filterGetAll", db)
	db.CreateTable("data", nil)
	key := common.NewObjectId().String()
	db.Save(key, "data", map[string]interface{}{"data": map[string]interface{}{
		"id":      key,
		"schemas": "message-callback",
		"count":   10.0,
	}})
	key2 := common.NewObjectId().String()
	db.Save(key2, "data", map[string]interface{}{"data": map[string]interface{}{
		"id":      key2,
		"schemas": "duper-message-callback",
		"count":   10.0,
	}})
	key3 := common.NewObjectId().String()
	db.Save(key3, "data", map[string]interface{}{"data": map[string]interface{}{
		"id":      key3,
		"schemas": "mega-message-callback",
		"count":   10.0,
	}})
	key4 := common.NewObjectId().String()
	db.Save(key4, "data", map[string]interface{}{"data": map[string]interface{}{
		"id":      key4,
		"schemas": "super-duper-message-callback",
		"count":   11.0,
	}})
	key5 := common.NewObjectId().String()
	db.Save(key5, "data", map[string]interface{}{"data": map[string]interface{}{
		"id":      key5,
		"schemas": "mega-super-duper-message-callback",
		"count":   11.0,
	}})
	tests := []struct {
		name string
		s    *BadgerStore
		arg  string
		// want    common.ObjectRows
		wantErr bool
	}{
		{
			"1",
			db,
			"message-callback",
			false,
		},
		{
			"2",
			db,
			"duper-message-callback",
			false,
		},
		{
			"3",
			db,
			"mega-message-callback",
			false,
		},
		{
			"4",
			db,
			"super-duper-message-callback",
			false,
		},
		{
			"5",
			db,
			"mega-super-duper-message-callback",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.s.FilterGetAll(map[string]interface{}{"q": map[string]interface{}{"data.schemas": tt.arg}}, 10, 0, "data", nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.FilterGetAll() %s error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			var dst map[string]interface{}
			ok, _ := got.Next(&dst)
			if ok {
				assert.Equal(t, tt.arg, dst["data"].(map[string]interface{})["schemas"])
			}
		})
	}
}
func createGeoDB(name, geoField, documentName, typefield string) *BadgerStore {
	mode := int(0777)
	testDbPath := filepath.Join(rootPath, name)
	indexPath := filepath.Join(testDbPath, "/db.index")
	dbPath := filepath.Join(testDbPath, "/db")
	os.Mkdir(testDbPath, os.FileMode(mode))
	os.RemoveAll(dbPath)
	os.RemoveAll(indexPath)
	os.Mkdir(dbPath, os.FileMode(mode))
	// ix, _ := indexer.NewMossIndexer(indexPath)
	keywordFieldMapping := bleve.NewKeywordFieldMapping()
	keywordFieldMapping.Analyzer = keyword.Name
	indexMapping := bleve.NewIndexMapping()
	rootMapping := bleve.NewDocumentStaticMapping()
	rootMapping.Dynamic = true
	rootMapping.Enabled = true
	dataFieldMapping := bleve.NewDocumentStaticMapping()
	dataFieldMapping.Dynamic = true
	dataFieldMapping.Enabled = true
	schemasFieldMapping := keywordFieldMapping
	dataFieldMapping.AddFieldMappingsAt("schemas", schemasFieldMapping)
	rootMapping.AddSubDocumentMapping("data", dataFieldMapping)
	indexMapping.DefaultMapping = rootMapping
	locationMapping := bleve.NewGeoPointFieldMapping()
	locationMapping.IncludeTermVectors = true
	locationMapping.IncludeInAll = true
	locationMapping.Index = true
	locationMapping.Store = true
	locationMapping.Type = "geopoint"
	geoMapping := bleve.NewDocumentMapping()
	geoMapping.AddFieldMappingsAt(geoField, locationMapping)
	indexMapping.AddDocumentMapping(documentName, geoMapping)
	indexMapping.TypeField = typefield
	_db, err := NewWithIndex(testDbPath, "memory", indexMapping, indexer.WithGeoField(geoField))
	if err != nil {
		panic(err)
	}
	logger.Info("created geo db")
	return _db
}
func removeDB(name string, db *BadgerStore) {
	if db != nil {
		db.Close()
	}
	os.RemoveAll(filepath.Join(rootPath, name))
}
func TestBadgerStore_Get(t *testing.T) {
	db := createDB("BatchInsert")
	defer removeDB("BatchInsert", db)
	store := "data"
	db.CreateTable(store, nil)
	rows := []interface{}{
		map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"Can retrieve",
			func(t *testing.T) {
				dst := map[string]interface{}{}
				row := rows[0].(map[string]interface{})
				db.Get(row["id"].(string), store, &dst)
				assert.Equal(t, row, dst, "retrieved row is not identical to saved row")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}
func TestBadgerStore_FilterGet(t *testing.T) {
	type args struct {
		filter map[string]interface{}
		store  string
		dst    interface{}
		opts   common.ObjectStoreOptions
	}

	db := createDB("filterGet")
	defer removeDB("filterGet", db)
	db.CreateTable("data", nil)

	key := common.NewObjectId().String()
	osi := map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"count": 10.0,
	}
	db.Save(key, "data", &osi)

	key2 := common.NewObjectId().String()
	tony := map[string]interface{}{
		"id":    key2,
		"name":  "tony emoekpere",
		"count": 11.0,
	}
	db.Save(key2, "data", &tony)

	var dst map[string]interface{}
	tests := []struct {
		name    string
		s       *BadgerStore
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "osiloke"}}, "data", &dst, nil},
			false,
		},
		{
			"not exist",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "unknown"}}, "data", &dst, nil},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.FilterGet(tt.args.filter, tt.args.store, tt.args.dst, tt.args.opts); (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.FilterGet() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(dst["id"], osi["id"]) {
				t.Errorf("BadgerStore.FilterGet() = %v, want %v", dst, osi)
			}
		})
	}
}
func TestBadgerStore_FilterGetAll(t *testing.T) {
	type args struct {
		filter map[string]interface{}
		count  int
		skip   int
		store  string
		opts   common.ObjectStoreOptions
	}

	db := createDB("filterGetAll")
	defer removeDB("filterGetAll", db)
	db.CreateTable("data", nil)
	key := common.NewObjectId().String()
	db.Save(key, "data", map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"count": 10.0,
	})
	key2 := common.NewObjectId().String()
	db.Save(key2, "data", map[string]interface{}{
		"id":    key2,
		"name":  "emike emoekpere",
		"count": 10.0,
	})
	key3 := common.NewObjectId().String()
	db.Save(key3, "data", map[string]interface{}{
		"id":    key3,
		"name":  "oduffa emoekpere",
		"count": 11.0,
	})
	key4 := common.NewObjectId().String()
	db.Save(key4, "data", map[string]interface{}{
		"id":    key4,
		"name":  "tony emoekpere",
		"count": 11.0,
	})
	tests := []struct {
		name string
		s    *BadgerStore
		args args
		// want    common.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "*emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "emike emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "tony emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "oduffa emoekpere"}}, 10, 0, "data", nil},
			false,
		},
		{
			"get item",
			db,
			args{map[string]interface{}{"q": map[string]interface{}{"name": "osiloke emoekpere"}}, 10, 0, "data", nil},
			false,
		},
	}
	tt := tests[0]
	got, err := tt.s.FilterGetAll(tt.args.filter, tt.args.count, tt.args.skip, tt.args.store, tt.args.opts)
	if (err != nil) != tt.wantErr {
		t.Errorf("BadgerStore.FilterGetAll() error = %v, wantErr %v", err, tt.wantErr)
		return
	}
	for _, tt := range tests[1:] {
		t.Run(tt.name, func(t *testing.T) {

			var dst map[string]interface{}
			ok, _ := got.Next(&dst)
			if ok != !tt.wantErr {
				t.Errorf("BadgerStore.FilterGetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// if !reflect.DeepEqual(got, tt.want) {
			// 	t.Errorf("BadgerStore.FilterGetAll() = %v, want %v", got, tt.want)
			// }
		})
	}
}
func TestBadgerStore_Query(t *testing.T) {
	type args struct {
		filter     map[string]interface{}
		aggregates map[string]interface{}
		count      int
		skip       int
		store      string
		opts       common.ObjectStoreOptions
	}

	db := createDB("Query")
	defer removeDB("Query", db)
	db.CreateTable("data", nil)
	key := common.NewObjectId().String()
	db.Save(key, "data", map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"type":  "person",
		"count": "12",
	})
	key2 := common.NewObjectId().String()
	db.Save(key2, "data", map[string]interface{}{
		"id":    key2,
		"name":  "emike emoekpere",
		"type":  "person",
		"count": "10",
	})
	key3 := common.NewObjectId().String()
	db.Save(key3, "data", map[string]interface{}{
		"id":    key3,
		"name":  "oduffa emoekpere",
		"type":  "person",
		"count": "11",
	})
	key4 := common.NewObjectId().String()
	db.Save(key4, "data", map[string]interface{}{
		"id":    key4,
		"name":  "tony emoekpere",
		"type":  "person",
		"count": "11",
	})
	tests := []struct {
		name string
		s    *BadgerStore
		args args
		// want    common.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{
				map[string]interface{}{"type": "person"},
				map[string]interface{}{
					"top": map[string]interface{}{
						"count": map[string]interface{}{
							"name":  "topCount",
							"field": "count",
							"count": 3,
						},
					},
				},
				10,
				0,
				"data",
				nil,
			},
			false,
		},
	}
	tt := tests[0]
	rows, agg, err := tt.s.Query(tt.args.filter, tt.args.aggregates, tt.args.count, tt.args.skip, tt.args.store, tt.args.opts)
	if (err != nil) != tt.wantErr {
		t.Errorf("BadgerStore.Query() error = %v, wantErr %v", err, tt.wantErr)
		return
	}
	tf := &search.TermFacets{}
	tf.Add(&search.TermFacet{Term: "11", Count: 2},
		&search.TermFacet{Term: "10", Count: 1},
		&search.TermFacet{Term: "12", Count: 1},
	)
	logger.Debug("facets", "facets", agg)
	assert.NotNil(t, rows, "rows were empty")
	match := common.Match{
		Field:       "count",
		UnMatched:   0,
		Matched:     4,
		Missing:     0,
		NumberRange: nil,
		DateRange:   nil,
		Top:         tf,
	}
	assert.Equal(t, match, agg["topCount"])
}

func TestBadgerStore_GeoQuery(t *testing.T) {
	type args struct {
		lon      float64
		lat      float64
		distance string
		filter   map[string]interface{}
		count    int
		skip     int
		store    string
		opts     common.ObjectStoreOptions
	}

	db := createGeoDB("GeoQuery", "location", "people", "bucket")
	// defer removeDB("GeoQuery", db)
	db.CreateTable("data", nil)
	key := common.NewObjectId().String()
	_, err := db.SaveWithGeo(key, "people", map[string]interface{}{
		"id":    key,
		"name":  "osiloke emoekpere",
		"type":  "person",
		"mode":  "shirt",
		"count": "12",
		"home": map[string]interface{}{
			"location": map[string]interface{}{
				"accuracy": "APPROXIMATE",
				"lat":      37.5483,
				"lon":      -121.989,
			}},
	}, "home.location")
	if err != nil {
		panic(err)
	}
	key2 := common.NewObjectId().String()
	db.SaveWithGeo(key2, "people", map[string]interface{}{
		"id":    key2,
		"name":  "emike emoekpere",
		"type":  "person",
		"mode":  "shirt",
		"count": "10",
		"home": map[string]interface{}{
			"location": map[string]interface{}{
				"accuracy": "ROOFTOP",
				"lat":      38.8999,
				"lon":      -77.0272,
			}},
	}, "home.location")
	key3 := common.NewObjectId().String()
	db.SaveWithGeo(key3, "people", map[string]interface{}{
		"id":    key3,
		"name":  "oduffa emoekpere",
		"type":  "person",
		"count": "11",
		"mode":  "shirt",
		"home": map[string]interface{}{
			"location": map[string]interface{}{
				"accuracy": "RANGE_INTERPOLATED",
				"lat":      37.3775,
				"lon":      -122.03,
			}},
	}, "home.location")
	key4 := common.NewObjectId().String()
	db.SaveWithGeo(key4, "people", map[string]interface{}{
		"id":    key4,
		"name":  "tony emoekpere",
		"type":  "person",
		"count": "11",
		"mode":  "shirt",
		"home": map[string]interface{}{
			"location": map[string]interface{}{
				"accuracy": "ROOFTOP",
				"lat":      38.9911,
				"lon":      -121.988,
			}},
	}, "home.location")
	tests := []struct {
		name string
		s    *BadgerStore
		args args
		// want    common.ObjectRows
		wantErr bool
	}{
		{
			"get item",
			db,
			args{
				-77.0272, 38.8999,
				"1mi",
				map[string]interface{}{"type": "person"},
				10,
				0,
				"people",
				nil,
			},
			false,
		},
	}
	tt := tests[0]
	rows, err := tt.s.GeoQuery(tt.args.lon, tt.args.lat, tt.args.distance, tt.args.filter, tt.args.count, tt.args.skip, tt.args.store, tt.args.opts)
	if (err != nil) != tt.wantErr {
		t.Errorf("BadgerStore.GeoQuery() error = %v, wantErr %v", err, tt.wantErr)
		return
	}
	t.Log(rows)
	assert.NotNil(t, rows, "rows were empty")
}

func TestBadgerStore_BatchInsert(t *testing.T) {
	db := createDB("BatchInsert")
	defer removeDB("BatchInsert", db)
	store := "data"
	db.CreateTable(store, nil)
	rows := []interface{}{
		map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, map[string]interface{}{
			"id":    common.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	keys, err := db.BatchInsert(rows, store, nil)
	tests := []struct {
		name string
		fn   func(t *testing.T)
	}{
		{
			"No Errors",
			func(t *testing.T) {
				assert.Nil(t, err, "errors while batch inserting")
			},
		},
		{
			"Accurate keys returned",
			func(t *testing.T) {
				assert.Equal(t, len(keys), len(rows), "inconsistency with returned keys count")
			},
		},
		{
			"Query for all keys should match batch insert",
			func(t *testing.T) {
				storedRows, err := db.All(10, 0, store)
				assert.Nil(t, err, "errors while retrieving all entries")

				ix := 0
				for {
					_, ok := storedRows.NextRaw()
					if !ok {
						break
					}
					ix++
				}
				assert.Equal(t, 4, ix, "stored rows inconsistency")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.fn)
	}

}

func TestBadgerStore_SaveTX(t *testing.T) {
	db := createDB("SaveTX")
	defer removeDB("SaveTX", db)
	store := "data"
	db.CreateTable(store, nil)
	rows := []map[string]interface{}{
		{
			"id":    common.NewObjectId().String(),
			"name":  "osiloke emoekpere",
			"count": 10.0,
		}, {
			"id":    common.NewObjectId().String(),
			"name":  "emike emoekpere",
			"count": 10.0,
		}, {
			"id":    common.NewObjectId().String(),
			"name":  "oduffa emoekpere",
			"count": 11.0,
		}, {
			"id":    common.NewObjectId().String(),
			"name":  "tony emoekpere",
			"count": 11.0,
		},
	}
	type args struct {
		key   string
		store string
		src   interface{}
		txn   common.Transaction
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			"saveOne",
			args{
				rows[0]["id"].(string),
				"data",
				rows[0],
				db.UpdateTransaction(),
			},
			rows[0]["id"].(string),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := db.SaveTX(tt.args.key, tt.args.store, tt.args.src, tt.args.txn)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			d := make(map[string]interface{})
			err = db.GetTX(tt.args.key, tt.args.store, &d, tt.args.txn)
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Log(d)
			if (d["id"].(string) != tt.args.key) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", errors.New("item not saved in transaction"), tt.wantErr)
				return
			}
			err = tt.args.txn.Commit()
			if (err != nil) != tt.wantErr {
				t.Errorf("BadgerStore.SaveTX() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			// if got != tt.want {
			// 	t.Errorf("BadgerStore.SaveTX() = %v, want %v", got, tt.want)
			// }
		})
	}
}
