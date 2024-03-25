package badger

//TODO: Extract methods into functions
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"time"

	// "fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	badgerdb "github.com/dgraph-io/badger/v4"
	log "github.com/mgutz/logxi/v1"
	common "github.com/osiloke/gostore-common"
	indexer "github.com/osiloke/gostore-indexer"
	"github.com/xiam/to"
)

var logger = log.New("gostore-contrib.badger")

type HasID interface {
	GetId() string
}

// TableConfig cofig for table
type TableConfig struct {
	NestedBucketFieldMatcher map[string]*regexp.Regexp //defines fields to be used to extract nested buckets for data
}

// BadgerStore gostore implementation that used badgerdb
type BadgerStore struct {
	Bucket      []byte
	Db          *badgerdb.DB
	Indexer     indexer.Indexer
	tableConfig map[string]*TableConfig
	t           *time.Ticker
	done        chan bool
}

// IndexedData represents a stored row
type IndexedData struct {
	Bucket  string      `json:"bucket"`
	GeoData interface{} `json:"location,omitempty"`
	Data    interface{} `json:"data"`
}

// Type type o data
func (d *IndexedData) Type() string {
	return "indexed_data"
}

func runValueLogGC(db *badgerdb.DB) {
	// at most do 10 value log gc each time.
	for i := 0; i < 10; i++ {
		err := db.RunValueLogGC(0.5)
		if err != nil {
			if err == badgerdb.ErrNoRewrite {
				// logger.Info("badger has no value log need gc now")
			} else {
				logger.Error("badger run value log gc failed", "err", err)
			}
			return
		}
		// logger.Info("badger run value log gc success")
	}
}

func (s *BadgerStore) setupTicker() {
	done := make(chan bool)
	ticker := time.NewTicker(1 * time.Minute)
	s.done = done
	s.t = ticker
	go func() {
		for range ticker.C {
			runValueLogGC(s.Db)
		}
		done <- true
	}()
	logger.Debug("setup ticker")
}
func NewDBOnly(dbPath string) (s *BadgerStore, err error) {
	opt := BadgerDefaultOptions(dbPath)
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		[]byte("_default"),
		db,
		nil,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	return
}

// New badger store
func New(root string) (s *BadgerStore, err error) {
	dbPath := filepath.Join(root, "db")
	logger.Debug("New badgerdb", "path", dbPath)
	indexPath := filepath.Join(root, "db.index")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {

		os.Mkdir(dbPath, os.FileMode(0600))
		logger.Debug("made badger db", "path", dbPath)
	}

	opt := BadgerDefaultOptions(dbPath)
	// opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	index := indexer.NewIndexer(indexPath, indexMapping)
	s = &BadgerStore{
		[]byte("_default"),
		db,
		index,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	return
}

func ListKeys(db *badgerdb.DB, allVersion bool) error {
	keySize := 0
	valueSize := 0
	keyCount := 0
	err := db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = allVersion
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				valueSize += len(v)
				return nil
			})
			if err != nil {
				return err
			}
			keySize += len(k)
			keyCount++
		}
		return nil
	})
	if err != nil {
		return err
	}
	MB := 1048576.0
	GB := 1073741824.0
	logger.Debug("finish get total badger db size",
		"size(GB)", float64(keySize+valueSize)/GB,
		"all-version", allVersion,
		"count", keyCount,
		"key-size(MB)", float64(keySize)/MB,
		"value-size(GB)", float64(valueSize)/GB,
	)
	return nil
}

// NewWithIndexer New badger store with indexer
func NewWithIndexer(root string, index indexer.Indexer) (s *BadgerStore, err error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0755))
		logger.Debug("created root path " + root)
	}
	dbPath := filepath.Join(root, "db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.Mkdir(dbPath, os.FileMode(0755))
		logger.Debug("created badger directory " + dbPath)
	}

	opt := BadgerDefaultOptions(dbPath)
	db, err := badgerdb.Open(opt)
	if err != nil {
		logger.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		[]byte("_default"),
		db,
		index,
		make(map[string]*TableConfig),
		nil,
		nil,
	}
	s.setupTicker()
	//	e.CreateBucket(bucket)
	return
}

var indexFilenamePrefix = map[string]string{
	"badger":      "badger_",
	"moss":        "moss_",
	"moss-scorch": "moss_scorch_",
	"geo-moss":    "geo_moss_",
}

// NewWithIndex New badger store with indexer
func NewWithIndex(root, index string, indexMapping mapping.IndexMapping, indexOpts ...indexer.IndexOptions) (s *BadgerStore, err error) {
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0755))
		logger.Debug("created root path " + root)
	}
	indexPath := filepath.Join(root, indexFilenamePrefix[index]+"db.index")
	var ix indexer.Indexer
	reIndex := false
	indexInitPath := filepath.Join(root, ".init")
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		reIndex = true
		os.Remove(indexInitPath)
	}
	if _, err := os.Stat(indexInitPath); os.IsNotExist(err) {
		reIndex = true
	}
	if dat, err := os.ReadFile(indexInitPath); err != nil || !strings.HasPrefix(string(dat), index) {
		// initialized index is not the same as current index
		reIndex = true
		logger.Warn("initialized db is different from current", "path", indexInitPath, "init", string(dat), "current", index)
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasSuffix(entry.Name(), "db.index") {
				err := os.RemoveAll(filepath.Join(root, entry.Name()))
				if err != nil {
					return nil, err
				}
				logger.Debug("removing index", "path", entry.Name())
			} else if strings.HasSuffix(entry.Name(), ".init") {
				err := os.RemoveAll(filepath.Join(root, entry.Name()))
				if err != nil {
					return nil, err
				}
				logger.Debug("removing index init", "path", entry.Name())
			}
		}
	}
	if index == "badger" {
		if _, err := os.Stat(indexPath); os.IsNotExist(err) {
			os.Mkdir(indexPath, os.FileMode(0755))
			logger.Debug("made badger db index path", "path", indexPath)
		}
		ix = indexer.NewBadgerIndexerWithMapping(indexPath, indexMapping)
	} else if index == "memory" {
		ix, _ = indexer.NewMemIndexerWithMapping(indexPath, indexMapping)
	} else if index == "moss-scorch" {
		ix, _ = indexer.NewMossScorchIndexerWithMapping(indexPath, indexMapping)
	} else if index == "moss" {
		ix, _ = indexer.NewMossIndexer(indexPath)
	} else if index == "geo-moss" {
		ix, _ = indexer.NewMossIndexerWithMapping(indexPath, indexMapping)
	} else {
		ix = indexer.NewIndexer(indexPath, indexMapping)
	}

	geoIndex := &indexer.GeoIndexer{Field: "_location", Indexer: ix}
	for _, opt := range indexOpts {
		opt(geoIndex)
	}
	s, err = NewWithIndexer(root, geoIndex)
	if reIndex {
		ixj, _ := json.Marshal(ix.Index().Mapping())
		logger.Debug("reindex db", "mapping", string(ixj))
		if err := indexer.ReIndex(s, ix); err != nil {
			return nil, err
		}
		err = ioutil.WriteFile(indexInitPath, []byte(index+"|"+time.Now().UTC().String()), os.ModePerm)
	}
	return
}

func (s *BadgerStore) CreateDatabase() error {
	return nil
}

func (s *BadgerStore) keyForTable(table string) string {
	return "t$" + table
}
func (s *BadgerStore) keyForTableId(table, id string) string {
	return s.keyForTable(table) + "|" + id
}

func (s *BadgerStore) CreateTable(table string, config interface{}) error {
	//config used to configure table
	// if c, ok := config.(map[string]interface{}); ok {
	// 	if nested, ok := c["nested"]; ok {
	// 		nbfm := make(map[string]*regexp.Regexp)
	// 		for k, v := range nested.(map[string]interface{}) {
	// 			nbfm[k] = regexp.MustCompile(v.(string))
	// 		}
	// 		s.tableConfig[table] = &TableConfig{NestedBucketFieldMatcher: nbfm}
	// 	}
	// }
	return nil
}

func (s *BadgerStore) GetStore() interface{} {
	return s.Db
}

// UpdateTransaction starts an update transaction
func (s *BadgerStore) UpdateTransaction() common.Transaction {
	return &BadgerTransaction{s.Db, s.Db.NewTransaction(true), "update"}
}

// FinishTransaction ebds transaction
func (s *BadgerStore) FinishTransaction(tx common.Transaction) error {
	return tx.Commit()
}

func (s *BadgerStore) CreateBucket(bucket string) error {
	return nil
}

func (s *BadgerStore) updateTableStats(table string, change uint) {

}

func (s *BadgerStore) _Get(key, store string) ([][]byte, error) {
	k := s.keyForTableId(store, key)
	storeKey := []byte(k)
	var val []byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		item, err2 := txn.Get(storeKey)
		if err2 != nil {
			logger.Info("error getting key", "store", store, "key", k, "err", err2.Error())
			return err2
		}
		err2 = item.Value(func(v []byte) error {
			val = append([]byte{}, v...)
			return nil
		})
		return err2
	})
	if err != nil {
		if err == badgerdb.ErrKeyNotFound {
			return nil, common.ErrNotFound
		}
		return nil, err
	}
	if len(val) == 0 {
		return nil, common.ErrNotFound
	}
	logger.Debug("_Get success", "key", key, "storeKey", k)
	data := make([][]byte, 2)
	data[0] = []byte(key)
	data[1] = val
	return data, nil
}

// DeleteByPrefix deletes entries by key prefix
// https://github.com/dgraph-io/badger/issues/598
func (s *BadgerStore) DeleteByPrefix(prefix []byte) {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := s.Db.Update(func(txn *badgerdb.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 100000
	s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					panic(err)
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				panic(err)
			}
		}

		return nil
	})
}

// DeleteByPrefix deletes entries by key prefix
// https://github.com/dgraph-io/badger/issues/598
func (s *BadgerStore) FilterDeleteByPrefix(store string) error {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := s.Db.Update(func(txn *badgerdb.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 100000
	b := s.Indexer.BatchIndex()
	prefix := []byte(s.keyForTable(store))
	return s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		keysForDelete := make([][]byte, 0, collectSize)
		keysCollected := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			b.Delete(string(strings.Split(string(key), "|")[1]))
			if keysCollected == collectSize {
				if err := deleteKeys(keysForDelete); err != nil {
					return err
				}
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
				s.Indexer.Batch(b)
			}
		}
		if keysCollected > 0 {
			if err := deleteKeys(keysForDelete); err != nil {
				panic(err)
			}
			s.Indexer.Batch(b)
		}

		return nil
	})
}
func (s *BadgerStore) _Delete(key, store string) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		return txn.Delete(storeKey)
	})
	if err != nil {
		if err == badgerdb.ErrKeyNotFound {
			return common.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *BadgerStore) _Save(key, store string, data []byte) error {
	storeKey := []byte(s.keyForTableId(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		logger.Debug("_Save", "key", key, "store", store, "storeKey", storeKey)
		err := txn.Set(storeKey, data)
		return err
	})
	return err
}

// All retrieves a specified number of key-value pairs from the BadgerDB store
// that match a given prefix and skips a specified number of pairs.
// It returns an ObjectRows interface containing the retrieved key-value pairs
// or an error if the operation fails.
//
// The count parameter determines the maximum number of key-value pairs to retrieve.
// The skip parameter specifies the number of pairs to skip before retrieving.
// The store parameter is used to define a prefix for the keys to match.
//
// The returned ObjectRows interface provides access to the retrieved key-value pairs.
// The entries field of the ObjectRows contains a multi-dimensional byte slice, where
// each element represents a key-value pair. The key is stored in entries[i][0] and the
// value is stored in entries[i][1].
// The length field of the ObjectRows indicates the number of retrieved key-value pairs.
//
// If no key-value pairs match the specified prefix or if the store does not exist,
// the function returns common.ErrNotFound.
//
// Example usage:
//
//	count := 10
//	skip := 0
//	store := "example"
//	rows, err := dataStore.All(count, skip, store)
//	if err != nil {
//	  fmt.Println("Error retrieving data:", err)
//	  return
//	}
//	for i := 0; i < rows.Length(); i++ {
//	  key := rows.Entry(i)[0]
//	  value := rows.Entry(i)[1]
//	  fmt.Printf("Key: %s, Value: %s\n", key, value)
//	}
//
// Note: Make sure to handle errors appropriately when using the returned ObjectRows
// and ensure the BadgerDB database is properly initialized and closed.
func (s *BadgerStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = count / 2
		it := txn.NewIterator(opts)
		defer it.Close()

		var skipCount int
		var rowCount int
		prefix := []byte(s.keyForTable(store))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if skipCount < skip {
				skipCount++
				continue
			}

			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			obj := make([][]byte, 2)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
			obj[1] = make([]byte, len(v))
			copy(obj[1], v)
			objs = append(objs, obj)

			rowCount++
			if rowCount == count {
				break
			}
		}

		return nil
	})

	if len(objs) > 0 {
		return &TransactionRows{entries: objs, length: len(objs)}, err
	}
	return nil, common.ErrNotFound
}

func (s *BadgerStore) GetAll(count int, skip int, bucket []string) (objs [][][]byte, err error) {
	return nil, common.ErrNotImplemented
}

func (s *BadgerStore) _Filter(prefix []byte, count int, skip int, resource string) (objs [][][]byte, err error) {
	return nil, common.ErrNotImplemented
}

func (s *BadgerStore) FilterSuffix(suffix []byte, count int, resource string) (objs [][]byte, err error) {
	return nil, common.ErrNotImplemented
}

func (s *BadgerStore) StreamFilter(key []byte, count int, resource string) chan []byte {
	return nil
}

func (s *BadgerStore) StreamAll(count int, resource string) chan [][]byte {
	return nil
}

func (s *BadgerStore) Stats(bucket string) (data map[string]interface{}, err error) {
	data = make(map[string]interface{})

	return
}

func (s *BadgerStore) Cursor() (common.Iterator, error) {
	rv := Iterator{
		iterator: s.Db.NewTransaction(false).NewIterator(badgerdb.DefaultIteratorOptions),
	}
	rv.iterator.Rewind()
	return &rv, nil
}

func (s *BadgerStore) AllCursor(store string) (common.ObjectRows, error) {
	rows := common.NewCursorRows()
	go func(rows *common.CursorRows) {
		defer func() {
			rows.Done() <- true
		}()
		err := s.Db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := []byte(s.keyForTable(store))
			it.Seek(prefix)
		OUTER:
			for {
				select {
				case <-rows.Exit():
					break OUTER
				case <-rows.NextChan():
				NEXTAGAIN:
					if !it.Valid() || !it.ValidForPrefix(prefix) {
						fmt.Println("invalid")
						continue
					}
					item := it.Item()
					k := item.KeyCopy(nil)
					v, err := item.ValueCopy(nil)
					if err != nil {
						return err
					}

					obj := make([][]byte, 2)
					obj[0] = make([]byte, len(k))
					copy(obj[0], k)
					unsplit := bytes.SplitN(k, []byte("|"), -1)
					k = unsplit[1]
					sn := string(bytes.SplitN(unsplit[0], []byte("$"), -1)[1])

					if sn == store {
						obj[1] = make([]byte, len(v))
						copy(obj[1], v)
						rows.OnNext([][]byte{k, v})
					} else {
						it.Next()
						goto NEXTAGAIN
					}
					it.Next()
				}
			}

			return nil
		})
		if err != nil {
			logger.Error("cursor rows for "+store+" failed", "err", err.Error())
		}
	}(rows)
	return rows, nil
}

func (s *BadgerStore) Stream() (*common.CursorRows, error) {
	return nil, common.ErrNotImplemented
}

func (s *BadgerStore) AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
}

// Since get items after a key
func (s *BadgerStore) Since(id string, count int, skip int, store string) (common.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			obj := make([][]byte, 2)
			err := item.Value(func(v []byte) error {
				obj[1] = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs)}, err
}

// Before Get all recent items from a key
func (s *BadgerStore) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(s.keyForTableId(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			obj := make([][]byte, 2)
			err := item.Value(func(v []byte) error {
				obj[1] = append([]byte{}, v...)
				return nil
			})
			if err != nil {
				return err
			}
			objs = append(objs, obj)
			obj[0] = make([]byte, len(k))
			copy(obj[0], k)
		}
		return nil
	})
	return &TransactionRows{entries: objs, length: len(objs)}, err
} //Get all existing items before a key

func (s *BadgerStore) FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
} //Get all recent items from a key
func (s *BadgerStore) FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	return nil, common.ErrNotImplemented
} //Get all existing items before a key
func (s *BadgerStore) FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (int64, error) {
	return 0, common.ErrNotImplemented
} //Get all existing items before a key

func (s *BadgerStore) Get(key string, store string, dst interface{}) error {
	data, err := s._Get(key, store)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}

func (s *BadgerStore) SaveRaw(key string, val []byte, store string) error {
	if err := s._Save(key, store, val); err != nil {
		return err
	}
	return nil
}
func (s *BadgerStore) Save(key, store string, src interface{}) (string, error) {
	data, err := json.Marshal(src)
	if err != nil {
		return "", err
	}
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Debug("Save", "key", key, "store", store, "storeKey", skey)
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		err := txn.Set(storeKey, data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	})
	return key, err
}

// SaveWithGeo save
func (s *BadgerStore) SaveWithGeo(key, store string, src interface{}, field string) (string, error) {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.keyForTableId(store, key)
		storeKey := []byte(skey)
		logger.Debug("SaveWithGeo", "key", key, "store", store, "storeKey", skey)
		err := s.Db.Update(func(txn *badgerdb.Txn) error {
			if len(field) > 0 {
				geo, err := valForPath(field, srcMap)
				if err == nil {
					srcMap["_location"] = geo

					data, err := json.Marshal(srcMap)
					if err != nil {
						return err
					}
					err = txn.Set(storeKey, data)
					if err != nil {
						return err
					}
					return s.Indexer.IndexDocument(key, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
				}
				return err
			}

			data, err := json.Marshal(src)
			if err != nil {
				return err
			}
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
		})
		return "", err
	}
	return key, errors.New("unable to save")
}

// SaveWithGeoTX save a key within a transaction
func (s *BadgerStore) SaveWithGeoTX(key, store string, src interface{}, field string, txn common.Transaction) error {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.keyForTableId(store, key)
		storeKey := []byte(skey)
		logger.Debug("SaveWithGeoTX", "key", key, "store", store, "storeKey", skey)
		if len(field) > 0 {
			geo, err := valForPath(field, srcMap)
			if err == nil {
				srcMap["_location"] = geo
				data, err := json.Marshal(srcMap)
				if err != nil {
					return err
				}
				err = txn.Set(storeKey, data)
				if err != nil {
					return err
				}
				return s.Indexer.IndexDocument(key, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
			}
			return err

		}

		data, err := json.Marshal(src)
		if err != nil {
			return err
		}
		err = txn.Set(storeKey, data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	}
	return errors.New("unable to save")
}

// SaveTX save a key within a transaction
func (s *BadgerStore) SaveTX(key, store string, src interface{}, txn common.Transaction) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Debug("SaveTX", "key", key, "store", store, "storeKey", skey)
	err = txn.Set(storeKey, data)
	if err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(key, IndexedData{Bucket: store, Data: src})
	return err
}

// GetTX get a key within a transaction
func (s *BadgerStore) GetTX(key string, store string, dst interface{}, txn common.Transaction) error {
	k := s.keyForTableId(store, key)
	storeKey := []byte(k)
	var val []byte
	val, err := txn.Get(storeKey)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return common.ErrNotFound
	}
	logger.Debug("GetTX success", "key", key, "storeKey", k)
	data := make([][]byte, 2)
	data[0] = []byte(key)
	data[1] = val
	if err := json.Unmarshal(data[1], dst); err != nil {
		return err
	}
	return nil
}

func (s *BadgerStore) SaveAll(store string, src ...interface{}) (keys []string, err error) {
	return nil, common.ErrNotImplemented
}
func (s *BadgerStore) Update(key string, store string, src interface{}) error {
	return common.ErrNotImplemented
	// //get existing
	// var existing map[string]interface{}
	// if err := s.Get(key, store, &existing); err != nil {
	// 	return err
	// }

	// logger.Info("update", "Store", store, "data", src, "existing", existing)
	// data, err := json.Marshal(src)
	// if err != nil {
	// 	return err
	// }
	// if err := json.Unmarshal(data, &existing); err != nil {
	// 	return err
	// }
	// data, err = json.Marshal(existing)
	// if err != nil {
	// 	return err
	// }
	// if err := s._Save(key, store, data); err != nil {
	// 	return err
	// }
	// err = s.Indexer.IndexDocument(key, IndexedData{store, existing})
	// return err
}
func (s *BadgerStore) Replace(key string, store string, src interface{}) error {
	_, err := s.Save(key, store, src)
	return err
}
func (s *BadgerStore) ReplaceTX(key string, store string, src interface{}, tx common.Transaction) error {
	return s.SaveTX(key, store, src, tx)
}
func (s *BadgerStore) DeleteTX(key string, store string, tx common.Transaction) error {
	skey := s.keyForTableId(store, key)
	storeKey := []byte(skey)
	logger.Info("DeleteTX", "key", key)
	return tx.Delete(storeKey)
}
func (s *BadgerStore) Delete(key string, store string) error {
	return s._Delete(key, store)
}

// Filter
func (s *BadgerStore) FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}
func (s *BadgerStore) FilterReplace(filter map[string]interface{}, src interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}
func (s *BadgerStore) FilterGet(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions) error {
	logger.Info("FilterGet", "filter", filter, "Store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		var (
			data [][]byte
		)

		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		query := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(query, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Info("FilterGet failed", "query", query)
			return err
		}
		logger.Info("FilterGet success", "query", query)
		if res.Total == 0 {
			logger.Info("FilterGet empty result", "result", res.String())
			return common.ErrNotFound
		}
		data, err = s._Get(res.Hits[0].ID, store)
		if err != nil {
			return err
		}

		err = json.Unmarshal(data[1], dst)
		return err
	}
	return common.ErrNotFound

}
func (s *BadgerStore) FilterGetTX(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions, tx common.Transaction) error {
	logger.Info("FilterGetTX", "filter", filter, "Store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		query := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(query, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Error("FilterGetTX failed", "query", query)
			return err
		}
		if res.Total == 0 {
			logger.Error("FilterGetTX empty result", "result", res.String())
			return common.ErrNotFound
		}
		key := res.Hits[0].ID
		k := s.keyForTableId(store, key)
		storeKey := []byte(k)
		data, err := tx.Get(storeKey)
		if err != nil {
			return err
		}

		err = json.Unmarshal(data, dst)
		return err
	}
	return common.ErrNotFound

}

// FilterGetAll allows you to filter a store if an indexer exists
func (s *BadgerStore) FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {
	if query, ok := filter["q"].(map[string]interface{}); ok {
		q := indexer.GetQueryString(store, query)
		logger.Info("FilterGetAll", "count", count, "skip", skip, "Store", store, "query", q)
		res, err := s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			logger.Warn("err", "error", err, "res")
			return nil, err
		}
		if res.Total == 0 {
			return nil, common.ErrNotFound
		}
		// return NewIndexedBadgerRows(store, res.Total, res, &s), nil
		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, nil
	}
	return nil, common.ErrNotFound
}

func (s *BadgerStore) Query(query, aggregates map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, common.AggregateResult, error) {
	if len(query) > 0 {
		var err error
		var res *bleve.SearchResult
		agg := common.AggregateResult{}
		q := indexer.GetQueryString(store, query)
		var order indexer.RequestOpt
		order = indexer.OrderRequest([]string{"-_score", "-_id"})
		if opts != nil {
			if orderBy := opts.GetOrderBy(); len(orderBy) > 0 {
				order = indexer.OrderRequest((orderBy))
			}
		}
		if len(aggregates) == 0 {
			logger.Info("Query", "count", count, "skip", skip, "Store", store, "query", q, "order", order)
			res, err = s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, order)

		} else {
			facets := indexer.Facets{}
			for k, v := range aggregates {
				if k == "top" && v != nil {
					facets.Top = make(map[string]indexer.TopFacet)
					for kk, vv := range v.(map[string]interface{}) {
						f := vv.(map[string]interface{})
						name := kk
						if n, ok := f["name"].(string); ok {
							name = n
						}
						facets.Top[name] = indexer.TopFacet{
							Name:  f["name"].(string),
							Field: "data." + f["field"].(string),
							Count: int(to.Int64(f["count"])),
						}
					}
				}
				if k == "range" && v != nil {
					facets.Range = make(map[string]indexer.RangeFacet)
					if ranges, ok := v.(map[string]interface{}); ok {
						for kk, vv := range ranges {
							f := vv.(map[string]interface{})
							name := kk
							facets.Range[name] = indexer.RangeFacet{
								Field:  "data." + f["field"].(string),
								Ranges: f["ranges"].([]interface{}),
							}
						}
					} else if ranges, ok := v.([]interface{}); ok {
						for _, vv := range ranges {
							f := vv.(map[string]interface{})
							name := f["name"].(string)
							facets.Range[name] = indexer.RangeFacet{
								Field:  "data." + f["field"].(string),
								Ranges: f["ranges"].([]interface{}),
							}
						}
					}
				}
			}
			logger.Info("Query", "count", count, "skip", skip, "Store", store, "query", q, "facets", facets, "orderBy", order)
			res, err = s.Indexer.FacetedQuery(q, &facets, count, skip, true, []string{}, order)

		}
		if err != nil {
			logger.Warn("err", "error", err)
			return nil, nil, err
		}
		if len(res.Facets) > 0 {
			for k, v := range res.Facets {
				if len(v.NumericRanges) > 0 {
					// numericRanges := make([]interface{}, len(v.NumericRanges))
					// for i, n := range v.NumericRanges{
					// 	numericRanges[i] = map[string]interface{}{"field": n.Name, "min": n.Min, "max": n.Max, "count": n.Count}
					// }
					agg[k] = common.Match{
						NumberRange: v.NumericRanges,
						Field:       strings.SplitN(v.Field, ".", 2)[1],
						Matched:     v.Total,
						UnMatched:   v.Other,
						Missing:     v.Missing,
						DateRange:   search.DateRangeFacet{},
					}
				} else {
					agg[k] = common.Match{
						Top:       v.Terms,
						Field:     strings.SplitN(v.Field, ".", 2)[1],
						Matched:   v.Total,
						UnMatched: v.Other,
						Missing:   v.Missing,
					}
				}
			}
		}
		if res.Total == 0 {
			return nil, agg, common.ErrNotFound
		}

		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, agg, err
	}
	return nil, nil, common.ErrNotFound
}

// GeoQuery query a geocapable indexer
func (s *BadgerStore) GeoQuery(lon, lat float64, distance string, query map[string]interface{}, count int, skip int, store string, opts common.ObjectStoreOptions) (common.ObjectRows, error) {

	var err error
	var res *bleve.SearchResult
	q := "*"
	if len(query) > 0 {
		q = indexer.GetQueryString(store, query)
		// if len(aggregates) == 0 {
	}
	logger.Info("GeoQuery", "count", count, "skip", skip, "Store", store, "lat", lat, "lon", lon, "distance", distance, "query", q)
	if geoIndexer, ok := s.Indexer.(indexer.GeoCapableIndexer); ok {
		res, err = geoIndexer.GeoDistanceQuery(q, lon, lat, distance, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
	} else {
		return nil, common.ErrNotImplemented
	}
	if err != nil {
		logger.Warn("err", "error", err)
		return nil, err
	}
	if res.Total == 0 {
		return nil, common.ErrNotFound
	}

	return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s}, err
}

// FilterDelete filter delete items
func (s *BadgerStore) FilterDelete(query map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	logger.Info("FilterDelete", "filter", query, "store", store)
	count := 1000
	res, err := s.Indexer.Query(indexer.GetQueryString(store, query))
	res, err = s.Indexer.QueryWithOptions(indexer.GetQueryString(store, query), count, 0, true, []string{})
	if err == nil {
		if res.Total == 0 {
			return common.ErrNotFound
		}
		for _, v := range res.Hits {
			err = s._Delete(v.ID, store)
			if err != nil {
				break
			}
			err = s.Indexer.UnIndexDocument(v.ID)
			if err != nil {
				break
			}
		}
	}
	return err
}

func (s *BadgerStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	if query, ok := filter["q"].(map[string]interface{}); ok {
		q := indexer.GetQueryString(store, query)
		logger.Info("FilterCount", "Store", store, "query", q)
		res, err := s.Indexer.Query(indexer.GetQueryString(store, query))
		if err != nil {
			return 0, err
		}
		if res.Total == 0 {
			return 0, common.ErrNotFound
		}
		return int64(res.Total), nil
	}
	return 0, common.ErrNotFound
}

// Misc gets
func (s *BadgerStore) GetByField(name, val, store string, dst interface{}) error { return nil }
func (s *BadgerStore) GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error) {
	return common.ErrNotImplemented
}

func (s *BadgerStore) BatchDelete(ids []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	return common.ErrNotImplemented
}

func (s *BadgerStore) BatchUpdate(id []interface{}, data []interface{}, store string, opts common.ObjectStoreOptions) (err error) {
	// keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		for _, src := range data {
			var key string
			if _v, ok := src.(map[string]interface{}); ok {
				if k, ok := _v["id"].(string); ok {
					key = k
				} else {
					key = common.NewObjectId().String()
					_v["id"] = key
				}
			} else if _v, ok := src.(HasID); ok {
				key = _v.GetId()
			} else {
				key = common.NewObjectId().String()
			}
			data, err := json.Marshal(src)
			if err != nil {
				return err
			}
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			indexedData := map[string]interface{}{"bucket": store, "data": src}
			logger.Debug("BatchInsert", "row", indexedData)
			b.Index(key, indexedData)
		}
		return s.Indexer.Batch(b)
	})
	return
}

func (s *BadgerStore) BatchFilterDelete(filter []map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	return common.ErrNotImplemented
}

func (s *BadgerStore) BatchInsert(data []interface{}, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	// err = s.Db.Update(func(txn *badgerdb.Txn) error {
	// 	for i, src := range data {
	// 		var key string
	// 		if _v, ok := src.(map[string]interface{}); ok {
	// 			if k, ok := _v["id"].(string); ok {
	// 				key = k
	// 			} else {
	// 				key = common.NewObjectId().String()
	// 				_v["id"] = key
	// 			}
	// 		} else if _v, ok := src.(HasID); ok {
	// 			key = _v.GetId()
	// 		} else {
	// 			key = common.NewObjectId().String()
	// 		}
	// 		data, err := json.Marshal(src)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		storeKey := []byte(s.keyForTableId(store, key))
	// 		err = txn.Set(storeKey, data)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		indexedData := IndexedData{store, src}
	// 		logger.Debug("BatchInsert", "row", indexedData)
	// 		b.Index(key, indexedData)
	// 		keys[i] = key
	// 	}
	// 	return s.Indexer.Batch(b)
	// })
	txn := s.Db.NewTransaction(true)
	defer txn.Discard()
	for i, src := range data {
		var key string
		if _v, ok := src.(map[string]interface{}); ok {
			if k, ok := _v["id"].(string); ok {
				key = k
			} else {
				key = common.NewObjectId().String()
				_v["id"] = key
			}
		} else if _v, ok := src.(HasID); ok {
			key = _v.GetId()
		} else {
			key = common.NewObjectId().String()
		}
		data, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}
		storeKey := []byte(s.keyForTableId(store, key))
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		logger.Debug("BatchInsert", "row", indexedData)
		b.Index(key, indexedData)
		keys[i] = key
	}
	if err2 := txn.Commit(); err2 != nil {
		return nil, err2
	}
	err = s.Indexer.Batch(b)
	return
}

func (s *BadgerStore) BatchInsertTX(data []interface{}, store string, opts common.ObjectStoreOptions, txn common.Transaction) (keys []string, err error) {
	keys = make([]string, len(data))
	b := s.Indexer.BatchIndex()
	for i, src := range data {
		var key string
		if _v, ok := src.(map[string]interface{}); ok {
			if k, ok := _v["id"].(string); ok {
				key = k
			} else {
				key = common.NewObjectId().String()
				_v["id"] = key
			}
		} else if _v, ok := src.(HasID); ok {
			key = _v.GetId()
		} else {
			key = common.NewObjectId().String()
		}
		data, err := json.Marshal(src)
		if err != nil {
			return nil, err
		}
		storeKey := []byte(s.keyForTableId(store, key))
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		logger.Debug("BatchInsertTX", "row", indexedData)
		b.Index(key, indexedData)
		keys[i] = key
	}
	if err2 := txn.Commit(); err2 != nil {
		return nil, err2
	}
	if err = txn.Restart(); err != nil {
		return
	}
	err = s.Indexer.Batch(b)
	return
}

func (s *BadgerStore) BatchInsertKVAndIndex(rows [][][]byte, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(rows))
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		b := s.Indexer.BatchIndex()
		for i, row := range rows {
			key := string(row[0])
			data := row[1]
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			var iData map[string]interface{}
			err := json.Unmarshal(row[1], &iData)

			if err != nil {
				return err
			}
			b.Index(key, IndexedData{Bucket: store, Data: iData})
			keys[i] = key
		}
		logger.Debug("copied", "rows", len(keys))
		return s.Indexer.Batch(b)
	})
	return
}
func (s *BadgerStore) BatchInsertKV(rows [][][]byte, store string, opts common.ObjectStoreOptions) (keys []string, err error) {
	keys = make([]string, len(rows))
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		for i, row := range rows {
			key := string(row[0])
			data := row[1]
			storeKey := []byte(s.keyForTableId(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			// dataAsStr := string(data)
			keys[i] = key
		}
		logger.Debug("copied", "rows", len(keys))
		return nil
	})
	return
}
func (s *BadgerStore) Close() {
	defer s.t.Stop()
	if s.Db != nil {
		s.Db.Close()
		logger.Debug("closed badger store")
	}
	if s.Indexer != nil {
		s.Indexer.Close()
		logger.Debug("closed badger index")
	}
}
