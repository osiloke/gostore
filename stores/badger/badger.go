package badger

//TODO: Extract methods into functions
import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	badgerdb "github.com/dgraph-io/badger/v4"
	log "github.com/mgutz/logxi/v1"
	common "github.com/osiloke/gostore/common"
	indexer "github.com/osiloke/gostore/indexer"
	"github.com/xiam/to"
)

type HasID interface {
	GetId() string
}

// TableConfig cofig for table
type TableConfig struct {
	NestedBucketFieldMatcher map[string]*regexp.Regexp //defines fields to be used to extract nested buckets for data
}

// KeyFormat provides a customizable format for storage keys.
type KeyFormat struct {
	TablePrefix string
	IdSeparator string
}

// StoreOpt defines a function that configures a BadgerStore.
type StoreOpt func(*BadgerStore)

// WithKeyFormat is a store option that sets the key format.
func WithKeyFormat(kf KeyFormat) StoreOpt {
	return func(s *BadgerStore) {
		s.KeyFormat = kf
	}
}

// WithReIndexBatchSize is a store option that sets the batch size for re-indexing.
func WithReIndexBatchSize(size int) StoreOpt {
	return func(s *BadgerStore) {
		s.ReIndexBatchSize = size
	}
}

func WithLogger(logger log.Logger) StoreOpt {
	return func(s *BadgerStore) {
		s.Logger = logger
	}
}

// BadgerStore gostore implementation that used badgerdb
type BadgerStore struct {
	Bucket           []byte
	Db               *badgerdb.DB
	Path             string
	Indexer          indexer.Indexer
	IndexType        string
	IndexPath        string
	IndexMapping     mapping.IndexMapping
	tableConfig      map[string]*TableConfig
	t                *time.Ticker
	quit             chan struct{}
	done             chan bool
	KeyFormat        KeyFormat
	ReIndexBatchSize int
	Logger           log.Logger
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

func runValueLogGC(db *badgerdb.DB, logger log.Logger) {
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
	quit := make(chan struct{})
	ticker := time.NewTicker(5 * time.Minute)
	s.done = done
	s.quit = quit
	s.t = ticker
	go func() {
		for {
			select {
			case <-ticker.C:
				runValueLogGC(s.Db, s.Logger)
			case <-quit:
				ticker.Stop()
				done <- true
				return
			}
		}
	}()
	s.Logger.Debug("setup ticker")
}

func (s *BadgerStore) stopTicker() {
	if s.quit != nil {
		close(s.quit)
		<-s.done
		s.quit = nil
	}
}
func NewDBOnly(dbPath string, opts ...StoreOpt) (s *BadgerStore, err error) {
	// Check if database is locked
	if _, err := os.Stat(filepath.Join(dbPath, "LOCK")); err == nil {
		return nil, common.ErrDatabaseLocked
	}

	opt := BadgerDefaultOptions(dbPath)
	db, err := badgerdb.Open(opt)
	if err != nil {
		log.New("gostore-contrib.badger").Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		Bucket:      []byte("_default"),
		Db:          db,
		Path:        dbPath,
		Indexer:     nil,
		tableConfig: make(map[string]*TableConfig),
		KeyFormat: KeyFormat{
			TablePrefix: "t$",
			IdSeparator: "|",
		},
		Logger: log.New("gostore-contrib.badger"),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.setupTicker()
	return
}

// NewRestorable creates a new BadgerStore specifically for restoration purposes.
// It opens the database with restore-specific options and does not initialize an indexer.
func NewRestorable(root string, opts ...StoreOpt) (s *BadgerStore, err error) {
	// Use restore options for opening the database
	dbPath := filepath.Join(root, "db")
	l := log.New("gostore-contrib.badger")
	l.Debug("New badgerdb", "path", dbPath)
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {

		os.Mkdir(dbPath, os.FileMode(0700))
		l.Debug("made badger db", "path", dbPath)
	}

	// Check if database is locked
	if _, err := os.Stat(filepath.Join(dbPath, "LOCK")); err == nil {
		return nil, common.ErrDatabaseLocked
	}
	opt := BadgerRestoreOptions(dbPath)
	db, err := badgerdb.Open(opt)
	if err != nil {
		log.New("gostore-contrib.badger").Error("unable to create badgerdb for restore", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		Bucket:      []byte("_default"),
		Db:          db,
		Path:        dbPath,
		Indexer:     nil, // Explicitly no indexer for restore
		tableConfig: make(map[string]*TableConfig),
		KeyFormat: KeyFormat{
			TablePrefix: "t$",
			IdSeparator: "|",
		},
		Logger: log.New("gostore-contrib.badger"),
	}
	for _, opt := range opts {
		opt(s)
	}
	// No ticker setup during restore
	return
}

// New badger store
func New(root string, opts ...StoreOpt) (s *BadgerStore, err error) {
	dbPath := filepath.Join(root, "db")
	l := log.New("gostore-contrib.badger")
	l.Debug("New badgerdb", "path", dbPath)
	indexPath := filepath.Join(root, "db.index")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {

		os.Mkdir(dbPath, os.FileMode(0700))
		l.Debug("made badger db", "path", dbPath)
	}

	// Check if database is locked
	if _, err := os.Stat(filepath.Join(dbPath, "LOCK")); err == nil {
		return nil, common.ErrDatabaseLocked
	}

	opt := BadgerDefaultOptions(dbPath)
	// opt.SyncWrites = true
	db, err := badgerdb.Open(opt)
	if err != nil {
		log.New("gostore-contrib.badger").Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	indexMapping := bleve.NewIndexMapping()
	indexMapping.IndexDynamic = false
	indexMapping.StoreDynamic = false
	index := indexer.NewIndexer(indexPath, indexMapping)
	s = &BadgerStore{
		Bucket:       []byte("_default"),
		Db:           db,
		Path:         dbPath,
		Indexer:      index,
		IndexPath:    indexPath,
		IndexMapping: indexMapping,
		IndexType:    "bleve",
		tableConfig:  make(map[string]*TableConfig),
		KeyFormat: KeyFormat{
			TablePrefix: "t$",
			IdSeparator: "|",
		},
		Logger: l,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.setupTicker()
	return
}

func ListKeys(db *badgerdb.DB, allVersion bool, logger log.Logger) error {
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
func NewWithIndexer(root string, index indexer.Indexer, opts ...StoreOpt) (s *BadgerStore, err error) {
	l := log.New("gostore-contrib.badger")
	if _, err := os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0700))
		l.Debug("created root path " + root)
	}
	dbPath := filepath.Join(root, "db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		os.Mkdir(dbPath, os.FileMode(0700))
		l.Debug("created badger directory " + dbPath)
	}

	// Check if database is locked
	if _, err := os.Stat(filepath.Join(dbPath, "LOCK")); err == nil {
		return nil, common.ErrDatabaseLocked
	}

	opt := BadgerDefaultOptions(dbPath)
	db, err := badgerdb.Open(opt)
	if err != nil {
		l.Error("unable to create badgerdb", "err", err.Error(), "opt", opt)
		return
	}
	s = &BadgerStore{
		Bucket:      []byte("_default"),
		Db:          db,
		Path:        dbPath,
		Indexer:     index,
		tableConfig: make(map[string]*TableConfig),
		KeyFormat: KeyFormat{
			TablePrefix: "t$",
			IdSeparator: "|",
		},
		Logger: l,
	}
	for _, opt := range opts {
		opt(s)
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
func NewWithIndex(root, index string, indexMapping mapping.IndexMapping, indexOpts []indexer.IndexOptions, storeOpts ...StoreOpt) (s *BadgerStore, err error) {

	if _, err = os.Stat(root); os.IsNotExist(err) {
		os.Mkdir(root, os.FileMode(0700))
		log.New("gostore-contrib.badger").Debug("created root path " + root)
	}
	dbPath := filepath.Join(root, "db")
	// Check if database is locked
	if _, err = os.Stat(filepath.Join(dbPath, "LOCK")); err == nil {
		return nil, common.ErrDatabaseLocked
	}
	indexPath := filepath.Join(root, indexFilenamePrefix[index]+"db.index")
	var ix indexer.Indexer
	reIndex := false
	indexInitFilePath := filepath.Join(root, ".init")
	if _, err = os.Stat(indexPath); os.IsNotExist(err) {
		reIndex = true
		os.Remove(indexInitFilePath)
	}
	if _, err = os.Stat(indexInitFilePath); os.IsNotExist(err) {
		reIndex = true
	}
	var dat []byte
	if dat, err = os.ReadFile(indexInitFilePath); err != nil || !strings.HasPrefix(string(dat), index) {
		// initialized index is not the same as current index
		reIndex = true
		log.New("gostore-contrib.badger").Warn("initialized db is different from current", "path", indexInitFilePath, "init", string(dat), "current", index)
		var entries []os.DirEntry
		entries, err = os.ReadDir(root)
		if err != nil {
			return
		}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasSuffix(entry.Name(), "db.index") {
				err = os.RemoveAll(filepath.Join(root, entry.Name()))
				if err != nil {
					return nil, err
				}
				log.New("gostore-contrib.badger").Debug("removing index", "path", entry.Name())
			} else if strings.HasSuffix(entry.Name(), ".init") {
				err = os.RemoveAll(filepath.Join(root, entry.Name()))
				if err != nil {
					return nil, err
				}
				log.New("gostore-contrib.badger").Debug("removing index init", "path", entry.Name())
			}
		}
	}
	switch index {
	case "badger":
		if _, osErr := os.Stat(indexPath); os.IsNotExist(osErr) {
			os.Mkdir(indexPath, os.FileMode(0700))
			log.New("gostore-contrib.badger").Debug("made badger db index path", "path", indexPath)
		}
		ix = indexer.NewBadgerIndexerWithMapping(indexPath, indexMapping)
	case "memory":
		ix, _ = indexer.NewMemIndexerWithMapping(indexPath, indexMapping)
	case "moss-scorch":
		ix, _ = indexer.NewMossScorchIndexerWithMapping(indexPath, indexMapping)
	case "moss":
		ix, _ = indexer.NewMossIndexer(indexPath)
	case "geo-moss":
		ix, _ = indexer.NewMossIndexerWithMapping(indexPath, indexMapping)
	default:
		ix = indexer.NewIndexer(indexPath, indexMapping)
	}

	geoIndex := &indexer.GeoIndexer{Field: "_location", Indexer: ix}
	for _, opt := range indexOpts {
		opt(geoIndex)
	}
	s, err = NewWithIndexer(root, geoIndex, storeOpts...)
	if err != nil {
		return
	}
	s.Logger.Debug("opened badger store with indexer", "root", root, "index", index, "reIndex", reIndex)

	if reIndex {
		ixj, _ := json.Marshal(ix.Index().Mapping())
		s.Logger.Debug("reindex db", "mapping", string(ixj))
		var reindexOpts []indexer.ReIndexOption
		if s.ReIndexBatchSize > 0 {
			reindexOpts = append(reindexOpts, indexer.WithBatchSize(s.ReIndexBatchSize))
		}
		s.Logger.Debug("starting reindex", "batchSize", s.ReIndexBatchSize)
		if err = indexer.ReIndex(index, indexInitFilePath, s, ix, reindexOpts...); err != nil {
			return
		}
		s.Logger.Debug("reindex completed successfully")
	}
	s.IndexType = index
	s.IndexPath = indexPath
	s.IndexMapping = indexMapping
	s.Logger.Debug("initialized index", "type", index, "path", indexPath)
	return
}

func (s *BadgerStore) ReopenIndex() error {
	if s.IndexPath == "" {
		return nil
	}
	var ix indexer.Indexer
	// Clean up existing index directory
	os.RemoveAll(s.IndexPath)
	os.MkdirAll(s.IndexPath, 0700)

	switch s.IndexType {
	case "badger":
		ix = indexer.NewBadgerIndexerWithMapping(s.IndexPath, s.IndexMapping)
	case "memory":
		ix, _ = indexer.NewMemIndexerWithMapping(s.IndexPath, s.IndexMapping)
	case "moss-scorch":
		ix, _ = indexer.NewMossScorchIndexerWithMapping(s.IndexPath, s.IndexMapping)
	case "moss":
		ix, _ = indexer.NewMossIndexer(s.IndexPath)
	case "geo-moss":
		ix, _ = indexer.NewMossIndexerWithMapping(s.IndexPath, s.IndexMapping)
	default:
		ix = indexer.NewIndexer(s.IndexPath, s.IndexMapping)
	}
	// Re-wrap in GeoIndexer if needed (NewWithIndex does this unconditionally?)
	// NewWithIndex does: geoIndex := &indexer.GeoIndexer{Field: "_location", Indexer: ix}
	// But NewWithIndex assumes it returns a store with GeoIndexer.
	// s.Indexer is interface.
	// If I just set s.Indexer = ix, I lose GeoIndexer wrapper if it was there?
	// NewWithIndex wraps it.
	// So I should wrap it.
	geoIndex := &indexer.GeoIndexer{Field: "_location", Indexer: ix}
	s.Indexer = geoIndex
	return nil
}

func (s *BadgerStore) CreateDatabase() error {
	return nil
}

// tableWithPrefix returns the key prefix used to namespace all entries belonging
// to the given table. The resulting key has the form:
//
//	<TablePrefix><table><IdSeaparator>  e.g. "t$users|"
//
// It is used when iterating or seeking over all rows within a table.
func (s *BadgerStore) tableWithPrefix(table string) string {
	return s.KeyFormat.TablePrefix + table + s.KeyFormat.IdSeparator
}

// tableKey combines a table name and a record ID into an un-prefixed
// composite key of the form:
//
//	<table><IdSeparator><id>  e.g. "users|abc123"
//
// This intermediate key is used by keyForTableId to build the full storage key.
func (s *BadgerStore) tableKey(table, id string) string {
	return table + s.KeyFormat.IdSeparator + id
}

// storedKey returns the full BadgerDB storage key for a specific record
// within a table. The key has the form:
//
//	<TablePrefix><table><IdSeparator><id>  e.g. "t$users|abc123"
//
// This key is used for single-record operations such as Get, Save, and Delete.
func (s *BadgerStore) storedKey(table, id string) string {
	return s.tableWithPrefix(table) + id
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
	txn := BadgerTransaction{db: s.Db, txn: nil, mode: "update"}
	txn.Restart()
	return &txn
}

// FinishTransaction ebds transaction
func (s *BadgerStore) FinishTransaction(tx common.Transaction) error {
	return tx.Commit()
}

// RestartTransaction restarts transaction
func (s *BadgerStore) RestartTransaction(txn common.Transaction) error {
	err := txn.Restart()
	if err == nil {
		s.Logger.Debug("BatchTX Restarted")
	}
	return err
}

func (s *BadgerStore) CreateBucket(bucket string) error {
	return nil
}

func (s *BadgerStore) updateTableStats(table string, change uint) {

}

func (s *BadgerStore) _Get(key, store string) ([][]byte, error) {
	k := s.storedKey(store, key)
	storeKey := []byte(k)
	var val []byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		item, err2 := txn.Get(storeKey)
		if err2 != nil {
			s.Logger.Info("error getting key", "store", store, "key", k, "err", err2.Error())
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
	s.Logger.Debug("_Get success", "key", key, "storeKey", k)
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
	prefix := []byte(s.tableWithPrefix(store))
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
			b.Delete(string(key))
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
	storeKey := []byte(s.storedKey(store, key))
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
	storeKey := []byte(s.storedKey(store, key))
	err := s.Db.Update(func(txn *badgerdb.Txn) error {
		s.Logger.Debug("_Save", "key", key, "store", store, "storeKey", storeKey)
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
//
// Deprecated: All() is inefficient for large datasets as it loads all results into memory.
// Use AllCursor() instead, which streams results using a producer-consumer pattern.

func (s *BadgerStore) All(count int, skip int, store string) (common.ObjectRows, error) {
	var objs [][][]byte
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = count / 2
		it := txn.NewIterator(opts)
		defer it.Close()

		var skipCount int
		var rowCount int
		prefix := []byte(s.tableWithPrefix(store))
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
		return &TransactionRows{entries: objs, length: len(objs), logger: s.Logger}, err
	}
	return nil, common.ErrNotFound
}

func (s *BadgerStore) GetAll(count int, skip int, bucket []string) (objs [][][]byte, err error) {
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

func (s *BadgerStore) Count(store string) (int64, error) {
	var count int64
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		if store == "" {
			for it.Rewind(); it.Valid(); it.Next() {
				count++
			}
		} else {
			prefix := []byte(s.tableWithPrefix(store))
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++
			}
		}
		return nil
	})
	return count, err
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
		// 1. CRITICAL: Ensure ProducerDone is always called to prevent the consumer from blocking forever.
		defer rows.ProducerDone()

		err := s.Db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			prefix := []byte(s.tableWithPrefix(store))
			it.Seek(prefix)

			for it.ValidForPrefix(prefix) {
				item := it.Item()
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err // Propagate error to the outer 'err' variable
				}

				k := item.KeyCopy(nil)
				keyParts := bytes.SplitN(k, []byte(s.KeyFormat.IdSeparator), 2)
				if len(keyParts) < 2 {
					it.Next()
					continue
				}
				id := keyParts[1]

				// 2. IMPROVEMENT: Check if the consumer has closed the cursor.
				// This stops pointless work if the consumer is no longer listening.
				if !rows.OnNext([][]byte{id, v}) {
					// Consumer has called Close(). Exit the transaction gracefully.
					return nil
				}

				it.Next()
			}

			return nil
		})

		// 3. IMPROVEMENT: If an error occurred, communicate it to the consumer.
		if err != nil {
			s.Logger.Error("cursor rows for "+store+" failed", "err", err.Error())
			// This allows the consumer to see the error by calling rows.LastError()
			rows.SetLastError(err)
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
	rows := common.NewCursorRows()
	prefix := []byte(s.tableWithPrefix(store))
	startKey := []byte(s.storedKey(store, id))
	go func(rows *common.CursorRows) {
		defer rows.Close()
		err := s.Db.View(func(txn *badgerdb.Txn) error {
			opts := badgerdb.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			skipped := 0
			sent := 0

			for it.Seek(startKey); it.ValidForPrefix(prefix); it.Next() {
				if skipped < skip {
					skipped++
					continue
				}

				item := it.Item()
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				k := item.KeyCopy(nil)
				keyParts := bytes.SplitN(k, []byte(s.KeyFormat.IdSeparator), 2)
				if len(keyParts) < 2 {
					continue
				}
				entryID := keyParts[1]

				if !rows.OnNext([][]byte{entryID, v}) {
					return nil // Consumer closed the cursor
				}

				sent++
				if count > 0 && sent >= count {
					break
				}
			}
			return nil
		})

		if err != nil {
			s.Logger.Error("cursor rows for "+store+" failed", "err", err.Error())
			rows.SetLastError(err)
		}
	}(rows)
	return rows, nil
}

// Before Get all recent items from a key
func (s *BadgerStore) Before(id string, count int, skip int, store string) (common.ObjectRows, error) {
	var objs [][][]byte
	prefix := []byte(s.tableWithPrefix(store))
	err := s.Db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchSize = 10
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek([]byte(s.storedKey(store, id))); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			// STAY WITHIN THE TABLE: Stop if we move into the previous table
			if !bytes.HasPrefix(k, prefix) {
				break
			}
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
	skey := s.storedKey(store, key)
	indexKey := s.tableKey(store, key)
	storeKey := []byte(skey)
	s.Logger.Debug("Save", "key", key, "store", store, "storeKey", skey, "indexKey", indexKey)
	err = s.Db.Update(func(txn *badgerdb.Txn) error {
		err := txn.Set(storeKey, data)
		if err != nil {
			return err
		}
		return s.Indexer.IndexDocument(indexKey, IndexedData{Bucket: store, Data: src})
	})
	return key, err
}

// SaveWithGeo save
func (s *BadgerStore) SaveWithGeo(key, store string, src interface{}, field string) (string, error) {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.storedKey(store, key)
		indexKey := s.tableKey(store, key)
		storeKey := []byte(skey)
		s.Logger.Debug("SaveWithGeo", "key", key, "store", store, "storeKey", skey, "indexKey", indexKey)
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
					return s.Indexer.IndexDocument(indexKey, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
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
			return s.Indexer.IndexDocument(indexKey, IndexedData{Bucket: store, Data: src})
		})
		return "", err
	}
	return key, errors.New("unable to save")
}

// SaveWithGeoTX save a key within a transaction
func (s *BadgerStore) SaveWithGeoTX(key, store string, src interface{}, field string, txn common.Transaction) error {
	if srcMap, ok := src.(map[string]interface{}); ok {
		skey := s.storedKey(store, key)
		indexKey := s.tableKey(store, key)
		storeKey := []byte(skey)
		s.Logger.Debug("SaveWithGeoTX", "key", key, "store", store, "storeKey", skey, "indexKey", indexKey)
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
				return s.Indexer.IndexDocument(indexKey, map[string]interface{}{"bucket": store, "data": srcMap, "location": geo})
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
		return s.Indexer.IndexDocument(indexKey, IndexedData{Bucket: store, Data: src})
	}
	return errors.New("unable to save")
}

// SaveTX save a key within a transaction
func (s *BadgerStore) SaveTX(key, store string, src interface{}, txn common.Transaction) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	skey := s.storedKey(store, key)
	indexKey := s.tableKey(store, key)
	storeKey := []byte(skey)
	s.Logger.Debug("SaveTX", "key", key, "store", store, "storeKey", skey, "indexKey", indexKey)
	err = txn.Set(storeKey, data)
	if err != nil {
		return err
	}
	err = s.Indexer.IndexDocument(indexKey, IndexedData{Bucket: store, Data: src})
	return err
}

// GetTX get a key within a transaction
func (s *BadgerStore) GetTX(key string, store string, dst interface{}, txn common.Transaction) error {
	k := s.storedKey(store, key)
	storeKey := []byte(k)
	var val []byte
	val, err := txn.Get(storeKey)
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return common.ErrNotFound
	}
	s.Logger.Debug("GetTX success", "key", key, "storeKey", k)
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
}
func (s *BadgerStore) Replace(key string, store string, src interface{}) error {
	_, err := s.Save(key, store, src)
	return err
}
func (s *BadgerStore) ReplaceTX(key string, store string, src interface{}, tx common.Transaction) error {
	return s.SaveTX(key, store, src, tx)
}
func (s *BadgerStore) DeleteTX(key string, store string, tx common.Transaction) error {
	skey := s.storedKey(store, key)
	storeKey := []byte(skey)
	s.Logger.Info("DeleteTX", "key", key)
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
	s.Logger.Info("FilterGet", "filter", filter, "store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		var (
			data [][]byte
		)

		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		q := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(q, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			s.Logger.Error("FilterGet search failed", "error", err, "query", q)
			return err
		}
		s.Logger.Info("FilterGet metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
		if res.Total == 0 {
			s.Logger.Info("FilterGet empty result", "store", store, "query", q)
			return common.ErrNotFound
		}
		idParts := strings.Split(res.Hits[0].ID, s.KeyFormat.IdSeparator)
		shortID := res.Hits[0].ID
		if len(idParts) > 1 {
			shortID = idParts[1]
		}
		data, err = s._Get(shortID, store)
		if err != nil {
			return err
		}

		err = json.Unmarshal(data[1], dst)
		return err
	}
	return common.ErrNotFound

}
func (s *BadgerStore) FilterGetTX(filter map[string]interface{}, store string, dst interface{}, opts common.ObjectStoreOptions, tx common.Transaction) error {
	s.Logger.Info("FilterGetTX", "filter", filter, "store", store, "opts", opts)
	if query, ok := filter["q"].(map[string]interface{}); ok {
		//check if filter contains a nested field which is used to traverse a sub bucket
		// res, err := s.Indexer.Query(indexer.GetQueryString(store, filter))
		q := indexer.GetQueryString(store, query)
		res, err := s.Indexer.QueryWithOptions(q, 1, 0, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			s.Logger.Error("FilterGetTX search failed", "error", err, "query", q)
			return err
		}
		s.Logger.Info("FilterGetTX metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
		if res.Total == 0 {
			s.Logger.Error("FilterGetTX empty result", "query", q)
			return common.ErrNotFound
		}
		key := res.Hits[0].ID
		idParts := strings.Split(key, s.KeyFormat.IdSeparator)
		shortID := key
		if len(idParts) > 1 {
			shortID = idParts[1]
		}
		k := s.storedKey(store, shortID)
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
		s.Logger.Info("FilterGetAll", "count", count, "skip", skip, "store", store, "query", q)
		res, err := s.Indexer.QueryWithOptions(q, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
		if err != nil {
			s.Logger.Warn("FilterGetAll search failed", "error", err, "query", q)
			return nil, err
		}
		s.Logger.Info("FilterGetAll metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
		if res.Total == 0 {
			return nil, common.ErrNotFound
		}
		// return NewIndexedBadgerRows(store, res.Total, res, &s), nil
		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s, logger: s.Logger}, nil
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
			s.Logger.Info("Query", "count", count, "skip", skip, "store", store, "query", q, "opts", opts)
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
			s.Logger.Info("Query", "count", count, "skip", skip, "store", store, "query", q, "facets", facets, "orderBy", order)
			res, err = s.Indexer.FacetedQuery(q, &facets, count, skip, true, []string{}, order)

		}
		if err != nil {
			s.Logger.Warn("Query failed", "error", err, "query", q)
			return nil, nil, err
		}
		s.Logger.Info("Query metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
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

		return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s, logger: s.Logger}, agg, err
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
	s.Logger.Info("GeoQuery", "count", count, "skip", skip, "store", store, "lat", lat, "lon", lon, "distance", distance, "query", q)
	if geoIndexer, ok := s.Indexer.(indexer.GeoCapableIndexer); ok {
		res, err = geoIndexer.GeoDistanceQuery(q, lon, lat, distance, count, skip, true, []string{}, indexer.OrderRequest([]string{"-_score", "-_id"}))
	} else {
		return nil, common.ErrNotImplemented
	}
	if err != nil {
		s.Logger.Warn("GeoQuery search failed", "error", err, "query", q)
		return nil, err
	}
	s.Logger.Info("GeoQuery metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
	if res.Total == 0 {
		return nil, common.ErrNotFound
	}

	return &SyncIndexRows{name: store, length: res.Total, result: res, bs: s, logger: s.Logger}, err
}

// FilterDelete filter delete items
func (s *BadgerStore) FilterDelete(query map[string]interface{}, store string, opts common.ObjectStoreOptions) error {
	s.Logger.Info("FilterDelete", "filter", query, "store", store)
	count := 1000
	q := indexer.GetQueryString(store, query)
	res, err := s.Indexer.QueryWithOptions(q, count, 0, true, []string{})
	if err == nil {
		s.Logger.Info("FilterDelete metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
		if res.Total == 0 {
			return common.ErrNotFound
		}
		for _, v := range res.Hits {
			idParts := strings.Split(v.ID, s.KeyFormat.IdSeparator)
			shortID := v.ID
			if len(idParts) > 1 {
				shortID = idParts[1]
			}
			err = s._Delete(shortID, store)
			if err != nil {
				break
			}
			indexKey := s.tableKey(store, v.ID)
			err = s.Indexer.UnIndexDocument(indexKey)
			if err != nil {
				break
			}
		}
	}
	return err
}

func (s *BadgerStore) FilterCount(filter map[string]interface{}, store string, opts common.ObjectStoreOptions) (int64, error) {
	var query map[string]interface{}
	if filter != nil {
		if q, ok := filter["q"].(map[string]interface{}); ok {
			query = q
		}
	} else {
		return 0, common.ErrNotFound
	}
	q := indexer.GetQueryString(store, query)
	s.Logger.Info("FilterCount", "store", store, "query", q)
	res, err := s.Indexer.Query(q)
	if err != nil {
		s.Logger.Warn("FilterCount search failed", "error", err, "query", q)
		return 0, err
	}
	s.Logger.Info("FilterCount metrics", "store", store, "Hits", len(res.Hits), "Total", res.Total, "MaxScore", res.MaxScore, "Took", res.Took)
	if res.Total == 0 {
		return 0, common.ErrNotFound
	}
	return int64(res.Total), nil
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
			storeKey := []byte(s.storedKey(store, key))
			indexKey := s.tableKey(store, key)
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			indexedData := map[string]interface{}{"bucket": store, "data": src}
			b.Index(indexKey, indexedData)
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
		storeKey := []byte(s.storedKey(store, key))
		indexKey := s.tableKey(store, key)
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		b.Index(indexKey, indexedData)
		keys[i] = key
	}
	if err2 := txn.Commit(); err2 != nil {
		if strings.Contains(err2.Error(), "Transaction Conflict") {
			return nil, common.ErrTransactionConflict
		}
		return nil, err2
	}
	s.Logger.Debug("BatchInsert", "row", len(keys))
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
		storeKey := []byte(s.storedKey(store, key))
		indexKey := s.tableKey(store, key)
		err = txn.Set(storeKey, data)
		if err != nil {
			return nil, err
		}
		indexedData := IndexedData{Bucket: store, Data: src}
		s.Logger.Debug("BatchInsertTX", "row", indexedData)
		b.Index(indexKey, indexedData)
		keys[i] = key
	}
	err = txn.Commit()
	if err == nil {
		s.Logger.Debug("BatchTX Committed")
	}
	if err = s.RestartTransaction(txn); err != nil {
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
			storeKey := []byte(s.storedKey(store, key))
			indexKey := s.tableKey(store, key)
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			var iData map[string]interface{}
			err := json.Unmarshal(row[1], &iData)

			if err != nil {
				return err
			}
			b.Index(indexKey, IndexedData{Bucket: store, Data: iData})
			keys[i] = key
		}
		s.Logger.Debug("copied", "rows", len(keys))
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
			storeKey := []byte(s.storedKey(store, key))
			err = txn.Set(storeKey, data)
			if err != nil {
				return err
			}
			// dataAsStr := string(data)
			keys[i] = key
		}
		s.Logger.Debug("copied", "rows", len(keys))
		return nil
	})
	return
}
func (s *BadgerStore) Close() {
	s.stopTicker()
	if s.Db != nil {
		s.Db.Close()
		s.Logger.Debug("closed badger store")
	}
	if s.Indexer != nil {
		s.Indexer.Close()
		s.Logger.Debug("closed badger index")
	}
}
