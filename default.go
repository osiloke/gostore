package gostore

/**
Store is deprecated
*/
type Store interface {
	Get(key []byte, resource string) ([][]byte, error)
	PrefixGet(prefix []byte, resource string) ([][]byte, error) //Gets an item with a prefix
	Save(key []byte, obj []byte, resource string) error
	Delete(key []byte, resource string) error
	DeleteAll(resource string) error
	GetAll(count int, skip int, resource string) ([][][]byte, error)
	GetAllAfter(key []byte, count int, skip int, resource string) ([][][]byte, error)  //Get all items after a key
	GetAllBefore(key []byte, count int, skip int, resource string) ([][][]byte, error) //Get all items before a key
	Filter(prefix []byte, count int, skip int, resource string) ([][][]byte, error)

	//Streaming api
	StreamFilter(key []byte, count int, resource string) chan []byte
	StreamAll(count int, resource string) chan [][]byte //Stream all entries through a channel

	//Misc api
	Stats(bucket string) (map[string]interface{}, error)
	GetStoreObject() interface{}
}

type ObjectStoreOptions interface {
	GetIndexes() map[string][]string
}

type DefaultObjectStoreOptions struct {
	Index map[string][]string
}

func (d DefaultObjectStoreOptions) GetIndexes() map[string][]string {
	return d.Index
}

//ObjectStore represents all api common to all database implementations
type ObjectStore interface {
	CreateDatabase() error
	CreateTable(table string, sample interface{}) error

	GetStore() interface{}
	Stats(store string) (map[string]interface{}, error)

	All(count int, skip int, store string) (ObjectRows, error)
	AllCursor(store string) (ObjectRows, error)
	AllWithinRange(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error)

	Since(id string, count int, skip int, store string) (ObjectRows, error)  //Get all recent items from a key
	Before(id string, count int, skip int, store string) (ObjectRows, error) //Get all existing items before a key

	FilterSince(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error)  //Get all recent items from a key
	FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error) //Get all existing items before a key
	FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (int64, error) //Get all existing items before a key

	Get(key string, store string, dst interface{}) error
	Save(store string, src interface{}) (string, error)
	SaveAll(store string, src ...interface{}) (keys []string, err error)
	Update(key string, store string, src interface{}) error
	Replace(key string, store string, src interface{}) error
	Delete(key string, store string) error

	FilterUpdate(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error
	FilterReplace(filter map[string]interface{}, src interface{}, store string, opts ObjectStoreOptions) error
	FilterGet(filter map[string]interface{}, store string, dst interface{}, opts ObjectStoreOptions) error
	FilterGetAll(filter map[string]interface{}, count int, skip int, store string, opts ObjectStoreOptions) (ObjectRows, error)
	FilterDelete(filter map[string]interface{}, store string, opts ObjectStoreOptions) error
	FilterCount(filter map[string]interface{}, store string, opts ObjectStoreOptions) (int64, error)

	GetByField(name, val, store string, dst interface{}) error
	GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error)

	BatchDelete(ids []interface{}, store string, opts ObjectStoreOptions) (err error)
	BatchUpdate(id []interface{}, data []interface{}, store string, opts ObjectStoreOptions) error
	BatchFilterDelete(filter []map[string]interface{}, store string, opts ObjectStoreOptions) error

	Close()
}

type ObjectRows interface {
	Next(interface{}) (bool, error)
	NextRaw() ([]byte, bool)
	Close()
	LastError() error
}

type StoreOptions map[string]interface{}

type StoreObj interface {
	SetKey(key string)
	GetKey() string
}

type StoreObjs []StoreObj

type TableConfig struct {
	NestedBucketFields map[string]string //defines fields to be used to extract nested buckets for data
}
