package gostore

/**
TODO: Define callbacks and also pass context
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
type ObjectStore interface {
	//Management Api
	CreateDatabase() error
	CreateTable(table string, sample interface{}) error

	//Misc api
	GetStore() interface{}
	Stats(store string) (map[string]interface{}, error)

	//New Api
	All(count int, skip int, store string) (ObjectRows, error)
	AllCursor(store string) (ObjectRows, error)

	Since(id string, count int, skip int, store string) (ObjectRows, error) //Get all recent items from a key
	Before(id string, count int, skip int, store string) (ObjectRows, error) //Get all existing items before a key

	FilterSince(id string, filter map[string]interface{}, count int, skip int, store string) (ObjectRows, error) //Get all recent items from a key
	FilterBefore(id string, filter map[string]interface{}, count int, skip int, store string) (ObjectRows, error) //Get all existing items before a key
	FilterBeforeCount(id string, filter map[string]interface{}, count int, skip int, store string) (int64, error) //Get all existing items before a key

	Get(key string, store string, dst interface{}) error
	Save(store string, src interface{}) (string, error)
	Update(key string, store string, src interface{}) error
	Delete(key string, store string) error

	//Filter

	FilterGet(filter map[string]interface{}, store string, dst interface{}) error
	FilterGetAll(filter map[string]interface{}, count int, skip int, store string) (ObjectRows, error)
	FilterDelete(filter map[string]interface{}, store string) error
	FilterCount(filter map[string]interface{}, store string) (int64, error)

	//Misc gets
	GetByField(name, val, store string, dst interface{}) error
	GetByFieldsByField(name, val, store string, fields []string, dst interface{}) (err error)

	Close()
}

type ObjectRows interface {
	Next(interface{}) (bool, error)
	Close()
}

type StoreOptions map[string]interface{}

type StoreObj interface {
	SetKey(key string)
	GetKey() string
}

type StoreObjs []StoreObj
