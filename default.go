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
}

type StoreObj interface {
	SetKey(key string)
	GetKey() string
}

type StoreObjs []StoreObj
