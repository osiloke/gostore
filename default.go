package stores

type Store interface{
	Get(key []byte, resource string) ([][]byte, error)
	Save(key []byte,  obj []byte, resource string) error
	Delete(key []byte, resource string) error
	DeleteAll(resource string) error
	GetAll(count int, resource string) ([][][]byte, error)
	Filter(key []byte, count int, resource string) ([][]byte, error)
	StreamFilter(key []byte, count int, resource string) chan []byte
}


type StoreObj interface{
	SetKey(key string)
	GetKey() string
}

type StoreObjs []StoreObj
