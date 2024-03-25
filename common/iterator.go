package common

type Iterator interface {
	Seek(key []byte)
	Next()
	Current() ([]byte, []byte, bool)
	Key() []byte
	Value() []byte
	Valid() bool
	Close() error
}
