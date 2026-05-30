package badger

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	badgerdb "github.com/dgraph-io/badger/v4"
	common "github.com/osiloke/gostore/common"
)

type defaultLog struct {
	*common.ZerologLogger
}

func defaultLogger() *defaultLog {
	return &defaultLog{common.NewZerologLogger("", "name", "badger")}
}

func (l *defaultLog) Errorf(f string, v ...interface{}) {
	l.Error(fmt.Sprintf(f, v...))
}

func (l *defaultLog) Warningf(f string, v ...interface{}) {
	l.Warn(fmt.Sprintf(f, v...))
}

func (l *defaultLog) Infof(f string, v ...interface{}) {
	// Suppressed to avoid polluting logs
}

func (l *defaultLog) Debugf(f string, v ...interface{}) {
	// Suppressed to avoid polluting logs
}

// OptionModifier is an optional function that can be set by consumers to modify the
// badgerdb.Options before the database is opened.
var OptionModifier func(opts badgerdb.Options) badgerdb.Options

func BadgerDefaultOptions(path string) badgerdb.Options {
	opt := badgerdb.DefaultOptions(path)
	// .WithCompression(options.ZSTD)
	opt.BlockCacheSize = 1024
	opt.Logger = defaultLogger()
	// opt.SyncWrites = true
	// opt.MaxLevels = 3
	if OptionModifier != nil {
		opt = OptionModifier(opt)
	}
	return opt
}

func BadgerRestoreOptions(path string) badgerdb.Options {
	opt := badgerdb.DefaultOptions(path).WithNumVersionsToKeep(math.MaxInt32)
	opt.Logger = defaultLogger()
	if OptionModifier != nil {
		opt = OptionModifier(opt)
	}
	return opt
}
func valForPath(key string, s interface{}) (v interface{}, err error) {
	keys := strings.Split(key, ".")

	var value interface{} = s
	for _, key := range keys {
		if value, err = getPath(key, value); err != nil {
			break
		}
	}
	if err == nil {
		return value, nil
	}
	return nil, err
}
func getPath(key string, s interface{}) (v interface{}, err error) {
	var (
		i  int64
		ok bool
	)
	switch s := s.(type) {
	case map[string]interface{}:
		if v, ok = s[key]; !ok {
			err = fmt.Errorf("Key not present. [Key:%s]", key)
		}
	case []interface{}:
		if i, err = strconv.ParseInt(key, 10, 64); err == nil {
			array := s
			if int(i) < len(array) {
				v = array[i]
			} else {
				err = fmt.Errorf("Index out of bounds. [Index:%d] [Array:%v]", i, array)
			}
		}
	}
	//fmt.Println("Value:", v, " Key:", key, "Error:", err)
	return v, err
}
