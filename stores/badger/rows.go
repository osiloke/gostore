package badger

import (
	"encoding/json"

	common "github.com/osiloke/gostore-common"
)

// //BadgerRows handles pulling row items in a goroutine
// type BadgerRows struct {
// 	i         int
// 	length    int
// 	retrieved chan string
// 	prefix    string
// 	closed    chan bool
// 	done      chan bool
// 	nextItem  chan interface{}
// 	itr       *badgerdb.Iterator
// 	lastError error
// 	isClosed  bool
// 	sync.RWMutex
// }

// // Next get next item
// func (s *BadgerRows) Next(dst interface{}) (bool, error) {
// 	if s.lastError != nil {
// 		return false, s.lastError
// 	}
// 	//NOTE: Consider saving id in badger data
// 	var _dst map[string]interface{}
// 	s.nextItem <- &_dst
// 	key := <-s.retrieved
// 	if key == "" {
// 		return false, nil
// 	}
// 	_dst["id"] = key
// 	_data, _ := json.Marshal(&_dst)
// 	err := json.Unmarshal(_data, dst)
// 	return true, err
// }

// // NextRaw get next raw item
// func (s *BadgerRows) NextRaw() ([]byte, bool) {
// 	// if s.lastError != nil {
// 	// 	return nil, false
// 	// }
// 	// //NOTE: Consider saving id in badger data
// 	// var _dst []byte
// 	// s.nextItem <- &_dst
// 	// <-s.retrieved
// 	// return _dst, true
// 	return nil, false
// }

// // LastError get last error
// func (s *BadgerRows) LastError() error {
// 	return s.lastError
// }

// // Close closes row iterator
// func (s *BadgerRows) Close() {
// 	logger.Info("close badger rows")
// 	s.closed <- true
// 	<-s.done
// 	close(s.done)
// }
// func (s *BadgerRows) close() {
// 	defer s.itr.Close()
// 	logger.Info("close retrieved")
// 	close(s.retrieved)
// 	logger.Info("close nextItem")
// 	close(s.nextItem)
// 	s.done <- true
// 	// s.isClosed = true
// }

// func newBadgerRows(itr *badgerdb.Iterator, prefix string) *BadgerRows {
// 	closed := make(chan bool)
// 	retrieved := make(chan string)
// 	nextItem := make(chan interface{})
// 	ci := 0
// 	b := BadgerRows{
// 		nextItem:  nextItem,
// 		closed:    closed,
// 		itr:       itr,
// 		retrieved: retrieved,
// 		prefix:    prefix,
// 		done:      make(chan bool),
// 	}

// 	go func() {
// 		defer b.close()
// 	OUTER:
// 		for {
// 			select {
// 			case <-closed:
// 				logger.Info("newBadgerRows closed")
// 				break OUTER
// 			case item := <-nextItem:
// 				if itr.Valid() {
// 					_item := itr.Item()
// 					logger.Debug("next item " + string(_item.Key()))
// 					var val []byte
// 					err := _item.Value(func(v []byte) error {
// 						val = make([]byte, len(v))
// 						copy(val, v)
// 						return nil
// 					})
// 					if err != nil {
// 						logger.Warn(fmt.Sprintf("Error while getting value for key: %q", _item.Key()))
// 						continue
// 					}
// 					err = json.Unmarshal(val, item)
// 					if err != nil {
// 						logger.Warn(err.Error())
// 						b.lastError = err
// 						retrieved <- ""
// 						break OUTER
// 					} else {
// 						key := strings.Split(string(_item.Key()), "|")[1]
// 						retrieved <- key
// 						ci++
// 					}
// 					itr.Next()
// 				} else {
// 					b.lastError = common.ErrEOF
// 					break OUTER
// 				}
// 			}
// 			// }
// 		}
// 	}()
// 	return &b
// }

// // SyncRows synchroniously get rows
// type SyncRows struct {
// 	length int
// 	itr    *badgerdb.Iterator
// 	ci     int
// }

// // Next get next item
// func (s *SyncRows) Next(dst interface{}) (bool, error) {
// 	err := common.ErrEOF
// 	if s.ci != s.length {
// 		s.itr.Next()
// 		if s.itr.Valid() {
// 			_item := s.itr.Item()
// 			logger.Debug("next item " + string(_item.Key()))
// 			var val []byte
// 			err = _item.Value(func(v []byte) error {
// 				val = make([]byte, len(v))
// 				copy(val, v)
// 				return nil
// 			})
// 			if err == nil {
// 				err = json.Unmarshal(val, dst)
// 				if err == nil {
// 					s.ci++
// 					return true, nil
// 				}
// 				logger.Warn(err.Error())
// 			}
// 			logger.Warn(fmt.Sprintf("Error while getting value for key: %q", _item.Key()))
// 		}
// 	}
// 	return false, err
// }

// // NextRaw get next raw item
// func (s *SyncRows) NextRaw() ([]byte, bool) {
// 	return nil, false
// }

// // LastError get last error
// func (s *SyncRows) LastError() error {
// 	return nil
// }

// // Count returns count of entries
// func (s *SyncRows) Count() int {
// 	return s.length
// }

// // Close closes row iterator
// func (s *SyncRows) Close() {
// 	s.itr.Close()
// }

// TransactionRows synchroniously get rows
type TransactionRows struct {
	length  int
	entries [][][]byte
	ci      int
}

// Next get next item
func (s *TransactionRows) Next(dst interface{}) (bool, error) {
	err := common.ErrEOF
	if s.ci < s.length {
		if err == nil {
			val := s.entries[s.ci][1]
			err = json.Unmarshal(val, dst)
			if err == nil {
				s.ci++
				return true, nil
			}
			logger.Warn(err.Error())
		}
	}
	return false, err
}

// NextRaw get next raw item
func (s *TransactionRows) NextRaw() ([]byte, bool) {
	if s.ci < s.length {
		val := s.entries[s.ci][1]
		s.ci++
		return val, true
	}
	return nil, false

}

// LastError get last error
func (s *TransactionRows) LastError() error {
	return nil
}

// Count returns count of entries
func (s *TransactionRows) Count() int {
	return s.length
}

// Close closes row iterator
func (s *TransactionRows) Close() {
}
