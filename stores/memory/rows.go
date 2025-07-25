package memory

import (
	"encoding/json"

	"dario.cat/mergo"
	common "github.com/gostore/gostore/common"
)

var logger = common.Logger("memory")

// TransactionRows synchroniously get rows
type TransactionRows struct {
	length  int
	entries []interface{}
	ci      int
}

// Next get next item
func (s *TransactionRows) Next(dst interface{}) (bool, error) {
	length := len(s.entries)
	var err error
	if s.ci < length {
		val := s.entries[s.ci]
		if _dst, ok := dst.(map[string]interface{}); ok {
			err = mergo.Map(&_dst, val.(map[string]interface{}))
			if err != nil {
				return false, nil
			}
		} else {
			dst = val
		}
		s.ci++
		return true, err
	}
	return false, common.ErrEOF
}

// NextRaw get next raw item
func (s *TransactionRows) NextRaw() ([]byte, bool) {
	length := len(s.entries)
	if s.ci < length {
		val := s.entries[s.ci]
		s.ci++
		s, err := json.Marshal(val)
		if err != nil {
			return nil, false
		}
		return s, true
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
