package memory

import (
	"encoding/json"

	"dario.cat/mergo"
	common "github.com/osiloke/gostore/common"
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
	if s.ci < length {
		val := s.entries[s.ci]
		s.ci++
		var err error
		switch d := dst.(type) {
		case map[string]interface{}:
			err = mergo.Map(&d, val.(map[string]interface{}))
		case *map[string]interface{}:
			err = mergo.Map(d, val.(map[string]interface{}))
		default:
			b, mErr := json.Marshal(val)
			if mErr != nil {
				return false, mErr
			}
			err = json.Unmarshal(b, dst)
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
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
