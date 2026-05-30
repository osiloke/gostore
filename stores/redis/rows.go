package redis

import (
	"encoding/json"

	"dario.cat/mergo"
	common "github.com/osiloke/gostore/common"
)

var logger = common.Logger("redis")

// RedisRows implements the common.ObjectRows interface.
type RedisRows struct {
	entries [][]byte // Raw JSON documents
	ci      int      // Current index
}

// Next gets the next item and decodes it into dst.
func (r *RedisRows) Next(dst interface{}) (bool, error) {
	length := len(r.entries)
	if r.ci < length {
		val := r.entries[r.ci]
		r.ci++

		// Decode the JSON data first
		var temp map[string]interface{}
		if err := json.Unmarshal(val, &temp); err != nil {
			return false, err
		}

		// Handle case where dst is map[string]interface{} (used extensively in gostore tests)
		if dstMap, ok := dst.(map[string]interface{}); ok {
			if err := mergo.Map(&dstMap, temp); err != nil {
				return false, err
			}
		} else {
			// Otherwise decode directly into the target pointer/struct
			if err := json.Unmarshal(val, dst); err != nil {
				return false, err
			}
		}
		return true, nil
	}
	return false, nil
}

// NextRaw gets the next raw JSON bytes.
func (r *RedisRows) NextRaw() ([]byte, bool) {
	length := len(r.entries)
	if r.ci < length {
		val := r.entries[r.ci]
		r.ci++
		return val, true
	}
	return nil, false
}

// LastError returns the last encountered error.
func (r *RedisRows) LastError() error {
	return nil
}

// Count returns the total number of entries.
func (r *RedisRows) Count() int {
	return len(r.entries)
}

// Close closes the rows iterator.
func (r *RedisRows) Close() {
	// No cleanup needed
}
