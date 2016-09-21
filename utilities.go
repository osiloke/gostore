package gostore

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %d", name, elapsed)
}
func ToInt(str string) (int64, error) {
	res, err := strconv.ParseInt(str, 0, 64)
	if err != nil {
		res = 0
	}
	return res, err
}

var ErrKeyNotValid = errors.New("record key was not generated")
var ErrNotFound = errors.New("does not exist")
var ErrNotAllDeleted = errors.New("not all rows were deleted")
var ErrDuplicatePk = errors.New("duplicate primary key exists")
var ErrNotImplemented = errors.New("not implemented yet")
var ErrEOF = errors.New("eof")

type Params map[string]interface{}

func ObjectType(i interface{}) reflect.Type {
	return reflect.TypeOf(i)
}
