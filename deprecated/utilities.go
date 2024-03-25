package gostore

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	logger.Debug(fmt.Sprintf("%s took %v", name, elapsed))
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

// func IterRows(rows ObjectRows, eachFunc func(), done chan bool) (err error) {
// 	defer rows.Close()
// 	ok := false
// 	for {
// 		select {
// 		case <-done:
// 			logger.Info("done")
// 			return
// 		default:

// 			var element interface{}
// 			ok, err = rows.Next(&element)
// 			if err != nil {
// 				logger.Error("error retrieving row", "err", err)
// 				return
// 			}
// 			logger.Info("element", "el", element)
// 			if ok {
// 				logger.Info("next element", "el", element)
// 				results = append(results, element)

// 			} else {
// 				if err != nil {
// 					logger.Warn("Error retrieving rows", "err", err)
// 				}
// 				return
// 			}
// 		}
// 	}
// 	return
// }

func ParseAllRows(rows ObjectRows, done chan bool) (results []interface{}, err error) {
	defer rows.Close()
	ok := false
	for {
		select {
		case <-done:
			logger.Info("done")
			return
		default:

			var element interface{}
			ok, err = rows.Next(&element)
			if err != nil {
				logger.Error("error retrieving row", "err", err)
				return
			}
			logger.Info("element", "el", element)
			if ok {
				logger.Info("next element", "el", element)
				results = append(results, element)

			} else {
				if err != nil {
					logger.Warn("Error retrieving rows", "err", err)
				}
				return
			}
		}
	}
	return
}
