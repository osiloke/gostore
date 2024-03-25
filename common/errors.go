package common

import "errors"

var ErrKeyNotValid = errors.New("record key was not generated")
var ErrNotFound = errors.New("does not exist")
var ErrNotAllDeleted = errors.New("not all rows were deleted")
var ErrDuplicatePk = errors.New("duplicate primary key exists")
var ErrNotImplemented = errors.New("not implemented yet")
var ErrEOF = errors.New("eof")
var ErrTimeout = errors.New("timeout occurred accessing next key")
