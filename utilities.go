package gostore

import(
	"time"
	"errors"
	"log"
)
func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %d", name, elapsed)
}


var ErrNotFound = errors.New("Does not exist")
var ErrDuplicatePk = errors.New("Duplicate primary key exists")