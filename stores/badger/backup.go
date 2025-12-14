package badger

import (
	"fmt"
	"net/http"
	"os"
	"time"

	badgerdb "github.com/dgraph-io/badger/v4"
)

// WriteToHTTP writes store to http writer
func (s *BadgerStore) WriteToHTTP(w http.ResponseWriter) error {
	since := uint64(0)
	_, err := s.Db.Backup(w, since)
	if err != nil {
		// http.Error(w, err.Error(), http.StatusInternalServerError)p
		return err
	}
	// _, size := db.Size()
	date := time.Now().Format("2006_01_02_15-04-05")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="badger_%d_%s.db"`, since, date))
	// w.Header().Set("Content-Length", strconv.Itoa(int(size)))
	return nil
}

func (s *BadgerStore) Restore(filename string) error {
	// Close existing store
	if s.Db != nil {
		if err := s.Db.Close(); err != nil {
			return err
		}
	}
	// Open File
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	// Open New Store with Restore Options
	opt := BadgerRestoreOptions(s.Path)
	db, err := badgerdb.Open(opt)
	if err != nil {
		return err
	}
	// Load data
	if err := db.Load(f, 16); err != nil {
		db.Close()
		return err
	}
	// Close Restore Store
	if err := db.Close(); err != nil {
		return err
	}
	// Reopen Default Store
	defaultOpt := BadgerDefaultOptions(s.Path)
	s.Db, err = badgerdb.Open(defaultOpt)
	return err
}
