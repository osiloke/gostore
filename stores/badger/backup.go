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
	logger.Info("Starting restore process")
	// Stop Ticker
	s.stopTicker()
	logger.Info("Stopped ticker")

	// Close existing store
	if s.Db != nil {
		logger.Info("Closing existing badger store")
		if err := s.Db.Close(); err != nil {
			return err
		}
	}
	// Close Indexer
	if s.Indexer != nil {
		logger.Info("Closing indexer")
		s.Indexer.Close()
	}

	// Open File
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// Open New Store with Restore Options
	logger.Info("Opening badger store with restore options")
	opt := BadgerRestoreOptions(s.Path)
	db, err := badgerdb.Open(opt)
	if err != nil {
		return err
	}

	// Load data
	logger.Info("Loading data from backup file")
	if err := db.Load(f, 1); err != nil {
		db.Close()
		return err
	}

	// Close Restore Store
	logger.Info("Closing restore store")
	if err := db.Close(); err != nil {
		return err
	}

	// Reopen Default Store
	logger.Info("Reopening badger store with default options")
	defaultOpt := BadgerDefaultOptions(s.Path)
	s.Db, err = badgerdb.Open(defaultOpt)
	if err != nil {
		return err
	}

	// Restart Ticker
	s.setupTicker()
	logger.Info("Restarted ticker")

	// Reopen Indexer
	if s.IndexPath != "" {
		logger.Info("Reopening indexer")
		if err := s.ReopenIndex(); err != nil {
			return err
		}
	}
	return nil
}
