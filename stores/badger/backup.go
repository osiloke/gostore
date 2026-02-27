package badger

import (
	"fmt"
	"net/http"
	"os"
	"time"
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
	s.Logger.Info("Starting restore process")
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	s.Logger.Info("Loading data from backup file into existing store")
	if err := s.Db.Load(f, 1); err != nil {
		return err
	}

	s.Logger.Info("Restore successful")
	return nil
}
