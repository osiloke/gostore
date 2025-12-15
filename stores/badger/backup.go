package badger

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"
)

// MemoryReader wraps an io.Reader and logs memory stats periodically
type MemoryReader struct {
	io.Reader
	totalRead int64
	lastLog   int64
	interval  int64
}

func (mr *MemoryReader) Read(p []byte) (n int, err error) {
	n, err = mr.Reader.Read(p)
	mr.totalRead += int64(n)
	if mr.totalRead-mr.lastLog > mr.interval {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		alloc := m.Alloc / 1024 / 1024
		sys := m.Sys / 1024 / 1024
		logger.Info(fmt.Sprintf("Restore Progress: Read=%vMB Memory: Alloc=%vMB Sys=%vMB", mr.totalRead/1024/1024, alloc, sys))
		mr.lastLog = mr.totalRead
	}
	return n, err
}

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
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	logger.Info("Loading data from backup file into existing store")
	mr := &MemoryReader{Reader: f, interval: 50 * 1024 * 1024} // Log every 50MB
	if err := s.Db.Load(mr, 1); err != nil {
		return err
	}

	logger.Info("Restore successful")
	return nil
}
