package badger

import (
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Define where to store the state
const stateFilePath = "last_backup_since.txt"

func (s *BadgerStore) WriteToHTTP(w http.ResponseWriter, r *http.Request) error {
	since := uint64(0)

	if sinceQuery := r.URL.Query().Get("since"); sinceQuery != "" {
		if parsed, err := strconv.ParseUint(sinceQuery, 10, 64); err == nil {
			since = parsed
		}
	} else {
		if data, err := os.ReadFile(stateFilePath); err == nil {
			if parsed, err := strconv.ParseUint(string(data), 10, 64); err == nil {
				since = parsed
			}
		}
	}

	numGo := 1
	if numGoQuery := r.URL.Query().Get("num_go"); numGoQuery != "" {
		if parsed, err := strconv.Atoi(numGoQuery); err == nil && parsed > 0 {
			numGo = parsed
		}
	}

	date := time.Now().Format("2006_01_02_15-04-05")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="badger_since_%d_%s.db"`, since, date))

	stream := s.Db.NewStream()
	stream.NumGo = numGo
	stream.SinceTs = since

	nextSince, err := stream.Backup(w, since)
	if err != nil {
		return err
	}

	// SAVE THE STATE FOR NEXT TIME
	// We convert the uint64 to a string and write it to our local tracking file
	if err := os.WriteFile(stateFilePath, []byte(strconv.FormatUint(nextSince, 10)), 0644); err != nil {
		s.Logger.Error(fmt.Sprintf("Backup succeeded, but failed to save state: %v", err))
	} else {
		s.Logger.Info(fmt.Sprintf("Backup completed. Next 'since' value (%d) saved to %s", nextSince, stateFilePath))
	}

	return nil
}

// GetLastBackupSince returns the last saved timestamp
func (s *BadgerStore) GetLastBackupSince(w http.ResponseWriter, r *http.Request) {
	data, err := os.ReadFile(stateFilePath)
	if err != nil {
		// If the file doesn't exist yet, we start at 0
		w.Write([]byte("0"))
		return
	}
	w.Write(data)
}
func (s *BadgerStore) Restore(filename string) error {
	s.Logger.Info("Starting restore process")

	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := s.Db.Load(f, 1); err != nil {
		return err
	}

	s.Logger.Info("Restore successful")
	return nil
}
