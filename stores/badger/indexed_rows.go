package badger

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/v2"
	common "github.com/osiloke/gostore-common"
)

type NextItem struct {
}

// New Api
type IndexedBadgerRows struct {
	lastError error
	isClosed  bool
	closed    chan bool
	retrieved chan string
	nextItem  chan interface{}
	mu        *sync.RWMutex
}

func (s *IndexedBadgerRows) Next(dst interface{}) (bool, error) {
	if s.lastError != nil {
		return false, s.lastError
	}

	s.nextItem <- dst
	key := <-s.retrieved
	if key == "" {
		return false, nil
	}
	return true, nil
}

func (s *IndexedBadgerRows) NextRaw() ([]byte, bool) {
	return nil, false
}
func (s *IndexedBadgerRows) LastError() error {
	return s.lastError
}
func (s *IndexedBadgerRows) Close() {
	// s.rows = nil
	s.mu.RLock()
	if s.isClosed {
		return
	}
	s.mu.RUnlock()
	s.closed <- true
	logger.Info("close badger rows")
	s.mu.Lock()
	s.isClosed = true
	s.mu.Unlock()
}
func NewIndexedBadgerRows(name string, total uint64, result *bleve.SearchResult, bs *BadgerStore) *IndexedBadgerRows {
	closed := make(chan bool, 1)
	nextItem := make(chan interface{})
	retrieved := make(chan string)
	ci := 0

	b := IndexedBadgerRows{isClosed: false, nextItem: nextItem, closed: closed, retrieved: retrieved, mu: &sync.RWMutex{}}
	go func() {
	OUTER:
		for {
			select {
			case <-closed:
				logger.Info("newIndexedBadgerRows closed")
				close(closed)
				break OUTER

			case item := <-nextItem:
				logger.Info("current index", "ci", ci, "total", result.Hits.Len())
				if ci == result.Hits.Len() {
					b.lastError = common.ErrEOF
					logger.Info("break badger rows loop")
					retrieved <- ""
					break OUTER

				} else {
					h := result.Hits[ci]
					logger.Info(fmt.Sprintf("retrieving %s from %s store in badgerdb", h.ID, name))
					row, err := bs._Get(h.ID, name)
					if err != nil {
						if err == common.ErrNotFound {
							//not found so remove from indexer
							bs.Indexer.UnIndexDocument(h.ID)
							retrieved <- ""
							continue
						} else {
							logger.Warn(err.Error())
							b.lastError = err
							retrieved <- ""
							break OUTER
						}

					}
					if err := json.Unmarshal(row[1], item); err != nil {
						logger.Warn(err.Error())
						b.lastError = err
						retrieved <- ""
						break OUTER

					}
					retrieved <- string(row[0])
					ci++
				}
			}
		}
		close(retrieved)
		close(nextItem)
		// close(closed)
	}()
	return &b
}

// SyncIndexRows synchroniously get rows
type SyncIndexRows struct {
	lastError error
	length    uint64
	name      string
	result    *bleve.SearchResult
	bs        *BadgerStore
	ci        uint64
}

// Next get next item
func (s *SyncIndexRows) Next(dst interface{}) (bool, error) {
	err := common.ErrEOF
	if int(s.ci) != s.result.Hits.Len() {
		h := s.result.Hits[s.ci]
		logger.Info("next row", "key", h.ID, "store", s.name)
		row, err := s.bs._Get(h.ID, s.name)
		if err == nil {
			err = json.Unmarshal(row[1], dst)
			if err == nil {
				s.ci++
				return true, nil
			}
			if err == common.ErrNotFound {
				//not found so remove from indexer
				s.bs.Indexer.UnIndexDocument(h.ID)
			} else {
				logger.Warn(err.Error())
			}
		}
	}
	s.lastError = err
	return false, err
}

// NextRaw get next raw item
func (s *SyncIndexRows) NextRaw() ([]byte, bool) {
	err := common.ErrEOF
	if int(s.ci) != s.result.Hits.Len() {
		h := s.result.Hits[s.ci]
		logger.Info("NEXT KEY", "id", h.ID, "store", s.name)
		row, err := s.bs._Get(h.ID, s.name)
		if err == nil {
			s.ci++
			return row[1], true
		}
		if err == common.ErrNotFound {
			//not found so remove from indexer
			s.bs.Indexer.UnIndexDocument(h.ID)
		} else {
			logger.Warn(err.Error())
		}
	}
	s.lastError = err
	return nil, false
}

// LastError get last error
func (s *SyncIndexRows) LastError() error {
	return s.lastError
}

// Count returns count of entries
func (s *SyncIndexRows) Count() int {
	return int(s.length)
}

// Close closes row iterator
func (s *SyncIndexRows) Close() {
	logger.Debug("finished processing rows", "result", s.result.String())
}
