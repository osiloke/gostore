package badger

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2"
	log "github.com/mgutz/logxi/v1"
	common "github.com/osiloke/gostore/common"
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
	logger    log.Logger
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
	s.logger.Info("close badger rows")
	s.mu.Lock()
	s.isClosed = true
	s.mu.Unlock()
}
func NewIndexedBadgerRows(name string, total uint64, result *bleve.SearchResult, bs *BadgerStore) *IndexedBadgerRows {
	closed := make(chan bool, 1)
	nextItem := make(chan interface{})
	retrieved := make(chan string)
	ci := 0

	b := IndexedBadgerRows{isClosed: false, nextItem: nextItem, closed: closed, retrieved: retrieved, mu: &sync.RWMutex{}, logger: bs.Logger}
	go func() {
	OUTER:
		for {
			select {
			case <-closed:
				b.logger.Info("newIndexedBadgerRows closed")
				close(closed)
				break OUTER

			case item := <-nextItem:
			INNER:
				for {
					b.logger.Info("current index", "ci", ci, "total", result.Hits.Len())
					if ci == result.Hits.Len() {
						b.lastError = common.ErrEOF
						b.logger.Info("break badger rows loop")
						retrieved <- ""
						break OUTER

					} else {
						h := result.Hits[ci]
						b.logger.Info(fmt.Sprintf("retrieving %s from %s store in badgerdb", h.ID, name))
						// h.ID is now the full badger DB key format without prefix (<store>|<id>)
						// but _Get expects only the <id> part because it prefixes it again.
						idParts := strings.Split(h.ID, "|")
						shortID := h.ID // fallback if split fails
						if len(idParts) > 1 {
							shortID = idParts[1]
						}
						row, err := bs._Get(shortID, name)
						if err != nil {
							if err == common.ErrNotFound {
								//not found so remove from indexer using the full key (h.ID)
								bs.Indexer.UnIndexDocument(h.ID)
								ci++
								continue INNER
							} else {
								b.logger.Warn(err.Error())
								b.lastError = err
								retrieved <- ""
								break OUTER
							}

						}
						if err := json.Unmarshal(row[1], item); err != nil {
							b.logger.Warn(err.Error())
							b.lastError = err
							retrieved <- ""
							break OUTER

						}
						retrieved <- string(row[0])
						ci++
						break INNER
					}
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
	logger    log.Logger
}

// Next get next item
func (s *SyncIndexRows) Next(dst interface{}) (bool, error) {
	for int(s.ci) != s.result.Hits.Len() {
		h := s.result.Hits[s.ci]
		s.logger.Info("next row", "key", h.ID, "store", s.name)
		idParts := strings.Split(h.ID, "|")
		shortID := h.ID
		if len(idParts) > 1 {
			shortID = idParts[1]
		}
		row, err := s.bs._Get(shortID, s.name)
		if err != nil {
			if err == common.ErrNotFound {
				//not found so remove from indexer using full key
				s.bs.Indexer.UnIndexDocument(h.ID)
				s.ci++
				continue
			}
			s.logger.Warn(err.Error())
			s.lastError = err
			return false, err
		}
		if err := json.Unmarshal(row[1], dst); err != nil {
			s.logger.Warn(err.Error())
			s.lastError = err
			return false, err
		}
		s.ci++
		return true, nil
	}
	s.lastError = common.ErrEOF
	return false, common.ErrEOF
}

// NextRaw get next raw item
func (s *SyncIndexRows) NextRaw() ([]byte, bool) {
	for int(s.ci) != s.result.Hits.Len() {
		h := s.result.Hits[s.ci]
		s.logger.Info("NEXT KEY", "id", h.ID, "store", s.name)
		idParts := strings.Split(h.ID, "|")
		shortID := h.ID
		if len(idParts) > 1 {
			shortID = idParts[1]
		}
		row, err := s.bs._Get(shortID, s.name)
		if err != nil {
			if err == common.ErrNotFound {
				//not found so remove from indexer using full key
				s.bs.Indexer.UnIndexDocument(h.ID)
				s.ci++
				continue
			}
			s.logger.Warn(err.Error())
			s.lastError = err
			return nil, false
		}
		s.ci++
		return row[1], true
	}
	s.lastError = common.ErrEOF
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
	s.logger.Debug("finished processing rows", "result", s.result.String())
}
