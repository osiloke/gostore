package common

import (
	"context"
	"time"
)

func NewCursorRows() *CursorRows {
	return &CursorRows{ci: 0, getChan: make(chan bool, 1), nextChan: make(chan [][]byte), exitChan: make(chan bool, 1), doneChan: make(chan bool, 1)}
}

type CursorRows struct {
	ci        int
	lastError error
	getChan   chan bool
	nextChan  chan [][]byte
	exitChan  chan bool
	doneChan  chan bool
}

func (s *CursorRows) Done() chan bool {
	return s.doneChan
}
func (s *CursorRows) NextChan() chan bool {
	return s.getChan
}
func (s *CursorRows) Exit() chan bool {
	return s.exitChan
}
func (s *CursorRows) OnNext(v [][]byte) {
	s.nextChan <- v
}

// Next get next item
func (s *CursorRows) Next(dst interface{}) (bool, error) {
	return false, nil
}

// NextRaw get next raw item
func (s *CursorRows) NextRaw() ([]byte, bool) {
	return nil, false
}

// NextRaw get next raw item
func (s *CursorRows) NextKV() ([][]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	s.getChan <- true
	select {
	case <-ctx.Done():
		s.lastError = ctx.Err()
		return nil, ErrTimeout
	case row := <-s.nextChan:
		if row == nil {
			return nil, ErrEOF
		}
		s.ci++
		return row, nil
	}
}

// LastError get last error
func (s *CursorRows) LastError() error {
	return s.lastError
}

// Count returns count of entries
func (s *CursorRows) Count() int {
	return s.ci
}

// Close closes row iterator
func (s *CursorRows) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer func() {
		cancel()
		close(s.getChan)
		close(s.nextChan)
		close(s.doneChan)
		close(s.exitChan)
	}()
	s.exitChan <- true
	select {
	case <-ctx.Done():
		s.lastError = ctx.Err()
		return
	case <-s.doneChan:
	}
}
