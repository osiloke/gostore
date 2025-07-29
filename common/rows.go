package common

import (
	"encoding/json"
	"sync"
	"time"
)

// NewCursorRows creates a new, deadlock-safe cursor.
func NewCursorRows() *CursorRows {
	return &CursorRows{
		// nextChan is buffered to decouple producer/consumer slightly.
		nextChan: make(chan [][]byte, 1),
		// exitChan is a struct channel that will be closed to broadcast the exit signal.
		exitChan: make(chan struct{}),
		// doneChan is buffered to ensure the producer doesn't block when signaling it's finished.
		doneChan: make(chan struct{}, 1),
	}
}

// CursorRows is an iterator designed for safe communication between a producer
// and a consumer goroutine.
type CursorRows struct {
	mu        sync.Mutex
	ci        int
	lastError error

	nextChan chan [][]byte
	exitChan chan struct{}
	doneChan chan struct{}
}

// OnNext should be called by the producer to send the next item.
// It returns false if the consumer has called Close(), indicating the producer should stop.
func (s *CursorRows) OnNext(v [][]byte) bool {
	select {
	case s.nextChan <- v:
		// Data was successfully sent.
		return true
	case <-s.exitChan:
		// The exit channel was closed, consumer wants to stop.
		return false
	}
}

// ProducerDone MUST be called by the producer goroutine, ideally via defer,
// to signal that it has finished processing, either normally or by being aborted.
func (s *CursorRows) ProducerDone() {
	// Close nextChan to signal EOF to any consumer waiting in NextKV.
	close(s.nextChan)
	// Signal that cleanup is complete. This unblocks the Close() method.
	s.doneChan <- struct{}{}
}

// NextKV gets the next key-value pair. It's called by the consumer.
func (s *CursorRows) NextKV() ([][]byte, error) {
	select {
	case row, ok := <-s.nextChan:
		if !ok {
			// Channel is closed and empty, meaning end of iteration.
			return nil, ErrEOF
		}
		s.mu.Lock()
		s.ci++
		s.mu.Unlock()
		return row, nil
	case <-time.After(5 * time.Second): // A simple timeout to prevent waiting forever on a stalled producer.
		s.SetLastError(ErrTimeout)
		return nil, ErrTimeout
	}
}

// Close signals the producer to stop and waits for it to confirm shutdown.
func (s *CursorRows) Close() {
	// Signal the producer to exit by closing the exit channel.
	// This is a non-blocking broadcast operation.
	close(s.exitChan)
	// Wait for the producer to call ProducerDone().
	<-s.doneChan
	// Drain any final item the producer might have sent before it saw the exit signal.
	for range s.nextChan {
	}
}

// SetLastError allows the producer to safely record an error that the consumer can retrieve.
func (s *CursorRows) SetLastError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastError = err
}

// --- The following methods depend on NextKV and require no significant changes ---

func (s *CursorRows) Next(dst interface{}) (bool, error) {
	row, err := s.NextKV()
	if err != nil {
		return false, err // Correctly propagates ErrEOF
	}
	err = json.Unmarshal(row[1], dst)
	if err != nil {
		s.SetLastError(err) // Safely set the unmarshal error
		return false, err
	}
	return true, nil
}

func (s *CursorRows) NextRaw() ([]byte, bool) {
	row, err := s.NextKV()
	if err != nil {
		return nil, false
	}
	return row[1], true
}

func (s *CursorRows) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastError
}

func (s *CursorRows) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ci
}
