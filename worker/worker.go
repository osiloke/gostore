package worker

import (
	"context"
	"sync"
)

// Job represents a job to be executed by a worker.
type Job interface {
	Execute()
}

// WorkerPool manages a pool of workers and a queue of jobs.
type WorkerPool struct {
	jobs   chan Job
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(numWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &WorkerPool{
		jobs:   make(chan Job),
		ctx:    ctx,
		cancel: cancel,
	}

	pool.wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go pool.worker()
	}

	return pool
}

// worker is a worker goroutine that processes jobs from the jobs channel.
func (p *WorkerPool) worker() {
	defer p.wg.Done()
	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				return
			}
			job.Execute()
		case <-p.ctx.Done():
			return
		}
	}
}

// Enqueue adds a job to the worker pool's queue.
func (p *WorkerPool) Enqueue(job Job) {
	p.jobs <- job
}

// Stop stops the worker pool and waits for all workers to finish.
func (p *WorkerPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
	p.cancel()
}
