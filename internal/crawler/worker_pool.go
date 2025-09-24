package crawler

import (
	"context"
	"errors"
	"sync"
)

type job func(ctx context.Context)

// WorkerPool coordinates crawl workers with a bounded queue to avoid deadlocks.
type WorkerPool struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan job
	wg     sync.WaitGroup
}

// NewWorkerPool creates a pool with the given concurrency and queue size.
func NewWorkerPool(parent context.Context, concurrency, queueSize int) (*WorkerPool, error) {
	if concurrency <= 0 || queueSize <= 0 {
		return nil, errors.New("worker pool requires positive concurrency and queue size")
	}
	ctx, cancel := context.WithCancel(parent)
	pool := &WorkerPool{
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan job, queueSize),
	}
	pool.start(concurrency)
	return pool, nil
}

func (p *WorkerPool) start(concurrency int) {
	for i := 0; i < concurrency; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case <-p.ctx.Done():
					return
				case job, ok := <-p.jobs:
					if !ok {
						return
					}
					job(p.ctx)
				}
			}
		}()
	}
}

// Submit schedules a job, rejecting if the context cancels or queue is full.
func (p *WorkerPool) Submit(ctx context.Context, fn job) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- fn:
		return nil
	}
}

// Close drains the queue and stops all workers.
func (p *WorkerPool) Close() {
	p.cancel()
	close(p.jobs)
	p.wg.Wait()
}
