package transport

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	queueCap     = 512
	workers      = 1
	sendInterval = 3000 * time.Millisecond
	maxRetries   = 5
	retryBackoff = 50 * time.Millisecond
)

var ErrQueueFull = errors.New("send queue full")

type sendJob struct {
	text     string
	err      chan error
	onSent   func()
	urgent   bool
	attempts int
}

type SendQueue struct {
	q        chan sendJob
	retry    chan sendJob
	sendFn   func(text string) error
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewSendQueue(sendFn func(text string) error) *SendQueue {
	sq := &SendQueue{
		q:      make(chan sendJob, queueCap),
		retry:  make(chan sendJob, 64),
		sendFn: sendFn,
		stopCh: make(chan struct{}),
	}
	for i := 0; i < workers; i++ {
		go sq.worker()
	}
	return sq
}

func (sq *SendQueue) Enqueue(text string) error {
	job := sendJob{text: text, urgent: true}
	if sq.stopped() {
		return errors.New("queue stopped")
	}
	select {
	case sq.q <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

func (sq *SendQueue) EnqueueAsync(ctx context.Context, text string) error {
	job := sendJob{text: text}
	if sq.stopped() {
		return errors.New("queue stopped")
	}
	select {
	case sq.q <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-sq.stopCh:
		return errors.New("queue stopped")
	}
}

func (sq *SendQueue) EnqueueAsyncCallback(ctx context.Context, text string, onSent func()) error {
	job := sendJob{text: text, onSent: onSent}
	if sq.stopped() {
		return errors.New("queue stopped")
	}
	select {
	case sq.q <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-sq.stopCh:
		return errors.New("queue stopped")
	}
}

func (sq *SendQueue) EnqueueWait(ctx context.Context, text string) error {
	job := sendJob{text: text, err: make(chan error, 1), urgent: true}
	select {
	case sq.q <- job:
	case <-ctx.Done():
		return ctx.Err()
	case <-sq.stopCh:
		return errors.New("queue stopped")
	}
	select {
	case err := <-job.err:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-sq.stopCh:
		return errors.New("queue stopped")
	}
}

func (sq *SendQueue) stopped() bool {
	select {
	case <-sq.stopCh:
		return true
	default:
		return false
	}
}

func (sq *SendQueue) Stop() {
	sq.stopOnce.Do(func() { close(sq.stopCh) })
}

func (sq *SendQueue) worker() {
	for {
		var job sendJob
		select {
		case job = <-sq.retry:
			job.urgent = true
		case <-sq.stopCh:
			return
		default:
			select {
			case job = <-sq.retry:
				job.urgent = true
			case job = <-sq.q:
			case <-sq.stopCh:
				return
			}
		}

		sq.dispatch(job)

		if !job.urgent {
			select {
			case <-time.After(sendInterval):
			case <-sq.stopCh:
				return
			}
		}
	}
}

func (sq *SendQueue) dispatch(job sendJob) {
	err := sq.sendFn(job.text)
	if err != nil && job.err == nil {
		if job.attempts >= maxRetries {
			return
		}
		job.attempts++
		time.AfterFunc(retryBackoff<<(job.attempts-1), func() {
			select {
			case sq.retry <- job:
			case <-sq.stopCh:
			default:
			}
		})
		return
	}
	if err == nil && job.onSent != nil {
		job.onSent()
	}
	if job.err != nil {
		job.err <- err
	}
}
