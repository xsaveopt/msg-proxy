package transport

import (
	"context"
	"errors"
	"sync"
)

const queueCap = 512

var ErrQueueFull = errors.New("send queue full")

type sendJob struct {
	text string
	err  chan error
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
	go sq.run()
	return sq
}

func (sq *SendQueue) Enqueue(text string) error {
	job := sendJob{text: text}
	select {
	case sq.q <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

func (sq *SendQueue) EnqueueWait(ctx context.Context, text string) error {
	job := sendJob{text: text, err: make(chan error, 1)}
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

func (sq *SendQueue) Stop() {
	sq.stopOnce.Do(func() { close(sq.stopCh) })
}

func (sq *SendQueue) run() {
	for {
		select {
		case job := <-sq.retry:
			sq.dispatch(job)
			continue
		case <-sq.stopCh:
			return
		default:
		}

		select {
		case job := <-sq.retry:
			sq.dispatch(job)
		case job := <-sq.q:
			sq.dispatch(job)
		case <-sq.stopCh:
			return
		}
	}
}

func (sq *SendQueue) dispatch(job sendJob) {
	err := sq.sendFn(job.text)
	if job.err != nil {
		job.err <- err
	}
}
