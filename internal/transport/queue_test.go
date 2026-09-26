package transport

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestSendQueueDelivers(t *testing.T) {
	var count atomic.Int32
	sq := NewSendQueue(func(text string) error {
		count.Add(1)
		return nil
	})
	defer sq.Stop()

	if err := sq.Enqueue("msg1"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := sq.Enqueue("msg2"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if count.Load() >= 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if count.Load() < 2 {
		t.Errorf("expected 2 messages sent, got %d", count.Load())
	}
}

func TestEnqueueWaitBlocks(t *testing.T) {
	sent := make(chan string, 1)
	sq := NewSendQueue(func(text string) error {
		sent <- text
		return nil
	})
	defer sq.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err := sq.EnqueueWait(ctx, "hello")
	if err != nil {
		t.Fatalf("EnqueueWait: %v", err)
	}

	select {
	case got := <-sent:
		if got != "hello" {
			t.Errorf("got %q, want %q", got, "hello")
		}
	default:
		t.Error("message was not actually sent")
	}
}

func TestEnqueueWaitCancelledContext(t *testing.T) {
	block := make(chan struct{})
	sq := NewSendQueue(func(text string) error {
		<-block
		return nil
	})
	defer func() {
		close(block)
		sq.Stop()
	}()

	for i := 0; i < queueCap; i++ {
		if err := sq.Enqueue("filler"); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sq.EnqueueWait(ctx, "blocked")
	if err == nil {
		t.Error("expected error when context cancelled")
	}
}

func TestRetryPriority(t *testing.T) {
	received := make(chan string, 10)
	sq := NewSendQueue(func(text string) error {
		received <- text
		return nil
	})
	defer sq.Stop()

	sq.retry <- sendJob{text: "retry-msg", err: make(chan error, 1)}
	sq.q <- sendJob{text: "normal-msg", err: make(chan error, 1)}

	timeout := time.After(200 * time.Millisecond)
	got := make(map[string]bool)
	for len(got) < 2 {
		select {
		case msg := <-received:
			got[msg] = true
		case <-timeout:
			t.Fatalf("expected 2 messages, got %d", len(got))
		}
	}
	if !got["retry-msg"] || !got["normal-msg"] {
		t.Errorf("expected both messages to be delivered, got %v", got)
	}
}

func TestEnqueueRetriesAfterSendFailure(t *testing.T) {
	var calls atomic.Int32
	delivered := make(chan string, 4)
	sq := NewSendQueue(func(text string) error {
		if calls.Add(1) == 1 {
			return errors.New("rate limited")
		}
		delivered <- text
		return nil
	})
	defer sq.Stop()

	if err := sq.Enqueue("fire-and-forget"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	select {
	case got := <-delivered:
		if got != "fire-and-forget" {
			t.Errorf("retried text: got %q, want %q", got, "fire-and-forget")
		}
	case <-time.After(time.Second):
		t.Fatalf("a failed fire-and-forget send was never retried (send attempts: %d)", calls.Load())
	}
}

func TestEnqueueAsyncCallbackRetriesAfterSendFailure(t *testing.T) {
	var calls atomic.Int32
	sq := NewSendQueue(func(text string) error {
		if calls.Add(1) == 1 {
			return errors.New("rate limited")
		}
		return nil
	})
	defer sq.Stop()

	sent := make(chan struct{}, 1)
	err := sq.EnqueueAsyncCallback(context.Background(), "chunk", func() { sent <- struct{}{} })
	if err != nil {
		t.Fatalf("EnqueueAsyncCallback: %v", err)
	}

	select {
	case <-sent:
	case <-time.After(sendInterval + 2*time.Second):
		t.Fatalf("a failed callback send was never retried, so onSent never fired (send attempts: %d)", calls.Load())
	}
}

func TestEnqueueStopsRetryingAfterCap(t *testing.T) {
	var calls atomic.Int32
	sq := NewSendQueue(func(string) error {
		calls.Add(1)
		return errors.New("rate limited")
	})
	defer sq.Stop()

	if err := sq.Enqueue("never-sent"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	want := int32(1 + maxRetries)
	deadline := time.Now().Add(retryBackoff<<maxRetries + time.Second)
	for time.Now().Before(deadline) && calls.Load() < want {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(retryBackoff << maxRetries)
	if got := calls.Load(); got != want {
		t.Errorf("send attempts: got %d, want %d", got, want)
	}
}

func TestEnqueueWaitReturnsSendErrorWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	sq := NewSendQueue(func(string) error {
		calls.Add(1)
		return errors.New("rate limited")
	})
	defer sq.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sq.EnqueueWait(ctx, "msg"); err == nil {
		t.Fatal("expected the send error to reach the caller")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("send attempts: got %d, want 1", got)
	}
}

func TestEnqueueReturnsErrQueueFull(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	sq := NewSendQueue(func(string) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil
	})
	defer func() {
		close(release)
		sq.Stop()
	}()

	if err := sq.Enqueue("in-flight"); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker never picked up the first job")
	}

	for i := 0; i < queueCap; i++ {
		if err := sq.Enqueue("filler"); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	if err := sq.Enqueue("overflow"); !errors.Is(err, ErrQueueFull) {
		t.Errorf("overflow Enqueue: got %v, want %v", err, ErrQueueFull)
	}
}

func TestAsyncJobsArePaced(t *testing.T) {
	stamps := make(chan time.Time, 2)
	sq := NewSendQueue(func(string) error {
		stamps <- time.Now()
		return nil
	})
	defer sq.Stop()

	for _, text := range []string{"first", "second"} {
		if err := sq.EnqueueAsync(context.Background(), text); err != nil {
			t.Fatalf("EnqueueAsync(%q): %v", text, err)
		}
	}

	var got []time.Time
	for len(got) < 2 {
		select {
		case ts := <-stamps:
			got = append(got, ts)
		case <-time.After(sendInterval + 2*time.Second):
			t.Fatalf("expected 2 sends, got %d", len(got))
		}
	}
	if gap := got[1].Sub(got[0]); gap < sendInterval-50*time.Millisecond {
		t.Errorf("async sends %v apart, want at least %v", gap, sendInterval)
	}
}

func TestUrgentJobsAreNotPaced(t *testing.T) {
	stamps := make(chan time.Time, 3)
	sq := NewSendQueue(func(string) error {
		stamps <- time.Now()
		return nil
	})
	defer sq.Stop()

	for i := 0; i < 3; i++ {
		if err := sq.Enqueue("urgent"); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	deadline := time.After(sendInterval / 2)
	for i := 0; i < 3; i++ {
		select {
		case <-stamps:
		case <-deadline:
			t.Fatalf("urgent sends were paced: only %d of 3 went out", i)
		}
	}
}

func TestEnqueueAfterStopIsRejected(t *testing.T) {
	var calls atomic.Int32
	sq := NewSendQueue(func(string) error {
		calls.Add(1)
		return nil
	})
	sq.Stop()

	cases := map[string]func() error{
		"Enqueue":              func() error { return sq.Enqueue("late") },
		"EnqueueAsync":         func() error { return sq.EnqueueAsync(context.Background(), "late") },
		"EnqueueAsyncCallback": func() error { return sq.EnqueueAsyncCallback(context.Background(), "late", func() {}) },
	}
	for name, enqueue := range cases {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				if err := enqueue(); err == nil {
					t.Fatalf("attempt %d: %s after Stop returned nil, want an error", i, name)
				}
			}
		})
	}

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Errorf("send function ran %d times after Stop", got)
	}
}

func TestEnqueueWaitAfterStopReturnsError(t *testing.T) {
	sq := NewSendQueue(func(string) error { return nil })
	sq.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sq.EnqueueWait(ctx, "late"); err == nil {
		t.Error("EnqueueWait after Stop returned nil, want an error")
	}
}
