package transport

import (
	"context"
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
