package session

import (
	"testing"
	"time"

	"msg-proxy/internal/protocol"
)

func TestSessionClose(t *testing.T) {
	s := New("test-id")
	if s.GetState() != StateConnecting {
		t.Errorf("initial state: got %d, want %d", s.GetState(), StateConnecting)
	}

	s.Close()
	s.Close()

	if s.GetState() != StateClosed {
		t.Errorf("after Close state: got %d, want %d", s.GetState(), StateClosed)
	}

	select {
	case <-s.Done():
	default:
		t.Error("Done() channel should be closed")
	}

	for range s.Incoming {
	}
}

func TestSessionTouch(t *testing.T) {
	s := New("touch-test")
	before := s.LastSeen
	time.Sleep(2 * time.Millisecond)
	s.Touch()
	if !s.LastSeen.After(before) {
		t.Error("Touch() did not update LastSeen")
	}
}

func TestManagerAddGet(t *testing.T) {
	m := NewManager()
	s := New("s1")

	_, ok := m.Add(s)
	if !ok {
		t.Error("first Add should succeed")
	}

	got := m.Get("s1")
	if got != s {
		t.Errorf("Get: got %v, want %v", got, s)
	}

	s2 := New("s1")
	existing, ok := m.Add(s2)
	if ok {
		t.Error("second Add with same ID should fail")
	}
	if existing != s {
		t.Errorf("existing: got %v, want %v", existing, s)
	}

	m.Delete("s1")
	if m.Get("s1") != nil {
		t.Error("session should be gone after Delete")
	}
}

func TestManagerReap(t *testing.T) {
	m := NewManager()
	s := New("reap-me")
	s.mu.Lock()
	s.LastSeen = time.Now().Add(-2 * time.Minute)
	s.mu.Unlock()

	m.Add(s)
	m.Reap(time.Minute)

	if m.Get("reap-me") != nil {
		t.Error("stale session should have been reaped")
	}
	select {
	case <-s.Done():
	default:
		t.Error("reaped session Done() should be closed")
	}
}

func TestManagerReapKeepsActive(t *testing.T) {
	m := NewManager()
	s := New("keep-me")
	s.Touch()

	m.Add(s)
	m.Reap(time.Minute)

	if m.Get("keep-me") == nil {
		t.Error("active session should NOT be reaped")
	}
}

func TestSessionIncoming(t *testing.T) {
	s := New("incoming-test")
	pkt := &protocol.Packet{SessionID: "incoming-test", Type: protocol.TypeAck}
	s.Incoming <- pkt

	got := <-s.Incoming
	if got != pkt {
		t.Errorf("got wrong packet: %v", got)
	}
}
