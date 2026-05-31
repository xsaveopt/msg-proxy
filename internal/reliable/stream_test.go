package reliable

import (
	"context"
	"sync"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
)

type mockBot struct {
	mu   sync.Mutex
	sent []*protocol.Packet
}

func (m *mockBot) Send(p *protocol.Packet) error {
	m.mu.Lock()
	m.sent = append(m.sent, p)
	m.mu.Unlock()
	return nil
}

func (m *mockBot) SendAsync(_ context.Context, p *protocol.Packet) error {
	return m.Send(p)
}

func (m *mockBot) SendAsyncCallback(_ context.Context, p *protocol.Packet, onSent func()) error {
	err := m.Send(p)
	if err == nil && onSent != nil {
		onSent()
	}
	return err
}

func (m *mockBot) SendWait(_ context.Context, p *protocol.Packet) error {
	return m.Send(p)
}

func (m *mockBot) getSent() []*protocol.Packet {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*protocol.Packet, len(m.sent))
	copy(out, m.sent)
	return out
}

func (m *mockBot) countByType(t string) int {
	n := 0
	for _, p := range m.getSent() {
		if p.Type == t {
			n++
		}
	}
	return n
}

func TestStreamSendReceive(t *testing.T) {
	bot := &mockBot{}
	s := New("sess1", bot, 0)
	defer s.Stop()

	ctx := context.Background()

	if err := s.Send(ctx, []byte("hello world")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	sent := bot.getSent()
	if len(sent) != 1 {
		t.Fatalf("expected 1 sent packet, got %d", len(sent))
	}
	if sent[0].Type != protocol.TypeData || sent[0].Seq != 0 {
		t.Errorf("unexpected packet: type=%q seq=%d", sent[0].Type, sent[0].Seq)
	}

	s.Deliver(ctx, sent[0])

	data, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "hello world" {
		t.Errorf("got %q, want %q", string(data), "hello world")
	}

	if n := bot.countByType(protocol.TypeDataAck); n != 1 {
		t.Errorf("expected 1 DACK, got %d", n)
	}
}

func TestStreamInOrderDelivery(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-order", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	for i := uint32(0); i < 3; i++ {
		s.Deliver(ctx, &protocol.Packet{
			SessionID: "sess-order",
			Seq:       i,
			Type:      protocol.TypeData,
			Payload:   protocol.EncodePayload([]byte{byte(i)}),
		})
	}

	for want := byte(0); want < 3; want++ {
		data, err := s.Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if data[0] != want {
			t.Errorf("got byte %d, want %d", data[0], want)
		}
	}
}

func TestStreamOutOfOrderDelivery(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-ooo", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	s.Deliver(ctx, &protocol.Packet{
		SessionID: "sess-ooo",
		Seq:       1,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload([]byte("second")),
	})

	select {
	case <-s.readyCh:
		t.Error("unexpected data before seq 0 delivered")
	default:
	}

	s.Deliver(ctx, &protocol.Packet{
		SessionID: "sess-ooo",
		Seq:       0,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload([]byte("first")),
	})

	first, _ := s.Read(ctx)
	second, _ := s.Read(ctx)

	if string(first) != "first" {
		t.Errorf("first: got %q, want %q", string(first), "first")
	}
	if string(second) != "second" {
		t.Errorf("second: got %q, want %q", string(second), "second")
	}

	dacks := []*protocol.Packet{}
	for _, p := range bot.getSent() {
		if p.Type == protocol.TypeDataAck {
			dacks = append(dacks, p)
		}
	}
	if len(dacks) == 0 {
		t.Fatal("no DACK sent")
	}
	last := dacks[len(dacks)-1]
	if last.Seq != 1 {
		t.Errorf("final DACK seq: got %d, want 1", last.Seq)
	}
}

func TestStreamDuplicateIgnored(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-dup", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	pkt := &protocol.Packet{
		SessionID: "sess-dup",
		Seq:       0,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload([]byte("once")),
	}

	s.Deliver(ctx, pkt)
	s.Deliver(ctx, pkt)

	data, _ := s.Read(ctx)
	if string(data) != "once" {
		t.Errorf("got %q, want %q", string(data), "once")
	}

	select {
	case extra := <-s.readyCh:
		t.Errorf("unexpected extra data from duplicate: %q", extra)
	default:
	}
}

func TestStreamDackClearsUnacked(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-dack", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	for _, payload := range []string{"a", "b", "c"} {
		if err := s.Send(ctx, []byte(payload)); err != nil {
			t.Fatalf("Send(%q): %v", payload, err)
		}
	}

	s.sendMu.Lock()
	if len(s.unacked) != 3 {
		t.Errorf("expected 3 unacked, got %d", len(s.unacked))
	}
	s.sendMu.Unlock()

	s.Deliver(ctx, &protocol.Packet{
		Type: protocol.TypeDataAck,
		Seq:  2,
	})

	s.sendMu.Lock()
	if len(s.unacked) != 0 {
		t.Errorf("expected 0 unacked after DACK, got %d", len(s.unacked))
	}
	s.sendMu.Unlock()
}

func TestStreamRetransmit(t *testing.T) {
	bot := &mockBot{}
	timeout := 40 * time.Millisecond
	s := New("sess-retx", bot, timeout)
	defer s.Stop()

	if err := s.Send(context.Background(), []byte("retransmit me")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	time.Sleep(timeout * 4)

	dataSends := bot.countByType(protocol.TypeData)
	if dataSends < 2 {
		t.Errorf("expected original + retransmit (>=2), got %d", dataSends)
	}
}

func TestStreamRetransmitStopsAfterDack(t *testing.T) {
	bot := &mockBot{}
	timeout := 40 * time.Millisecond
	s := New("sess-nodack", bot, timeout)
	defer s.Stop()

	ctx := context.Background()
	if err := s.Send(ctx, []byte("will be acked")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	s.Deliver(ctx, &protocol.Packet{Type: protocol.TypeDataAck, Seq: 0})

	countBefore := bot.countByType(protocol.TypeData)
	time.Sleep(timeout * 4)
	countAfter := bot.countByType(protocol.TypeData)

	if countAfter > countBefore {
		t.Errorf("retransmit happened after DACK: before=%d after=%d", countBefore, countAfter)
	}
}

func TestStreamStopUnblocksRead(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-stop", bot, 0)

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		_, err := s.Read(ctx)
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	s.Stop()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected error after Stop, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Read did not unblock after Stop")
	}
}
