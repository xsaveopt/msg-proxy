package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/session"
)

const waitTimeout = 5 * time.Second

func noopLogger() *slog.Logger {
	return slog.New(noopHandler{})
}

type noopHandler struct{}

func (noopHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (noopHandler) Handle(context.Context, slog.Record) error { return nil }
func (noopHandler) WithAttrs(_ []slog.Attr) slog.Handler      { return noopHandler{} }
func (noopHandler) WithGroup(_ string) slog.Handler           { return noopHandler{} }

type fakeBot struct {
	sent  chan *protocol.Packet
	inbox chan *protocol.Packet
}

func newFakeBot() *fakeBot {
	return &fakeBot{
		sent:  make(chan *protocol.Packet, 256),
		inbox: make(chan *protocol.Packet, 256),
	}
}

func (b *fakeBot) Send(p *protocol.Packet) error {
	select {
	case b.sent <- p:
	default:
	}
	return nil
}

func (b *fakeBot) SendAsync(_ context.Context, p *protocol.Packet) error { return b.Send(p) }

func (b *fakeBot) SendWait(_ context.Context, p *protocol.Packet) error { return b.Send(p) }

func (b *fakeBot) SendAsyncCallback(_ context.Context, p *protocol.Packet, onSent func()) error {
	err := b.Send(p)
	if err == nil && onSent != nil {
		onSent()
	}
	return err
}

func (b *fakeBot) StartReceiver(ctx context.Context) <-chan *protocol.Packet {
	out := make(chan *protocol.Packet, 16)
	go func() {
		defer close(out)
		for {
			select {
			case pkt, ok := <-b.inbox:
				if !ok {
					return
				}
				select {
				case out <- pkt:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func waitPacket(t *testing.T, ch <-chan *protocol.Packet, typ string) *protocol.Packet {
	t.Helper()
	deadline := time.After(waitTimeout)
	for {
		select {
		case pkt := <-ch:
			if pkt.Type == typ {
				return pkt
			}
		case <-deadline:
			t.Fatalf("timed out waiting for a %q packet", typ)
		}
	}
}

func expectNoPacket(t *testing.T, ch <-chan *protocol.Packet) {
	t.Helper()
	select {
	case pkt := <-ch:
		t.Fatalf("unexpected %q packet for session %q", pkt.Type, pkt.SessionID)
	case <-time.After(200 * time.Millisecond):
	}
}

func echoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	t.Cleanup(func() {
		_ = ln.Close()
		<-accepting
	})
	return ln.Addr().String()
}

func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return addr
}

func startProxy(t *testing.T, bot *fakeBot) *Proxy {
	t.Helper()
	p := New(bot, noopLogger())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, time.Minute)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(waitTimeout):
			t.Error("Run did not stop after cancel")
		}
	})
	return p
}

func waitStates(t *testing.T, p *Proxy, want int) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		p.mu.RLock()
		got := len(p.states)
		p.mu.RUnlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	p.mu.RLock()
	got := len(p.states)
	p.mu.RUnlock()
	t.Fatalf("tracked connections: got %d, want %d", got, want)
}

func TestRunConnectsAndPumps(t *testing.T) {
	target := echoServer(t)
	bot := newFakeBot()
	p := startProxy(t, bot)

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeConnect, Target: target}

	if ack := waitPacket(t, bot.sent, protocol.TypeAck); ack.SessionID != "s1" {
		t.Errorf("ACK session: got %q, want %q", ack.SessionID, "s1")
	}

	bot.inbox <- &protocol.Packet{
		SessionID: "s1",
		Seq:       0,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload([]byte("ping")),
	}

	data := waitPacket(t, bot.sent, protocol.TypeData)
	if data.SessionID != "s1" {
		t.Errorf("DATA session: got %q, want %q", data.SessionID, "s1")
	}
	payload, err := protocol.DecodePayload(data.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(payload) != "ping" {
		t.Errorf("echoed payload: got %q, want %q", payload, "ping")
	}

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeClose}

	waitStates(t, p, 0)
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if p.manager.Get("s1") == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Error("session should be removed from the manager after CLOSE")
}

func TestRunDialFailureSendsError(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeConnect, Target: deadAddr(t)}

	errPkt := waitPacket(t, bot.sent, protocol.TypeError)
	if errPkt.SessionID != "s1" {
		t.Errorf("ERROR session: got %q, want %q", errPkt.SessionID, "s1")
	}
	msg, err := protocol.DecodePayload(errPkt.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(msg) == 0 {
		t.Error("ERROR payload should carry the dial error")
	}

	waitStates(t, p, 0)
}

func TestHandleConnectIgnoresDuplicateSession(t *testing.T) {
	target := echoServer(t)
	bot := newFakeBot()
	p := startProxy(t, bot)

	sess := session.New("s1")
	if _, ok := p.manager.Add(sess); !ok {
		t.Fatal("first add should succeed")
	}
	t.Cleanup(sess.Close)

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeConnect, Target: target}
	expectNoPacket(t, bot.sent)
}

func TestDispatchIgnoresUnknownSessionsAndTypes(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	for _, pkt := range []*protocol.Packet{
		{SessionID: "ghost", Type: protocol.TypeData, Payload: protocol.EncodePayload([]byte("x"))},
		{SessionID: "ghost", Type: protocol.TypeDataAck},
		{SessionID: "ghost", Type: protocol.TypeClose},
		{SessionID: "ghost", Type: "nonsense"},
	} {
		bot.inbox <- pkt
	}

	expectNoPacket(t, bot.sent)
	waitStates(t, p, 0)
}

func TestConnectRegistersSessionAndStream(t *testing.T) {
	target := echoServer(t)
	bot := newFakeBot()
	p := startProxy(t, bot)

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeConnect, Target: target}
	waitPacket(t, bot.sent, protocol.TypeAck)
	waitStates(t, p, 1)

	sess := p.manager.Get("s1")
	if sess == nil {
		t.Fatal("session should be registered after CONNECT")
	}
	if got := sess.GetState(); got != session.StateConnected {
		t.Errorf("state: got %v, want %v", got, session.StateConnected)
	}
}

func TestRunStopsWhenPacketsClose(t *testing.T) {
	bot := newFakeBot()
	p := New(bot, noopLogger())

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(context.Background(), time.Minute)
	}()

	close(bot.inbox)
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Run did not stop when the packet channel closed")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	bot := newFakeBot()
	p := New(bot, noopLogger())
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx, time.Minute)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("Run did not stop on cancel")
	}
}

func TestRunClosesTunnelWhenTargetHangsUp(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	bot := newFakeBot()
	p := startProxy(t, bot)

	bot.inbox <- &protocol.Packet{SessionID: "s1", Type: protocol.TypeConnect, Target: ln.Addr().String()}
	waitPacket(t, bot.sent, protocol.TypeAck)

	select {
	case conn := <-accepted:
		if err := conn.Close(); err != nil {
			t.Fatalf("close target: %v", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("target was never dialled")
	}

	if closePkt := waitPacket(t, bot.sent, protocol.TypeClose); closePkt.SessionID != "s1" {
		t.Errorf("CLOSE session: got %q, want %q", closePkt.SessionID, "s1")
	}
	waitStates(t, p, 0)
}
