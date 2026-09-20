package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/session"
	"msg-proxy/internal/socks5"
)

const (
	waitTimeout = 5 * time.Second
	testTarget  = "example.com:80"
)

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

	mu      sync.Mutex
	sendErr error
}

func newFakeBot() *fakeBot {
	return &fakeBot{
		sent:  make(chan *protocol.Packet, 256),
		inbox: make(chan *protocol.Packet, 256),
	}
}

func (b *fakeBot) setSendErr(err error) {
	b.mu.Lock()
	b.sendErr = err
	b.mu.Unlock()
}

func (b *fakeBot) Send(p *protocol.Packet) error {
	b.mu.Lock()
	err := b.sendErr
	b.mu.Unlock()
	if err != nil {
		return err
	}
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

func tcpPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan net.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		accepted <- c
	}()

	client, err = net.DialTimeout("tcp", ln.Addr().String(), waitTimeout)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-accepted:
	case err := <-errCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(waitTimeout):
		t.Fatal("accept timed out")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

func readReply(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(waitTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read SOCKS5 reply: %v", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	return reply
}

func startProxy(t *testing.T, bot *fakeBot) *Proxy {
	t.Helper()
	p := New(bot, noopLogger())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go p.receiveLoop(ctx, bot.StartReceiver(ctx))
	return p
}

func connectThroughProxy(t *testing.T, p *Proxy, bot *fakeBot) (conn net.Conn, sessionID string, done chan struct{}) {
	t.Helper()
	client, server := tcpPair(t)

	done = make(chan struct{})
	go func() {
		defer close(done)
		p.handleConnect(server, socks5.ConnectRequest{Target: testTarget})
	}()

	connect := waitPacket(t, bot.sent, protocol.TypeConnect)
	if connect.Target != testTarget {
		t.Fatalf("CONNECT target: got %q, want %q", connect.Target, testTarget)
	}
	return client, connect.SessionID, done
}

func newSession(t *testing.T, p *Proxy, id string) *session.Session {
	t.Helper()
	sess := session.New(id)
	if _, ok := p.manager.Add(sess); !ok {
		t.Fatalf("session %q already registered", id)
	}
	t.Cleanup(sess.Close)
	return sess
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("handleConnect did not return")
	}
}

func TestHandleConnectTunnelsData(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	conn, sessionID, done := connectThroughProxy(t, p, bot)

	bot.inbox <- &protocol.Packet{SessionID: sessionID, Type: protocol.TypeAck}

	if reply := readReply(t, conn); !bytes.Equal(reply, []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("SOCKS5 reply: got %v, want a success reply", reply)
	}

	if _, err := conn.Write([]byte("from the browser")); err != nil {
		t.Fatalf("write: %v", err)
	}

	data := waitPacket(t, bot.sent, protocol.TypeData)
	if data.SessionID != sessionID {
		t.Errorf("session: got %q, want %q", data.SessionID, sessionID)
	}
	if data.Seq != 0 {
		t.Errorf("seq: got %d, want 0", data.Seq)
	}
	payload, err := protocol.DecodePayload(data.Payload)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if string(payload) != "from the browser" {
		t.Errorf("payload: got %q, want %q", payload, "from the browser")
	}

	bot.inbox <- &protocol.Packet{
		SessionID: sessionID,
		Seq:       0,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload([]byte("from the server")),
	}

	if err := conn.SetReadDeadline(time.Now().Add(waitTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	buf := make([]byte, len("from the server"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read tunnelled data: %v", err)
	}
	if string(buf) != "from the server" {
		t.Errorf("tunnelled data: got %q, want %q", buf, "from the server")
	}

	dack := waitPacket(t, bot.sent, protocol.TypeDataAck)
	if dack.Seq != 0 {
		t.Errorf("DACK seq: got %d, want 0", dack.Seq)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if closePkt := waitPacket(t, bot.sent, protocol.TypeClose); closePkt.SessionID != sessionID {
		t.Errorf("CLOSE session: got %q, want %q", closePkt.SessionID, sessionID)
	}
	waitDone(t, done)

	if p.manager.Get(sessionID) != nil {
		t.Error("session should be removed from the manager")
	}
}

func TestHandleConnectSendFails(t *testing.T) {
	bot := newFakeBot()
	bot.setSendErr(errors.New("telegram is down"))
	p := startProxy(t, bot)

	client, server := tcpPair(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.handleConnect(server, socks5.ConnectRequest{Target: testTarget})
	}()

	if reply := readReply(t, client); reply[1] != 0x01 {
		t.Errorf("reply: got 0x%02x at byte 1, want 0x01", reply[1])
	}
	waitDone(t, done)
}

func TestHandleConnectAckOutcomes(t *testing.T) {
	cases := []struct {
		name  string
		reply *protocol.Packet
	}{
		{"remote error", &protocol.Packet{Type: protocol.TypeError, Payload: protocol.EncodePayload([]byte("connection refused"))}},
		{"unexpected type", &protocol.Packet{Type: protocol.TypeClose}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bot := newFakeBot()
			p := startProxy(t, bot)

			conn, sessionID, done := connectThroughProxy(t, p, bot)

			reply := *tc.reply
			reply.SessionID = sessionID
			bot.inbox <- &reply

			if got := readReply(t, conn); got[1] != 0x01 {
				t.Errorf("reply: got 0x%02x at byte 1, want 0x01", got[1])
			}
			waitDone(t, done)
		})
	}
}

func TestHandleConnectAckTimeout(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)
	p.ackTimeout = 50 * time.Millisecond

	conn, _, done := connectThroughProxy(t, p, bot)

	if reply := readReply(t, conn); reply[1] != 0x01 {
		t.Errorf("reply: got 0x%02x at byte 1, want 0x01", reply[1])
	}
	waitDone(t, done)
}

func TestHandleConnectSessionClosedWhileWaiting(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	conn, sessionID, done := connectThroughProxy(t, p, bot)

	sess := p.manager.Get(sessionID)
	if sess == nil {
		t.Fatal("session should be registered before the ACK wait")
	}
	sess.Close()

	if reply := readReply(t, conn); reply[1] != 0x01 {
		t.Errorf("reply: got 0x%02x at byte 1, want 0x01", reply[1])
	}
	waitDone(t, done)
}

func TestHandleConnectIdleReapTearsDownTunnel(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	conn, sessionID, done := connectThroughProxy(t, p, bot)
	bot.inbox <- &protocol.Packet{SessionID: sessionID, Type: protocol.TypeAck}
	readReply(t, conn)

	p.manager.Reap(0)
	waitDone(t, done)

	if err := conn.SetReadDeadline(time.Now().Add(waitTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Error("the browser connection should be closed after the session is reaped")
	}
}

func TestHandleConnectServerClosesSession(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	conn, sessionID, done := connectThroughProxy(t, p, bot)
	bot.inbox <- &protocol.Packet{SessionID: sessionID, Type: protocol.TypeAck}
	readReply(t, conn)

	bot.inbox <- &protocol.Packet{SessionID: sessionID, Type: protocol.TypeClose}
	waitDone(t, done)

	if err := conn.SetReadDeadline(time.Now().Add(waitTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Error("the browser connection should be closed after a CLOSE packet")
	}
}

func TestReceiveLoopRoutesToSession(t *testing.T) {
	bot := newFakeBot()
	p := startProxy(t, bot)

	bot.inbox <- &protocol.Packet{SessionID: "unknown", Type: protocol.TypeData}

	added := newSession(t, p, "known")
	bot.inbox <- &protocol.Packet{SessionID: "known", Type: protocol.TypeAck}

	select {
	case pkt := <-added.Incoming:
		if pkt.Type != protocol.TypeAck {
			t.Errorf("type: got %q, want %q", pkt.Type, protocol.TypeAck)
		}
	case <-time.After(waitTimeout):
		t.Fatal("packet never reached the session")
	}
}

func TestReceiveLoopStopsOnContextCancel(t *testing.T) {
	packets := make(chan *protocol.Packet)
	ctx, cancel := context.WithCancel(context.Background())

	p := New(newFakeBot(), noopLogger())
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.receiveLoop(ctx, packets)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("receiveLoop did not stop on cancel")
	}
}

func TestReceiveLoopStopsWhenPacketsClose(t *testing.T) {
	packets := make(chan *protocol.Packet)
	p := New(newFakeBot(), noopLogger())

	done := make(chan struct{})
	go func() {
		defer close(done)
		p.receiveLoop(context.Background(), packets)
	}()

	close(packets)
	select {
	case <-done:
	case <-time.After(waitTimeout):
		t.Fatal("receiveLoop did not stop when the packet channel closed")
	}
}
