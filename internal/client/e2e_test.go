package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"msg-proxy/internal/client"
	"msg-proxy/internal/protocol"
	"msg-proxy/internal/server"
)

const e2eTimeout = 10 * time.Second

func e2eLogger() *slog.Logger {
	return slog.New(silentHandler{})
}

type silentHandler struct{}

func (silentHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (silentHandler) Handle(context.Context, slog.Record) error { return nil }
func (silentHandler) WithAttrs(_ []slog.Attr) slog.Handler      { return silentHandler{} }
func (silentHandler) WithGroup(_ string) slog.Handler           { return silentHandler{} }

type linkBot struct {
	peer  *linkBot
	inbox chan *protocol.Packet
	done  chan struct{}
	once  sync.Once
}

func newLinkPair() (*linkBot, *linkBot) {
	a := &linkBot{inbox: make(chan *protocol.Packet, 1024), done: make(chan struct{})}
	b := &linkBot{inbox: make(chan *protocol.Packet, 1024), done: make(chan struct{})}
	a.peer, b.peer = b, a
	return a, b
}

func (l *linkBot) stop() {
	l.once.Do(func() { close(l.done) })
}

func (l *linkBot) deliver(p *protocol.Packet) error {
	text, err := protocol.Encode(p)
	if err != nil {
		return err
	}
	clone, err := protocol.Decode(text)
	if err != nil {
		return err
	}
	select {
	case l.peer.inbox <- clone:
		return nil
	case <-l.done:
		return errors.New("bot stopped")
	case <-l.peer.done:
		return errors.New("peer stopped")
	}
}

func (l *linkBot) Send(p *protocol.Packet) error { return l.deliver(p) }

func (l *linkBot) SendAsync(_ context.Context, p *protocol.Packet) error { return l.deliver(p) }

func (l *linkBot) SendWait(_ context.Context, p *protocol.Packet) error { return l.deliver(p) }

func (l *linkBot) SendAsyncCallback(_ context.Context, p *protocol.Packet, onSent func()) error {
	err := l.deliver(p)
	if err == nil && onSent != nil {
		onSent()
	}
	return err
}

func (l *linkBot) StartReceiver(ctx context.Context) <-chan *protocol.Packet {
	out := make(chan *protocol.Packet, 64)
	go func() {
		defer close(out)
		for {
			select {
			case pkt := <-l.inbox:
				select {
				case out <- pkt:
				case <-ctx.Done():
					return
				case <-l.done:
					return
				}
			case <-ctx.Done():
				return
			case <-l.done:
				return
			}
		}
	}()
	return out
}

func echoTarget(t *testing.T) string {
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

func freeAddr(t *testing.T) string {
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

func startTunnel(t *testing.T) string {
	t.Helper()

	clientBot, serverBot := newLinkPair()
	ctx, cancel := context.WithCancel(context.Background())

	srv := server.New(serverBot, e2eLogger())
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		srv.Run(ctx, time.Minute)
	}()

	socksAddr := freeAddr(t)
	cli := client.New(clientBot, e2eLogger())
	clientDone := make(chan error, 1)
	go func() {
		clientDone <- cli.Run(ctx, socksAddr, time.Minute)
	}()

	t.Cleanup(func() {
		cancel()
		clientBot.stop()
		serverBot.stop()
		select {
		case err := <-clientDone:
			if err != nil {
				t.Errorf("client Run: %v", err)
			}
		case <-time.After(e2eTimeout):
			t.Error("client Run did not stop")
		}
		select {
		case <-serverDone:
		case <-time.After(e2eTimeout):
			t.Error("server Run did not stop")
		}
	})

	waitForListener(t, socksAddr)
	return socksAddr
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(e2eTimeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("nothing listening on %s", addr)
}

func dialThroughTunnel(t *testing.T, socksAddr, target string) net.Conn {
	t.Helper()

	conn, err := net.DialTimeout("tcp", socksAddr, e2eTimeout)
	if err != nil {
		t.Fatalf("dial SOCKS5: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(e2eTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		t.Fatalf("read greeting reply: %v", err)
	}
	if greeting[0] != 0x05 || greeting[1] != 0x00 {
		t.Fatalf("greeting reply: got %v, want [5 0]", greeting)
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("split target: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}

	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(port))
	req = append(req, portBytes...)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read CONNECT reply: %v", err)
	}
	if reply[1] != 0x00 {
		t.Fatalf("CONNECT reply: got 0x%02x at byte 1, want 0x00", reply[1])
	}
	return conn
}

func TestEndToEndEcho(t *testing.T) {
	target := echoTarget(t)
	socksAddr := startTunnel(t)

	conn := dialThroughTunnel(t, socksAddr, target)

	for _, msg := range []string{"hello through telegram", "and again"} {
		if _, err := conn.Write([]byte(msg)); err != nil {
			t.Fatalf("write %q: %v", msg, err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("read echo of %q: %v", msg, err)
		}
		if string(got) != msg {
			t.Errorf("echo: got %q, want %q", got, msg)
		}
	}
}

func TestEndToEndLargePayloadIsChunkedAndReassembled(t *testing.T) {
	target := echoTarget(t)
	socksAddr := startTunnel(t)

	conn := dialThroughTunnel(t, socksAddr, target)

	payload := make([]byte, 4*protocol.MaxPayloadBytes+17)
	rng := rand.New(rand.NewSource(1))
	if _, err := rng.Read(payload); err != nil {
		t.Fatalf("generate payload: %v", err)
	}

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(payload)
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Error("echoed payload does not match what was sent")
	}
}

func TestEndToEndTargetRefused(t *testing.T) {
	socksAddr := startTunnel(t)
	dead := freeAddr(t)

	conn, err := net.DialTimeout("tcp", socksAddr, e2eTimeout)
	if err != nil {
		t.Fatalf("dial SOCKS5: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(e2eTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write greeting: %v", err)
	}
	if _, err := io.ReadFull(conn, make([]byte, 2)); err != nil {
		t.Fatalf("read greeting reply: %v", err)
	}

	host, portStr, err := net.SplitHostPort(dead)
	if err != nil {
		t.Fatalf("split target: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, host...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(port))
	req = append(req, portBytes...)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}

	reply := make([]byte, 10)
	if _, err := io.ReadFull(conn, reply); err != nil {
		t.Fatalf("read CONNECT reply: %v", err)
	}
	if reply[1] != 0x01 {
		t.Errorf("CONNECT reply: got 0x%02x at byte 1, want 0x01", reply[1])
	}
}

func TestEndToEndClientCloseTearsDownTarget(t *testing.T) {
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

	socksAddr := startTunnel(t)
	conn := dialThroughTunnel(t, socksAddr, ln.Addr().String())

	var target net.Conn
	select {
	case target = <-accepted:
	case <-time.After(e2eTimeout):
		t.Fatal("target was never dialled")
	}
	t.Cleanup(func() { _ = target.Close() })

	if err := conn.Close(); err != nil {
		t.Fatalf("close SOCKS5 conn: %v", err)
	}

	if err := target.SetReadDeadline(time.Now().Add(e2eTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := target.Read(make([]byte, 1)); err == nil {
		t.Error("the target connection should be closed once the client hangs up")
	}
}
