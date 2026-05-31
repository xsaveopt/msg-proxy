package socks5

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"testing"
	"time"
)

func dialSocks5(t *testing.T, addr, target string) (net.Conn, byte) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	host, portStr, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatalf("split host/port: %v", err)
	}
	var port uint16
	portN, _ := net.LookupPort("tcp", portStr)
	port = uint16(portN)

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	authReply := make([]byte, 2)
	if _, err := conn.Read(authReply); err != nil {
		t.Fatalf("read auth reply: %v", err)
	}
	if authReply[0] != 0x05 || authReply[1] != 0x00 {
		t.Fatalf("unexpected auth reply: %v", authReply)
	}

	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, []byte(host)...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, port)
	req = append(req, portBytes...)
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	reply := make([]byte, 10)
	if _, err := conn.Read(reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}
	return conn, reply[1]
}

func TestHandshakeSuccess(t *testing.T) {
	got := make(chan ConnectRequest, 1)
	srv := New("127.0.0.1:0", noopLogger(), func(conn net.Conn, req ConnectRequest) {
		got <- req
		_ = SendSuccess(conn)
		_ = conn.Close()
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.serve(conn)
		}
	}()
	defer func() { _ = ln.Close() }()

	conn, rep := dialSocks5(t, ln.Addr().String(), "example.com:80")
	defer func() { _ = conn.Close() }()

	if rep != 0x00 {
		t.Errorf("expected success (0x00), got 0x%02x", rep)
	}

	select {
	case req := <-got:
		if req.Target != "example.com:80" {
			t.Errorf("target: got %q, want %q", req.Target, "example.com:80")
		}
	case <-time.After(time.Second):
		t.Error("handler not called")
	}
}

func TestHandshakeIPv4(t *testing.T) {
	got := make(chan ConnectRequest, 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	srv := New("", noopLogger(), func(conn net.Conn, req ConnectRequest) {
		got <- req
		_ = SendSuccess(conn)
		_ = conn.Close()
	})

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go srv.serve(conn)
	}()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	authReply := make([]byte, 2)
	if _, err := conn.Read(authReply); err != nil {
		t.Fatalf("read auth reply: %v", err)
	}

	req := []byte{0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0x01, 0xBB}
	if _, err := conn.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	reply := make([]byte, 10)
	if _, err := conn.Read(reply); err != nil {
		t.Fatalf("read reply: %v", err)
	}

	select {
	case r := <-got:
		if r.Target != "1.2.3.4:443" {
			t.Errorf("target: got %q, want %q", r.Target, "1.2.3.4:443")
		}
	case <-time.After(time.Second):
		t.Error("handler not called")
	}
}

func noopLogger() *slog.Logger {
	return slog.New(noopHandler{})
}

type noopHandler struct{}

func (noopHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (noopHandler) Handle(context.Context, slog.Record) error { return nil }
func (noopHandler) WithAttrs(_ []slog.Attr) slog.Handler      { return noopHandler{} }
func (noopHandler) WithGroup(_ string) slog.Handler           { return noopHandler{} }
