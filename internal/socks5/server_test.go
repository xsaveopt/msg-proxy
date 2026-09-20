package socks5

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
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

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

func waitForListener(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
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

	client, err = net.DialTimeout("tcp", ln.Addr().String(), 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	select {
	case server = <-accepted:
	case err := <-errCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("accept timed out")
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

func TestListenAndServe(t *testing.T) {
	addr := freeAddr(t)
	got := make(chan ConnectRequest, 1)
	srv := New(addr, noopLogger(), func(conn net.Conn, req ConnectRequest) {
		got <- req
		_ = SendSuccess(conn)
		_ = conn.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe(ctx) }()
	waitForListener(t, addr)

	conn, rep := dialSocks5(t, addr, "example.com:443")
	_ = conn.Close()
	if rep != 0x00 {
		t.Errorf("reply: got 0x%02x, want 0x00", rep)
	}

	select {
	case req := <-got:
		if req.Target != "example.com:443" {
			t.Errorf("target: got %q, want %q", req.Target, "example.com:443")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler not called")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("ListenAndServe after cancel: got %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServe did not return after cancel")
	}
}

func TestListenAndServeBadAddr(t *testing.T) {
	srv := New("127.0.0.1:99999", noopLogger(), func(net.Conn, ConnectRequest) {
		t.Error("handler must not be called")
	})
	err := srv.ListenAndServe(context.Background())
	if err == nil {
		t.Fatal("expected an error for an invalid address")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error: got %q, want it to mention listen", err.Error())
	}
}

func TestServeHandshakeFailureClosesConn(t *testing.T) {
	srv := New("", noopLogger(), func(net.Conn, ConnectRequest) {
		t.Error("handler must not be called on a failed handshake")
	})

	client, server := tcpPair(t)
	done := make(chan struct{})
	go func() {
		srv.serve(server)
		close(done)
	}()

	if _, err := client.Write([]byte{0x04, 0x01, 0x00}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Error("expected the connection to be closed")
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serve did not return")
	}
}

func TestServeRecoversHandlerPanic(t *testing.T) {
	srv := New("", noopLogger(), func(net.Conn, ConnectRequest) {
		panic("boom")
	})

	client, server := tcpPair(t)
	done := make(chan struct{})
	go func() {
		srv.serve(server)
		close(done)
	}()

	if _, err := client.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if _, err := io.ReadFull(client, make([]byte, 2)); err != nil {
		t.Fatalf("read auth reply: %v", err)
	}
	req := append([]byte{0x05, 0x01, 0x00, 0x03, byte(len("host"))}, []byte("host")...)
	req = append(req, 0x00, 0x50)
	if _, err := client.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serve did not return after the handler panicked")
	}

	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Error("expected the connection to be closed after a panic")
	}
}

func TestHandshakeIPv6(t *testing.T) {
	client, server := tcpPair(t)

	result := make(chan *ConnectRequest, 1)
	errCh := make(chan error, 1)
	go func() {
		req, err := Handshake(server)
		if err != nil {
			errCh <- err
			return
		}
		result <- req
	}()

	if _, err := client.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		t.Fatalf("write auth: %v", err)
	}
	if _, err := io.ReadFull(client, make([]byte, 2)); err != nil {
		t.Fatalf("read auth reply: %v", err)
	}

	addr := net.ParseIP("2001:db8::1").To16()
	req := append([]byte{0x05, 0x01, 0x00, 0x04}, addr...)
	req = append(req, 0x01, 0xBB)
	if _, err := client.Write(req); err != nil {
		t.Fatalf("write request: %v", err)
	}

	select {
	case got := <-result:
		if got.Target != "2001:db8::1:443" {
			t.Errorf("target: got %q, want %q", got.Target, "2001:db8::1:443")
		}
	case err := <-errCh:
		t.Fatalf("Handshake: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Handshake did not return")
	}
}

func TestHandshakeErrors(t *testing.T) {
	cases := []struct {
		name    string
		bytes   []byte
		wantErr string
	}{
		{"empty", nil, "read auth header"},
		{"bad version", []byte{0x04, 0x01, 0x00}, "unsupported SOCKS version"},
		{"truncated methods", []byte{0x05, 0x03, 0x00}, "read methods"},
		{"truncated request header", []byte{0x05, 0x01, 0x00, 0x05, 0x01}, "read request header"},
		{"bad request version", []byte{0x05, 0x01, 0x00, 0x04, 0x01, 0x00, 0x03}, "unsupported SOCKS version in request"},
		{"bad command", []byte{0x05, 0x01, 0x00, 0x05, 0x02, 0x00, 0x03}, "unsupported command"},
		{"unknown atyp", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x09}, "unknown address type"},
		{"truncated ipv4", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x01, 0x01, 0x02}, "read IPv4"},
		{"truncated ipv6", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x04, 0x01}, "read IPv6"},
		{"truncated domain length", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x03}, "read domain length"},
		{"truncated domain", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x03, 0x08, 'a', 'b'}, "read domain"},
		{"truncated port", []byte{0x05, 0x01, 0x00, 0x05, 0x01, 0x00, 0x01, 1, 2, 3, 4, 0x01}, "read port"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, server := tcpPair(t)

			errCh := make(chan error, 1)
			go func() {
				_, err := Handshake(server)
				errCh <- err
			}()

			if len(tc.bytes) > 0 {
				if _, err := client.Write(tc.bytes); err != nil {
					t.Fatalf("write: %v", err)
				}
			}
			if err := client.(*net.TCPConn).CloseWrite(); err != nil {
				t.Fatalf("close write: %v", err)
			}

			select {
			case err := <-errCh:
				if err == nil {
					t.Fatal("expected an error")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error: got %q, want it to contain %q", err.Error(), tc.wantErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Handshake did not return")
			}
		})
	}
}

func TestSendSuccessBytes(t *testing.T) {
	client, server := tcpPair(t)

	if err := SendSuccess(server); err != nil {
		t.Fatalf("SendSuccess: %v", err)
	}
	got := make([]byte, 10)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	want := []byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Errorf("reply: got %v, want %v", got, want)
	}
}

func TestSendSuccessOnClosedConn(t *testing.T) {
	_, server := tcpPair(t)
	if err := server.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := SendSuccess(server); err == nil {
		t.Error("expected an error writing to a closed connection")
	}
}

func TestSendFailureBytesAndClose(t *testing.T) {
	client, server := tcpPair(t)

	SendFailure(server)

	got := make([]byte, 10)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	want := []byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Errorf("reply: got %v, want %v", got, want)
	}

	if err := client.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Error("SendFailure should close the connection")
	}
}
