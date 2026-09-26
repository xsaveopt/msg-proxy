package client_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
)

func duplicateAll(p *protocol.Packet, push func(*protocol.Packet) error) error {
	dup := *p
	if err := push(p); err != nil {
		return err
	}
	return push(&dup)
}

type reorderer struct {
	mu   sync.Mutex
	held *protocol.Packet
	hits atomic.Int32
}

func (r *reorderer) fault(p *protocol.Packet, push func(*protocol.Packet) error) error {
	if p.Type != protocol.TypeData {
		return push(p)
	}
	r.mu.Lock()
	if r.held == nil {
		r.held = p
		r.mu.Unlock()
		time.AfterFunc(20*time.Millisecond, func() {
			r.mu.Lock()
			mine := r.held == p
			if mine {
				r.held = nil
			}
			r.mu.Unlock()
			if mine {
				_ = push(p)
			}
		})
		return nil
	}
	held := r.held
	r.held = nil
	r.mu.Unlock()
	r.hits.Add(1)
	if err := push(p); err != nil {
		return err
	}
	return push(held)
}

type dackDropper struct {
	n       atomic.Int32
	dropped atomic.Int32
}

func (d *dackDropper) fault(p *protocol.Packet, push func(*protocol.Packet) error) error {
	if p.Type == protocol.TypeDataAck && d.n.Add(1)%2 == 1 {
		d.dropped.Add(1)
		return nil
	}
	return push(p)
}

func randomPayload(seed int64, size int) []byte {
	payload := make([]byte, size)
	rand.New(rand.NewSource(seed)).Read(payload)
	return payload
}

func echoRoundTrip(conn net.Conn, payload []byte) error {
	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(payload)
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		return fmt.Errorf("read echo: %w", err)
	}
	if err := <-writeErr; err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if !bytes.Equal(got, payload) {
		return fmt.Errorf("echoed %d bytes do not match what was sent", len(payload))
	}
	return nil
}

func TestEndToEndSurvivesDuplicatedPackets(t *testing.T) {
	target := echoTarget(t)
	clientBot, serverBot := newLinkPair()
	clientBot.fault = duplicateAll
	serverBot.fault = duplicateAll
	socksAddr := startTunnelOver(t, clientBot, serverBot)

	conn := dialThroughTunnel(t, socksAddr, target)
	if err := echoRoundTrip(conn, randomPayload(2, 3*protocol.MaxPayloadBytes+9)); err != nil {
		t.Fatal(err)
	}
	if err := echoRoundTrip(conn, []byte("after duplicates")); err != nil {
		t.Fatal(err)
	}
}

func TestEndToEndSurvivesReorderedData(t *testing.T) {
	target := echoTarget(t)
	clientBot, serverBot := newLinkPair()
	up, down := &reorderer{}, &reorderer{}
	clientBot.fault = up.fault
	serverBot.fault = down.fault
	socksAddr := startTunnelOver(t, clientBot, serverBot)

	conn := dialThroughTunnel(t, socksAddr, target)
	if err := echoRoundTrip(conn, randomPayload(3, 6*protocol.MaxPayloadBytes+1)); err != nil {
		t.Fatal(err)
	}
	if up.hits.Load() == 0 {
		t.Error("no client to server DATA packets were reordered, the fault never fired")
	}
}

func TestEndToEndSurvivesDroppedDacks(t *testing.T) {
	target := echoTarget(t)
	clientBot, serverBot := newLinkPair()
	up, down := &dackDropper{}, &dackDropper{}
	clientBot.fault = up.fault
	serverBot.fault = down.fault
	socksAddr := startTunnelOver(t, clientBot, serverBot)

	conn := dialThroughTunnel(t, socksAddr, target)
	for i, size := range []int{10, 2*protocol.MaxPayloadBytes + 3, 5} {
		if err := echoRoundTrip(conn, randomPayload(int64(i), size)); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
	if up.dropped.Load()+down.dropped.Load() == 0 {
		t.Error("no DACKs were dropped, the fault never fired")
	}
}

func TestEndToEndConcurrentSessions(t *testing.T) {
	target := echoTarget(t)
	clientBot, serverBot := newLinkPair()
	up, down := &reorderer{}, &reorderer{}
	clientBot.fault = up.fault
	serverBot.fault = down.fault
	socksAddr := startTunnelOver(t, clientBot, serverBot)

	const sessions = 8
	conns := make([]net.Conn, sessions)
	for i := range conns {
		conns[i] = dialThroughTunnel(t, socksAddr, target)
	}

	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i, conn := range conns {
		wg.Add(1)
		go func(i int, conn net.Conn) {
			defer wg.Done()
			payload := randomPayload(int64(100+i), 2*protocol.MaxPayloadBytes+i*37)
			if err := echoRoundTrip(conn, payload); err != nil {
				errs <- fmt.Errorf("session %d: %w", i, err)
			}
		}(i, conn)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
