package reliable

import (
	"context"
	"fmt"
	"sync"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/stats"
)

const RetransmitTimeout = 30 * time.Second

type Sender interface {
	Send(p *protocol.Packet) error
	SendAsync(ctx context.Context, p *protocol.Packet) error
	SendAsyncCallback(ctx context.Context, p *protocol.Packet, onSent func()) error
	SendWait(ctx context.Context, p *protocol.Packet) error
}

type pending struct {
	pkt    *protocol.Packet
	sent   bool
	sentAt time.Time
}

type Stream struct {
	id  string
	bot Sender

	sendMu  sync.Mutex
	nextSeq uint32
	unacked map[uint32]*pending

	recvMu     sync.Mutex
	nextExpect uint32
	recvBuf    map[uint32][]byte
	readyCh    chan []byte

	retransmitTimeout time.Duration
	stopCh            chan struct{}
	stopOnce          sync.Once
}

func New(id string, bot Sender, retransmitTimeout time.Duration) *Stream {
	if retransmitTimeout <= 0 {
		retransmitTimeout = RetransmitTimeout
	}
	s := &Stream{
		id:                id,
		bot:               bot,
		unacked:           make(map[uint32]*pending),
		recvBuf:           make(map[uint32][]byte),
		readyCh:           make(chan []byte, 256),
		retransmitTimeout: retransmitTimeout,
		stopCh:            make(chan struct{}),
	}
	go s.retransmitLoop()
	return s
}

func (s *Stream) Send(ctx context.Context, data []byte) error {
	for _, chunk := range protocol.SplitData(data) {
		s.sendMu.Lock()
		seq := s.nextSeq
		s.nextSeq++
		pkt := &protocol.Packet{
			SessionID: s.id,
			Seq:       seq,
			Type:      protocol.TypeData,
			Payload:   protocol.EncodePayload(chunk),
		}
		p := &pending{pkt: pkt}
		s.unacked[seq] = p
		s.sendMu.Unlock()

		capturedP := p
		capturedSeq := seq
		onSent := func() {
			s.sendMu.Lock()
			if q, ok := s.unacked[capturedSeq]; ok && q == capturedP {
				capturedP.sent = true
				capturedP.sentAt = time.Now()
			}
			s.sendMu.Unlock()
		}

		if err := s.bot.SendAsyncCallback(ctx, pkt, onSent); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stream) Deliver(ctx context.Context, pkt *protocol.Packet) {
	switch pkt.Type {
	case protocol.TypeData:
		s.deliverData(ctx, pkt)
	case protocol.TypeDataAck:
		s.handleDack(pkt.Seq)
	}
}

func (s *Stream) Read(ctx context.Context) ([]byte, error) {
	select {
	case data := <-s.readyCh:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.stopCh:
		return nil, fmt.Errorf("stream stopped")
	}
}

func (s *Stream) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

func (s *Stream) deliverData(ctx context.Context, pkt *protocol.Packet) {
	data, err := protocol.DecodePayload(pkt.Payload)
	if err != nil {
		return
	}

	s.recvMu.Lock()
	seq := pkt.Seq

	if seq < s.nextExpect {
		ackSeq := s.nextExpect - 1
		s.recvMu.Unlock()
		s.sendDack(ackSeq)
		return
	}

	s.recvBuf[seq] = data

	var ready [][]byte
	for {
		chunk, ok := s.recvBuf[s.nextExpect]
		if !ok {
			break
		}
		delete(s.recvBuf, s.nextExpect)
		ready = append(ready, chunk)
		s.nextExpect++
	}

	var ackSeq uint32
	hasAck := len(ready) > 0
	if hasAck {
		ackSeq = s.nextExpect - 1
	}
	s.recvMu.Unlock()

	for _, chunk := range ready {
		select {
		case s.readyCh <- chunk:
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}

	if hasAck {
		s.sendDack(ackSeq)
	}
}

func (s *Stream) handleDack(ackSeq uint32) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	for seq := range s.unacked {
		if seq <= ackSeq {
			delete(s.unacked, seq)
		}
	}
}

func (s *Stream) sendDack(ackSeq uint32) {
	// Send is fire-and-forget; a dropped DACK just delays peer cleanup until
	// the next cumulative DACK or retransmit.
	_ = s.bot.Send(&protocol.Packet{
		SessionID: s.id,
		Seq:       ackSeq,
		Type:      protocol.TypeDataAck,
	})
}

func (s *Stream) retransmitLoop() {
	ticker := time.NewTicker(s.retransmitTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.retransmit()
		}
	}
}

func (s *Stream) retransmit() {
	now := time.Now()
	s.sendMu.Lock()
	var toSend []*protocol.Packet
	for _, p := range s.unacked {
		if !p.sent {
			continue
		}
		if now.Sub(p.sentAt) >= s.retransmitTimeout {
			p.sentAt = now
			toSend = append(toSend, p.pkt)
		}
	}
	s.sendMu.Unlock()

	if len(toSend) > 0 {
		stats.Global.Retransmits.Add(int64(len(toSend)))
	}
	for _, pkt := range toSend {
		// Fire-and-forget; the retransmit loop will pick the packet up again
		// on the next tick if this Send fails.
		_ = s.bot.Send(pkt)
	}
}
