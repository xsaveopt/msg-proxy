package reliable

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/transport"
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

type flakyBot struct {
	mockBot
	failOn int32
	calls  atomic.Int32
}

func (f *flakyBot) SendAsyncCallback(ctx context.Context, p *protocol.Packet, onSent func()) error {
	if f.calls.Add(1) == f.failOn {
		return errors.New("enqueue failed")
	}
	return f.mockBot.SendAsyncCallback(ctx, p, onSent)
}

type queueSender struct {
	q *transport.SendQueue
}

func (s queueSender) encode(p *protocol.Packet) string {
	text, _ := protocol.Encode(p)
	return text
}

func (s queueSender) Send(p *protocol.Packet) error { return s.q.Enqueue(s.encode(p)) }

func (s queueSender) SendAsync(ctx context.Context, p *protocol.Packet) error {
	return s.q.EnqueueAsync(ctx, s.encode(p))
}

func (s queueSender) SendAsyncCallback(ctx context.Context, p *protocol.Packet, onSent func()) error {
	return s.q.EnqueueAsyncCallback(ctx, s.encode(p), onSent)
}

func (s queueSender) SendWait(ctx context.Context, p *protocol.Packet) error {
	return s.q.EnqueueWait(ctx, s.encode(p))
}

func dataPacket(id string, seq uint32, payload []byte) *protocol.Packet {
	return &protocol.Packet{
		SessionID: id,
		Seq:       seq,
		Type:      protocol.TypeData,
		Payload:   protocol.EncodePayload(payload),
	}
}

func readWithin(t *testing.T, s *Stream) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, err := s.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return data
}

func lastDack(bot *mockBot) (*protocol.Packet, int) {
	var last *protocol.Packet
	n := 0
	for _, p := range bot.getSent() {
		if p.Type == protocol.TypeDataAck {
			last = p
			n++
		}
	}
	return last, n
}

func TestStreamReDacksAlreadyDeliveredSeq(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-redack", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	s.Deliver(ctx, dataPacket("sess-redack", 0, []byte("a")))
	s.Deliver(ctx, dataPacket("sess-redack", 1, []byte("b")))
	readWithin(t, s)
	readWithin(t, s)

	_, before := lastDack(bot)
	s.Deliver(ctx, dataPacket("sess-redack", 0, []byte("a")))

	last, after := lastDack(bot)
	if after != before+1 {
		t.Fatalf("DACKs: got %d after replaying seq 0, want %d", after, before+1)
	}
	if last.Seq != 1 {
		t.Errorf("re-DACK seq: got %d, want 1", last.Seq)
	}
	select {
	case extra := <-s.readyCh:
		t.Errorf("replayed chunk was delivered again: %q", extra)
	default:
	}
}

func TestStreamDropsCorruptPayload(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-corrupt", bot, 0)
	defer s.Stop()
	ctx := context.Background()

	for _, payload := range []string{"***not base64***", "aGVsbG8="} {
		s.Deliver(ctx, &protocol.Packet{SessionID: "sess-corrupt", Seq: 0, Type: protocol.TypeData, Payload: payload})
	}

	select {
	case data := <-s.readyCh:
		t.Fatalf("corrupt payload produced data: %q", data)
	default:
	}
	if _, n := lastDack(bot); n != 0 {
		t.Fatalf("corrupt payload was DACKed %d times", n)
	}

	s.Deliver(ctx, dataPacket("sess-corrupt", 0, []byte("clean")))
	if got := readWithin(t, s); string(got) != "clean" {
		t.Errorf("got %q, want %q", got, "clean")
	}
	if last, _ := lastDack(bot); last == nil || last.Seq != 0 {
		t.Errorf("expected a DACK for seq 0 after the clean retransmit, got %+v", last)
	}
}

func TestStreamMultiChunkRoundTrip(t *testing.T) {
	senderBot := &mockBot{}
	sender := New("sess-multi", senderBot, 0)
	defer sender.Stop()
	receiver := New("sess-multi", &mockBot{}, 0)
	defer receiver.Stop()
	ctx := context.Background()

	payload := bytes.Repeat([]byte("0123456789"), (2*protocol.MaxPayloadBytes+5)/10+1)
	if err := sender.Send(ctx, payload); err != nil {
		t.Fatalf("Send: %v", err)
	}

	sent := senderBot.getSent()
	wantChunks := len(protocol.SplitData(payload))
	if len(sent) != wantChunks {
		t.Fatalf("DATA packets: got %d, want %d", len(sent), wantChunks)
	}
	for i := len(sent) - 1; i >= 0; i-- {
		if sent[i].Seq != uint32(i) {
			t.Errorf("packet %d seq: got %d, want %d", i, sent[i].Seq, i)
		}
		receiver.Deliver(ctx, sent[i])
	}

	var got []byte
	for len(got) < len(payload) {
		got = append(got, readWithin(t, receiver)...)
	}
	if !bytes.Equal(got, payload) {
		t.Error("reassembled payload does not match what was sent")
	}
}

func TestStreamSendStopsAtFirstFailedChunk(t *testing.T) {
	bot := &flakyBot{failOn: 2}
	s := New("sess-partial", bot, 0)
	defer s.Stop()

	payload := bytes.Repeat([]byte("x"), 3*protocol.MaxPayloadBytes)
	if err := s.Send(context.Background(), payload); err == nil {
		t.Fatal("expected Send to report the failed chunk")
	}

	if got := bot.calls.Load(); got != 2 {
		t.Errorf("enqueue attempts: got %d, want 2", got)
	}
	sent := bot.getSent()
	if len(sent) != 1 || sent[0].Seq != 0 {
		t.Fatalf("expected only seq 0 on the wire, got %d packets", len(sent))
	}
}

func TestStreamFullReadyChDoesNotLoseData(t *testing.T) {
	bot := &mockBot{}
	s := New("sess-full", bot, 0)
	defer s.Stop()

	capacity := cap(s.readyCh)
	for i := 0; i < capacity; i++ {
		s.Deliver(context.Background(), dataPacket("sess-full", uint32(i), []byte{byte(i)}))
	}

	overflow := uint32(capacity)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	s.Deliver(ctx, dataPacket("sess-full", overflow, []byte("overflow")))
	cancel()

	for i := 0; i < capacity; i++ {
		readWithin(t, s)
	}

	s.Deliver(context.Background(), dataPacket("sess-full", overflow, []byte("overflow")))

	readCtx, readCancel := context.WithTimeout(context.Background(), time.Second)
	defer readCancel()
	data, err := s.Read(readCtx)
	if err != nil {
		t.Fatalf("chunk %d was dropped when readyCh was full and its retransmit was treated as a duplicate: %v", overflow, err)
	}
	if string(data) != "overflow" {
		t.Errorf("got %q, want %q", data, "overflow")
	}
}

func TestStreamDeliverOnFullReadyChUnblocksOnStop(t *testing.T) {
	s := New("sess-block", &mockBot{}, 0)

	for i := 0; i < cap(s.readyCh); i++ {
		s.Deliver(context.Background(), dataPacket("sess-block", uint32(i), []byte{byte(i)}))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Deliver(context.Background(), dataPacket("sess-block", uint32(cap(s.readyCh)), []byte("blocked")))
	}()

	time.Sleep(20 * time.Millisecond)
	s.Stop()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Deliver stayed blocked on a full readyCh after Stop")
	}
}

func TestStreamRecoversFromFailedQueueSend(t *testing.T) {
	var attempts atomic.Int32
	delivered := make(chan *protocol.Packet, 16)
	q := transport.NewSendQueue(func(text string) error {
		pkt, err := protocol.Decode(text)
		if err != nil {
			return err
		}
		if pkt.Type == protocol.TypeData && attempts.Add(1) == 1 {
			return errors.New("rate limited")
		}
		delivered <- pkt
		return nil
	})
	defer q.Stop()

	s := New("sess-queue", queueSender{q: q}, 40*time.Millisecond)
	defer s.Stop()

	if err := s.Send(context.Background(), []byte("must arrive")); err != nil {
		t.Fatalf("Send: %v", err)
	}

	deadline := time.After(8 * time.Second)
	for {
		select {
		case pkt := <-delivered:
			if pkt.Type == protocol.TypeData && pkt.Seq == 0 {
				return
			}
		case <-deadline:
			t.Fatalf("DATA seq 0 never reached the wire after its first send failed (attempts: %d)", attempts.Load())
		}
	}
}
