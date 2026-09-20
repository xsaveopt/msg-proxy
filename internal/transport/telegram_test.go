package transport

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/stats"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

const (
	testChatID   = -1001234567890
	testMTPChanl = -(testChatID + 1000000000000)
)

type fakeInvoker struct {
	mu   sync.Mutex
	reqs []bin.Encoder
	fn   func(input bin.Encoder, output bin.Decoder) error
}

func (f *fakeInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	f.mu.Lock()
	f.reqs = append(f.reqs, input)
	f.mu.Unlock()
	if f.fn == nil {
		return errors.New("no fake response configured")
	}
	return f.fn(input, output)
}

func (f *fakeInvoker) requests() []bin.Encoder {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bin.Encoder(nil), f.reqs...)
}

type fakeAuth struct {
	status    *auth.Status
	statusErr error
	botErr    error

	mu       sync.Mutex
	botCalls int
	botToken string
}

func (f *fakeAuth) Status(context.Context) (*auth.Status, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	return f.status, nil
}

func (f *fakeAuth) Bot(_ context.Context, token string) (*tg.AuthAuthorization, error) {
	f.mu.Lock()
	f.botCalls++
	f.botToken = token
	f.mu.Unlock()
	if f.botErr != nil {
		return nil, f.botErr
	}
	return &tg.AuthAuthorization{}, nil
}

func (f *fakeAuth) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.botCalls
}

func noopLogger() *slog.Logger {
	return slog.New(noopHandler{})
}

type noopHandler struct{}

func (noopHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (noopHandler) Handle(context.Context, slog.Record) error { return nil }
func (noopHandler) WithAttrs(_ []slog.Attr) slog.Handler      { return noopHandler{} }
func (noopHandler) WithGroup(_ string) slog.Handler           { return noopHandler{} }

func chatsInvoker(chats ...tg.ChatClass) *fakeInvoker {
	return &fakeInvoker{fn: func(_ bin.Encoder, output bin.Decoder) error {
		box, ok := output.(*tg.MessagesChatsBox)
		if !ok {
			return errors.New("unexpected output type")
		}
		box.Chats = &tg.MessagesChats{Chats: chats}
		return nil
	}}
}

func selfFunc(id int64) func(context.Context) (*tg.User, error) {
	return func(context.Context) (*tg.User, error) {
		return &tg.User{ID: id, Username: "proxybot"}, nil
	}
}

func TestResolvePeerFromChats(t *testing.T) {
	inv := chatsInvoker(
		&tg.Chat{ID: 999},
		&tg.Channel{ID: testMTPChanl, AccessHash: 4242},
	)

	peer, err := resolvePeer(context.Background(), tg.NewClient(inv), testChatID)
	if err != nil {
		t.Fatalf("resolvePeer: %v", err)
	}
	if peer.ChannelID != testMTPChanl {
		t.Errorf("ChannelID: got %d, want %d", peer.ChannelID, int64(testMTPChanl))
	}
	if peer.AccessHash != 4242 {
		t.Errorf("AccessHash: got %d, want 4242", peer.AccessHash)
	}

	reqs := inv.requests()
	if len(reqs) != 1 {
		t.Fatalf("requests: got %d, want 1", len(reqs))
	}
	req, ok := reqs[0].(*tg.ChannelsGetChannelsRequest)
	if !ok {
		t.Fatalf("request type: got %T, want *tg.ChannelsGetChannelsRequest", reqs[0])
	}
	ch, ok := req.ID[0].(*tg.InputChannel)
	if !ok {
		t.Fatalf("input channel type: got %T", req.ID[0])
	}
	if ch.ChannelID != testMTPChanl {
		t.Errorf("requested channel: got %d, want %d", ch.ChannelID, int64(testMTPChanl))
	}
}

func TestResolvePeerFromChatsSlice(t *testing.T) {
	inv := &fakeInvoker{fn: func(_ bin.Encoder, output bin.Decoder) error {
		box, ok := output.(*tg.MessagesChatsBox)
		if !ok {
			return errors.New("unexpected output type")
		}
		box.Chats = &tg.MessagesChatsSlice{
			Count: 1,
			Chats: []tg.ChatClass{&tg.Channel{ID: testMTPChanl, AccessHash: 7}},
		}
		return nil
	}}

	peer, err := resolvePeer(context.Background(), tg.NewClient(inv), testChatID)
	if err != nil {
		t.Fatalf("resolvePeer: %v", err)
	}
	if peer.AccessHash != 7 {
		t.Errorf("AccessHash: got %d, want 7", peer.AccessHash)
	}
}

func TestResolvePeerNotFound(t *testing.T) {
	cases := map[string][]tg.ChatClass{
		"empty":         nil,
		"other channel": {&tg.Channel{ID: testMTPChanl + 1}},
		"not a channel": {&tg.Chat{ID: testMTPChanl}},
	}
	for name, chats := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := resolvePeer(context.Background(), tg.NewClient(chatsInvoker(chats...)), testChatID)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), "not found") {
				t.Errorf("error: got %q, want it to mention not found", err.Error())
			}
		})
	}
}

func TestResolvePeerInvokeError(t *testing.T) {
	inv := &fakeInvoker{fn: func(bin.Encoder, bin.Decoder) error {
		return errors.New("rpc exploded")
	}}

	_, err := resolvePeer(context.Background(), tg.NewClient(inv), testChatID)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "get channel") {
		t.Errorf("error: got %q, want it to mention get channel", err.Error())
	}
}

func TestSetupAuthorizesBot(t *testing.T) {
	b := &Bot{logger: noopLogger()}
	fa := &fakeAuth{status: &auth.Status{Authorized: false}}
	api := tg.NewClient(chatsInvoker(&tg.Channel{ID: testMTPChanl, AccessHash: 11}))

	if err := b.setup(context.Background(), fa, selfFunc(555), api, "bot-token", testChatID); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if fa.calls() != 1 {
		t.Errorf("Bot auth calls: got %d, want 1", fa.calls())
	}
	if fa.botToken != "bot-token" {
		t.Errorf("token: got %q, want %q", fa.botToken, "bot-token")
	}
	if b.selfID != 555 {
		t.Errorf("selfID: got %d, want 555", b.selfID)
	}
	if b.api != api {
		t.Error("api should be stored on the bot")
	}
	peer, ok := b.peer.(*tg.InputPeerChannel)
	if !ok {
		t.Fatalf("peer type: got %T", b.peer)
	}
	if peer.AccessHash != 11 {
		t.Errorf("peer AccessHash: got %d, want 11", peer.AccessHash)
	}
}

func TestSetupSkipsAuthWhenAuthorized(t *testing.T) {
	b := &Bot{logger: noopLogger()}
	fa := &fakeAuth{status: &auth.Status{Authorized: true}}
	api := tg.NewClient(chatsInvoker(&tg.Channel{ID: testMTPChanl}))

	if err := b.setup(context.Background(), fa, selfFunc(1), api, "bot-token", testChatID); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if fa.calls() != 0 {
		t.Errorf("Bot auth calls: got %d, want 0", fa.calls())
	}
}

func TestSetupErrors(t *testing.T) {
	okAPI := func() *tg.Client { return tg.NewClient(chatsInvoker(&tg.Channel{ID: testMTPChanl})) }
	badAPI := func() *tg.Client {
		return tg.NewClient(&fakeInvoker{fn: func(bin.Encoder, bin.Decoder) error {
			return errors.New("rpc exploded")
		}})
	}

	cases := []struct {
		name    string
		auth    *fakeAuth
		self    func(context.Context) (*tg.User, error)
		api     *tg.Client
		wantErr string
	}{
		{
			name:    "status fails",
			auth:    &fakeAuth{statusErr: errors.New("no connection")},
			self:    selfFunc(1),
			api:     okAPI(),
			wantErr: "auth status",
		},
		{
			name:    "bot auth fails",
			auth:    &fakeAuth{status: &auth.Status{}, botErr: errors.New("bad token")},
			self:    selfFunc(1),
			api:     okAPI(),
			wantErr: "bot auth",
		},
		{
			name: "self fails",
			auth: &fakeAuth{status: &auth.Status{Authorized: true}},
			self: func(context.Context) (*tg.User, error) {
				return nil, errors.New("who am i")
			},
			api:     okAPI(),
			wantErr: "get self",
		},
		{
			name:    "peer resolution fails",
			auth:    &fakeAuth{status: &auth.Status{Authorized: true}},
			self:    selfFunc(1),
			api:     badAPI(),
			wantErr: "resolve peer",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Bot{logger: noopLogger()}
			err := b.setup(context.Background(), tc.auth, tc.self, tc.api, "bot-token", testChatID)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error: got %q, want it to contain %q", err.Error(), tc.wantErr)
			}
			if b.peer != nil {
				t.Error("peer must stay unset on failure")
			}
		})
	}
}

func newUpdate(text string, channelID, fromID int64, date int) *tg.UpdateNewChannelMessage {
	msg := &tg.Message{
		Message: text,
		PeerID:  &tg.PeerChannel{ChannelID: channelID},
		Date:    date,
	}
	if fromID != 0 {
		msg.FromID = &tg.PeerUser{UserID: fromID}
	}
	return &tg.UpdateNewChannelMessage{Message: msg}
}

func TestOnNewChannelMessageAcceptsPacket(t *testing.T) {
	b := &Bot{
		logger:    noopLogger(),
		startTime: 1000,
		selfID:    99,
		updates:   make(chan *protocol.Packet, 4),
	}
	handler := b.onNewChannelMessage(testMTPChanl)

	text, err := protocol.Encode(&protocol.Packet{SessionID: "abc", Type: protocol.TypeData, Seq: 3})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	before := stats.Global.MsgRecv.Load()
	if err := handler(context.Background(), tg.Entities{}, newUpdate(text, testMTPChanl, 7, 2000)); err != nil {
		t.Fatalf("handler: %v", err)
	}

	select {
	case pkt := <-b.updates:
		if pkt.SessionID != "abc" || pkt.Seq != 3 {
			t.Errorf("packet: got %+v", pkt)
		}
	default:
		t.Fatal("no packet delivered")
	}
	if got := stats.Global.MsgRecv.Load(); got != before+1 {
		t.Errorf("MsgRecv: got %d, want %d", got, before+1)
	}
}

func TestOnNewChannelMessageDrops(t *testing.T) {
	text, err := protocol.Encode(&protocol.Packet{SessionID: "abc", Type: protocol.TypeData})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	cases := []struct {
		name   string
		update *tg.UpdateNewChannelMessage
	}{
		{"service message", &tg.UpdateNewChannelMessage{Message: &tg.MessageService{}}},
		{"empty text", newUpdate("", testMTPChanl, 7, 2000)},
		{"other channel", newUpdate(text, testMTPChanl+1, 7, 2000)},
		{"own message", newUpdate(text, testMTPChanl, 99, 2000)},
		{"stale message", newUpdate(text, testMTPChanl, 7, 1000)},
		{"not a packet", newUpdate("hello there", testMTPChanl, 7, 2000)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := &Bot{
				logger:    noopLogger(),
				startTime: 1000,
				selfID:    99,
				updates:   make(chan *protocol.Packet, 4),
			}
			if err := b.onNewChannelMessage(testMTPChanl)(context.Background(), tg.Entities{}, tc.update); err != nil {
				t.Fatalf("handler: %v", err)
			}
			if len(b.updates) != 0 {
				t.Errorf("expected the update to be dropped, got %d packets", len(b.updates))
			}
		})
	}
}

func TestOnNewChannelMessageDropsWhenFull(t *testing.T) {
	b := &Bot{
		logger:    noopLogger(),
		startTime: 1000,
		selfID:    99,
		updates:   make(chan *protocol.Packet, 1),
	}
	handler := b.onNewChannelMessage(testMTPChanl)

	text, err := protocol.Encode(&protocol.Packet{SessionID: "abc", Type: protocol.TypeData})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := handler(context.Background(), tg.Entities{}, newUpdate(text, testMTPChanl, 7, 2000)); err != nil {
			t.Fatalf("handler: %v", err)
		}
	}
	if len(b.updates) != 1 {
		t.Errorf("updates buffered: got %d, want 1", len(b.updates))
	}
}

func TestStartReceiverForwardsPackets(t *testing.T) {
	b := &Bot{updates: make(chan *protocol.Packet, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := b.StartReceiver(ctx)
	b.updates <- &protocol.Packet{SessionID: "one"}
	b.updates <- &protocol.Packet{SessionID: "two"}

	for _, want := range []string{"one", "two"} {
		select {
		case pkt := <-out:
			if pkt.SessionID != want {
				t.Errorf("session: got %q, want %q", pkt.SessionID, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}

	cancel()
	select {
	case _, ok := <-out:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not stop on cancel")
	}
}

func TestStartReceiverStopsWhenUpdatesClose(t *testing.T) {
	b := &Bot{updates: make(chan *protocol.Packet, 1)}
	out := b.StartReceiver(context.Background())

	close(b.updates)
	select {
	case _, ok := <-out:
		if ok {
			t.Error("channel should be closed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("receiver did not stop when updates closed")
	}
}

func TestStopIsIdempotent(t *testing.T) {
	runCtx, runCancel := context.WithCancel(context.Background())

	b := &Bot{
		queue:     NewSendQueue(func(string) error { return nil }),
		runCancel: runCancel,
		runCtx:    runCtx,
	}

	b.Stop()
	b.Stop()

	if err := runCtx.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("run context: got %v, want canceled", err)
	}
	select {
	case <-b.queue.stopCh:
	default:
		t.Error("the send queue should be stopped")
	}
}

func newQueuedBot(sendFn func(string) error) *Bot {
	runCtx, cancel := context.WithCancel(context.Background())
	b := &Bot{logger: noopLogger(), runCtx: runCtx, runCancel: cancel}
	b.queue = NewSendQueue(sendFn)
	return b
}

func TestSendVariantsEncodePackets(t *testing.T) {
	pkt := &protocol.Packet{SessionID: "sess", Seq: 9, Type: protocol.TypeData, Target: "example.com:80"}

	cases := map[string]func(b *Bot) error{
		"Send":              func(b *Bot) error { return b.Send(pkt) },
		"SendWait":          func(b *Bot) error { return b.SendWait(context.Background(), pkt) },
		"SendAsync":         func(b *Bot) error { return b.SendAsync(context.Background(), pkt) },
		"SendAsyncCallback": func(b *Bot) error { return b.SendAsyncCallback(context.Background(), pkt, func() {}) },
	}

	for name, send := range cases {
		t.Run(name, func(t *testing.T) {
			sent := make(chan string, 1)
			b := newQueuedBot(func(text string) error {
				sent <- text
				return nil
			})
			defer b.Stop()

			if err := send(b); err != nil {
				t.Fatalf("%s: %v", name, err)
			}

			select {
			case text := <-sent:
				got, err := protocol.Decode(text)
				if err != nil {
					t.Fatalf("decode: %v", err)
				}
				if got.SessionID != pkt.SessionID || got.Seq != pkt.Seq || got.Type != pkt.Type || got.Target != pkt.Target {
					t.Errorf("packet round trip: got %+v, want %+v", got, pkt)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("packet never reached the send function")
			}
		})
	}
}

func TestSendAsyncCallbackFiresOnSuccess(t *testing.T) {
	called := make(chan struct{}, 1)
	b := newQueuedBot(func(string) error { return nil })
	defer b.Stop()

	err := b.SendAsyncCallback(context.Background(), &protocol.Packet{SessionID: "s"}, func() {
		called <- struct{}{}
	})
	if err != nil {
		t.Fatalf("SendAsyncCallback: %v", err)
	}

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("callback not invoked")
	}
}

func TestSendWaitPropagatesError(t *testing.T) {
	b := newQueuedBot(func(string) error { return errors.New("telegram is down") })
	defer b.Stop()

	err := b.SendWait(context.Background(), &protocol.Packet{SessionID: "s"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "telegram is down") {
		t.Errorf("error: got %q", err.Error())
	}
}

func TestRawSend(t *testing.T) {
	var gotText string
	inv := &fakeInvoker{fn: func(input bin.Encoder, output bin.Decoder) error {
		if req, ok := input.(*tg.MessagesSendMessageRequest); ok {
			gotText = req.Message
		}
		box, ok := output.(*tg.UpdatesBox)
		if !ok {
			return errors.New("unexpected output type")
		}
		box.Updates = &tg.Updates{}
		return nil
	}}

	b := &Bot{
		logger: noopLogger(),
		runCtx: context.Background(),
		api:    tg.NewClient(inv),
		peer:   &tg.InputPeerChannel{ChannelID: testMTPChanl, AccessHash: 1},
	}

	before := stats.Global.MsgSent.Load()
	if err := b.rawSend("hello"); err != nil {
		t.Fatalf("rawSend: %v", err)
	}
	if gotText != "hello" {
		t.Errorf("message text: got %q, want %q", gotText, "hello")
	}
	if got := stats.Global.MsgSent.Load(); got != before+1 {
		t.Errorf("MsgSent: got %d, want %d", got, before+1)
	}
}

func TestRawSendError(t *testing.T) {
	inv := &fakeInvoker{fn: func(bin.Encoder, bin.Decoder) error {
		return errors.New("flood wait")
	}}
	b := &Bot{
		logger: noopLogger(),
		runCtx: context.Background(),
		api:    tg.NewClient(inv),
		peer:   &tg.InputPeerChannel{ChannelID: testMTPChanl},
	}

	before := stats.Global.MsgSent.Load()
	err := b.rawSend("hello")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "send message") {
		t.Errorf("error: got %q, want it to mention send message", err.Error())
	}
	if got := stats.Global.MsgSent.Load(); got != before {
		t.Errorf("MsgSent must not move on failure: got %d, want %d", got, before)
	}
}
