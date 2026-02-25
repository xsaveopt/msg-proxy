package transport

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/stats"

	"github.com/gotd/contrib/middleware/floodwait"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
)

type Bot struct {
	chatID    int64
	startTime int
	logger    *slog.Logger
	queue     *SendQueue
	api       *tg.Client
	peer      tg.InputPeerClass
	selfID    int64
	updates   chan *protocol.Packet
	runCtx    context.Context
	runCancel context.CancelFunc
	stopOnce  sync.Once
}

func NewBot(token string, appID int, appHash string, chatID int64, logger *slog.Logger) (*Bot, error) {
	runCtx, cancel := context.WithCancel(context.Background())
	b := &Bot{
		chatID:    chatID,
		startTime: int(time.Now().Unix()),
		logger:    logger,
		updates:   make(chan *protocol.Packet, 128),
		runCtx:    runCtx,
		runCancel: cancel,
	}

	mtpChannelID := -(chatID + 1000000000000)

	disp := tg.NewUpdateDispatcher()
	disp.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
		msg, ok := u.Message.(*tg.Message)
		if !ok || msg.Message == "" {
			return nil
		}
		ch, ok := msg.PeerID.(*tg.PeerChannel)
		if !ok || ch.ChannelID != mtpChannelID {
			return nil
		}
		if from, ok := msg.FromID.(*tg.PeerUser); ok && from.UserID == b.selfID {
			return nil
		}
		if msg.Date <= b.startTime {
			b.logger.Debug("dropping stale message", "msg_date", msg.Date, "start_time", b.startTime)
			return nil
		}
		pkt, err := protocol.Decode(msg.Message)
		if err != nil {
			b.logger.Debug("ignoring non-packet message", "err", err)
			return nil
		}
		stats.Global.MsgRecv.Add(1)
		select {
		case b.updates <- pkt:
		default:
			b.logger.Warn("update channel full, dropping packet")
		}
		return nil
	})

	client := telegram.NewClient(appID, appHash, telegram.Options{
		SessionStorage: new(session.StorageMemory),
		UpdateHandler:  disp,
		Middlewares:    []telegram.Middleware{floodwait.NewSimpleWaiter()},
	})

	ready := make(chan error, 1)
	go func() {
		err := client.Run(runCtx, func(ctx context.Context) error {
			status, err := client.Auth().Status(ctx)
			if err != nil {
				ready <- fmt.Errorf("auth status: %w", err)
				return err
			}
			if !status.Authorized {
				if _, err := client.Auth().Bot(ctx, token); err != nil {
					ready <- fmt.Errorf("bot auth: %w", err)
					return err
				}
			}

			self, err := client.Self(ctx)
			if err != nil {
				ready <- fmt.Errorf("get self: %w", err)
				return err
			}
			b.selfID = self.ID
			b.logger.Info("authenticated", "bot", self.Username)

			api := client.API()
			peer, err := resolvePeer(ctx, api, chatID)
			if err != nil {
				ready <- fmt.Errorf("resolve peer: %w", err)
				return err
			}
			b.api = api
			b.peer = peer
			ready <- nil

			<-ctx.Done()
			return nil
		})
		if err != nil && err != context.Canceled {
			b.logger.Error("client run error", "err", err)
			select {
			case ready <- err:
			default:
			}
		}
	}()

	if err := <-ready; err != nil {
		cancel()
		return nil, err
	}

	b.queue = NewSendQueue(b.rawSend)
	return b, nil
}

func (b *Bot) SendAsync(ctx context.Context, p *protocol.Packet) error {
	text, err := protocol.Encode(p)
	if err != nil {
		return fmt.Errorf("encode packet: %w", err)
	}
	return b.queue.EnqueueAsync(ctx, text)
}

func (b *Bot) SendAsyncCallback(ctx context.Context, p *protocol.Packet, onSent func()) error {
	text, err := protocol.Encode(p)
	if err != nil {
		return fmt.Errorf("encode packet: %w", err)
	}
	return b.queue.EnqueueAsyncCallback(ctx, text, onSent)
}

func (b *Bot) Send(p *protocol.Packet) error {
	text, err := protocol.Encode(p)
	if err != nil {
		return fmt.Errorf("encode packet: %w", err)
	}
	return b.queue.Enqueue(text)
}

func (b *Bot) SendWait(ctx context.Context, p *protocol.Packet) error {
	text, err := protocol.Encode(p)
	if err != nil {
		return fmt.Errorf("encode packet: %w", err)
	}
	return b.queue.EnqueueWait(ctx, text)
}

func (b *Bot) StartReceiver(ctx context.Context) <-chan *protocol.Packet {
	out := make(chan *protocol.Packet, 128)
	go func() {
		defer close(out)
		for {
			select {
			case pkt, ok := <-b.updates:
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

func (b *Bot) Stop() {
	b.stopOnce.Do(func() {
		b.queue.Stop()
		b.runCancel()
	})
}

func (b *Bot) rawSend(text string) error {
	ctx, cancel := context.WithTimeout(b.runCtx, 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err := message.NewSender(b.api).To(b.peer).Text(ctx, text)
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		stats.Global.RateLimits.Add(1)
		b.logger.Debug("slow send", "elapsed", elapsed.Round(time.Millisecond))
	}
	if err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	stats.Global.MsgSent.Add(1)
	return nil
}

func resolvePeer(ctx context.Context, api *tg.Client, chatID int64) (*tg.InputPeerChannel, error) {
	channelID := -(chatID + 1000000000000)

	result, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{
		&tg.InputChannel{ChannelID: channelID, AccessHash: 0},
	})
	if err != nil {
		return nil, fmt.Errorf("get channel: %w", err)
	}

	var chats []tg.ChatClass
	switch r := result.(type) {
	case *tg.MessagesChats:
		chats = r.Chats
	case *tg.MessagesChatsSlice:
		chats = r.Chats
	default:
		return nil, fmt.Errorf("unexpected response type %T", result)
	}

	for _, chat := range chats {
		ch, ok := chat.(*tg.Channel)
		if !ok {
			continue
		}
		if ch.ID == channelID {
			return &tg.InputPeerChannel{
				ChannelID:  ch.ID,
				AccessHash: ch.AccessHash,
			}, nil
		}
	}

	return nil, fmt.Errorf("channel %d not found (bot must be an admin)", chatID)
}
