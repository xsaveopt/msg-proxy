package transport

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"msg-proxy/internal/protocol"
	"msg-proxy/internal/stats"
)

type Bot struct {
	api    *tgbotapi.BotAPI
	chatID int64
	queue  *SendQueue
	logger *slog.Logger
}

func NewBot(token string, chatID int64, logger *slog.Logger) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("telegram auth: %w", err)
	}
	b := &Bot{
		api:    api,
		chatID: chatID,
		logger: logger.With("bot", api.Self.UserName),
	}
	b.queue = NewSendQueue(b.rawSend)
	return b, nil
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
	go b.receiveLoop(ctx, out)
	return out
}

func (b *Bot) Stop() {
	b.queue.Stop()
}

func (b *Bot) rawSend(text string) error {
	msg := tgbotapi.NewMessage(b.chatID, text)
	_, err := b.api.Send(msg)
	if err != nil {
		var apiErr *tgbotapi.Error
		if errors.As(err, &apiErr) {
			switch apiErr.Code {
			case 429:
				stats.Global.RateLimits.Add(1)
				wait := time.Duration(apiErr.RetryAfter) * time.Second
				if wait == 0 {
					wait = 5 * time.Second
				}
				b.logger.Warn("rate limited, retrying", "wait", wait)
				time.Sleep(wait)
				select {
				case b.queue.retry <- sendJob{text: text}:
				default:
					b.logger.Error("retry queue full, dropping message")
				}
				return nil
			case 401, 403:
				b.logger.Error("fatal Telegram error", "code", apiErr.Code, "err", err)
				return fmt.Errorf("fatal Telegram error %d: %w", apiErr.Code, err)
			}
		}
		return fmt.Errorf("send message: %w", err)
	}
	stats.Global.MsgSent.Add(1)
	return nil
}

func (b *Bot) receiveLoop(ctx context.Context, out chan<- *protocol.Packet) {
	defer close(out)

	cfg := tgbotapi.NewUpdate(0)
	cfg.Timeout = 30
	cfg.AllowedUpdates = []string{"message"}

	updates := b.api.GetUpdatesChan(cfg)

	for {
		select {
		case <-ctx.Done():
			b.api.StopReceivingUpdates()
			return
		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Message == nil || update.Message.Text == "" {
				continue
			}
			if update.Message.Chat.ID != b.chatID {
				continue
			}
			pkt, err := protocol.Decode(update.Message.Text)
			if err != nil {
				b.logger.Debug("ignoring non-packet message", "err", err)
				continue
			}
			stats.Global.MsgRecv.Add(1)
			select {
			case out <- pkt:
			case <-ctx.Done():
				return
			}
		}
	}
}
