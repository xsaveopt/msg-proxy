package client

import (
	"context"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"
	"msg-proxy/internal/protocol"
	"msg-proxy/internal/reliable"
	"msg-proxy/internal/session"
	"msg-proxy/internal/socks5"
	"msg-proxy/internal/stats"
	"msg-proxy/internal/transport"
)

type Proxy struct {
	bot     *transport.Bot
	manager *session.Manager
	logger  *slog.Logger
}

func New(bot *transport.Bot, logger *slog.Logger) *Proxy {
	return &Proxy{
		bot:     bot,
		manager: session.NewManager(),
		logger:  logger,
	}
}

func (p *Proxy) Run(ctx context.Context, socks5Addr string, idleTimeout time.Duration) error {
	stop := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stop)
	}()
	p.manager.StartReaper(10*time.Second, idleTimeout, stop)

	packets := p.bot.StartReceiver(ctx)
	go p.receiveLoop(ctx, packets)

	srv := socks5.New(socks5Addr, p.logger, p.handleConnect)
	return srv.ListenAndServe(ctx)
}

func (p *Proxy) receiveLoop(ctx context.Context, packets <-chan *protocol.Packet) {
	for {
		select {
		case pkt, ok := <-packets:
			if !ok {
				return
			}
			sess := p.manager.Get(pkt.SessionID)
			if sess == nil {
				p.logger.Warn("unknown session", "session", pkt.SessionID, "type", pkt.Type)
				continue
			}
			sess.Touch()
			select {
			case sess.Incoming <- pkt:
			case <-sess.Done():
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (p *Proxy) handleConnect(conn net.Conn, req socks5.ConnectRequest) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Error("panic in CONNECT handler", "err", r)
			conn.Close()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessionID := uuid.New().String()
	logger := p.logger.With("session", sessionID, "target", req.Target)
	logger.Info("new SOCKS5 CONNECT")
	stats.Global.ConnOpen()
	defer stats.Global.ConnClose()

	sess := session.New(sessionID)
	sess.Target = req.Target
	if _, ok := p.manager.Add(sess); !ok {
		logger.Error("session ID collision (should never happen)")
		socks5.SendFailure(conn)
		return
	}
	defer p.manager.Delete(sessionID)
	defer sess.Close()

	if err := p.bot.SendWait(ctx, &protocol.Packet{
		SessionID: sessionID,
		Type:      protocol.TypeConnect,
		Target:    req.Target,
	}); err != nil {
		logger.Error("send CONNECT failed", "err", err)
		socks5.SendFailure(conn)
		return
	}

	ackCtx, ackCancel := context.WithTimeout(ctx, 15*time.Second)
	defer ackCancel()

	var ackPkt *protocol.Packet
	select {
	case pkt, ok := <-sess.Incoming:
		if !ok {
			logger.Warn("session closed while waiting for ACK")
			socks5.SendFailure(conn)
			return
		}
		ackPkt = pkt
	case <-ackCtx.Done():
		logger.Warn("timeout waiting for ACK")
		socks5.SendFailure(conn)
		return
	}

	switch ackPkt.Type {
	case protocol.TypeAck:
	case protocol.TypeError:
		msg, _ := protocol.DecodePayload(ackPkt.Payload)
		logger.Warn("remote returned error", "msg", string(msg))
		socks5.SendFailure(conn)
		return
	default:
		logger.Warn("unexpected packet type while waiting for ACK", "type", ackPkt.Type)
		socks5.SendFailure(conn)
		return
	}

	if err := socks5.SendSuccess(conn); err != nil {
		logger.Error("send SOCKS5 success failed", "err", err)
		return
	}
	sess.SetState(session.StateConnected)
	logger.Info("tunnel established")

	stream := reliable.New(sessionID, p.bot, reliable.RetransmitTimeout)
	defer stream.Stop()

	done := make(chan struct{}, 1)
	signal := func() { select { case done <- struct{}{}: default: } }

	go func() {
		defer signal()
		for {
			select {
			case pkt, ok := <-sess.Incoming:
				if !ok {
					return
				}
				switch pkt.Type {
				case protocol.TypeData, protocol.TypeDataAck:
					stream.Deliver(ctx, pkt)
				case protocol.TypeClose, protocol.TypeError:
					logger.Info("server closed connection", "type", pkt.Type)
					return
				}
			case <-ctx.Done():
				return
			case <-sess.Done():
				return
			}
		}
	}()

	go func() {
		defer signal()
		p.readFromBrowser(ctx, sess, conn, stream, logger)
	}()

	go func() {
		defer signal()
		p.writeFromTelegram(ctx, conn, stream, logger)
	}()

	<-done
	conn.Close()
}

func (p *Proxy) readFromBrowser(ctx context.Context, sess *session.Session, conn net.Conn, stream *reliable.Stream, logger *slog.Logger) {
	buf := make([]byte, protocol.MaxPayloadBytes)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			sess.Touch()
			if sendErr := stream.Send(ctx, buf[:n]); sendErr != nil {
				logger.Debug("stream send failed", "err", sendErr)
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				logger.Debug("browser read error", "err", err)
			}
			p.bot.SendWait(ctx, &protocol.Packet{
				SessionID: sess.ID,
				Type:      protocol.TypeClose,
			})
			return
		}
	}
}

func (p *Proxy) writeFromTelegram(ctx context.Context, conn net.Conn, stream *reliable.Stream, logger *slog.Logger) {
	for {
		data, err := stream.Read(ctx)
		if err != nil {
			logger.Debug("stream read stopped", "err", err)
			return
		}
		if _, err := conn.Write(data); err != nil {
			logger.Debug("browser write error", "err", err)
			return
		}
	}
}
