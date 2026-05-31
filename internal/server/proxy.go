package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"msg-proxy/internal/protocol"
	"msg-proxy/internal/reliable"
	"msg-proxy/internal/session"
	"msg-proxy/internal/stats"
	"msg-proxy/internal/transport"
)

type connState struct {
	stream *reliable.Stream
	cancel context.CancelFunc
}

type Proxy struct {
	bot     *transport.Bot
	manager *session.Manager
	logger  *slog.Logger

	mu     sync.RWMutex
	states map[string]*connState
}

func New(bot *transport.Bot, logger *slog.Logger) *Proxy {
	return &Proxy{
		bot:     bot,
		manager: session.NewManager(),
		logger:  logger,
		states:  make(map[string]*connState),
	}
}

func (p *Proxy) Run(ctx context.Context, idleTimeout time.Duration) {
	stop := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stop)
	}()
	p.manager.StartReaper(10*time.Second, idleTimeout, stop)

	packets := p.bot.StartReceiver(ctx)
	for {
		select {
		case pkt, ok := <-packets:
			if !ok {
				return
			}
			p.dispatch(ctx, pkt)
		case <-ctx.Done():
			return
		}
	}
}

func (p *Proxy) dispatch(ctx context.Context, pkt *protocol.Packet) {
	switch pkt.Type {
	case protocol.TypeConnect:
		p.handleConnect(ctx, pkt)
	case protocol.TypeData, protocol.TypeDataAck:
		p.handleDataPacket(pkt)
	case protocol.TypeClose:
		p.handleClose(pkt)
	default:
		p.logger.Warn("unknown packet type", "type", pkt.Type, "session", pkt.SessionID)
	}
}

func (p *Proxy) handleConnect(ctx context.Context, pkt *protocol.Packet) {
	logger := p.logger.With("session", pkt.SessionID, "target", pkt.Target)
	logger.Info("new CONNECT request")

	sess := session.New(pkt.SessionID)
	if _, ok := p.manager.Add(sess); !ok {
		logger.Warn("duplicate session ID, ignoring")
		return
	}

	go func() {
		connCtx, connCancel := context.WithCancel(ctx)

		stats.Global.ConnOpen()
		defer func() {
			stats.Global.ConnClose()
			connCancel()
			p.mu.Lock()
			delete(p.states, pkt.SessionID)
			p.mu.Unlock()
			p.manager.Delete(pkt.SessionID)
			sess.Close()
		}()

		conn, err := net.DialTimeout("tcp", pkt.Target, 10*time.Second)
		if err != nil {
			logger.Warn("TCP dial failed", "err", err)
			_ = p.bot.SendWait(ctx, &protocol.Packet{
				SessionID: pkt.SessionID,
				Type:      protocol.TypeError,
				Payload:   protocol.EncodePayload([]byte(err.Error())),
			})
			return
		}
		defer func() { _ = conn.Close() }()

		stream := reliable.New(pkt.SessionID, p.bot, reliable.RetransmitTimeout)
		defer stream.Stop()

		p.mu.Lock()
		p.states[pkt.SessionID] = &connState{stream: stream, cancel: connCancel}
		p.mu.Unlock()

		sess.SetState(session.StateConnected)
		sess.Touch()

		if err := p.bot.SendWait(connCtx, &protocol.Packet{
			SessionID: pkt.SessionID,
			Type:      protocol.TypeAck,
		}); err != nil {
			logger.Error("send ACK failed", "err", err)
			return
		}
		logger.Info("connected, ACK sent")

		done := make(chan struct{}, 1)
		signal := func() {
			select {
			case done <- struct{}{}:
			default:
			}
		}

		go func() {
			defer signal()
			p.readFromTCP(connCtx, sess, conn, stream, logger)
		}()

		go func() {
			defer signal()
			p.writeToTCP(connCtx, conn, stream, logger)
		}()

		<-done
	}()
}

func (p *Proxy) handleDataPacket(pkt *protocol.Packet) {
	p.mu.RLock()
	state := p.states[pkt.SessionID]
	p.mu.RUnlock()

	if state == nil {
		p.logger.Warn("unknown session", "session", pkt.SessionID, "type", pkt.Type)
		return
	}
	if sess := p.manager.Get(pkt.SessionID); sess != nil {
		sess.Touch()
	}
	state.stream.Deliver(context.Background(), pkt)
}

func (p *Proxy) handleClose(pkt *protocol.Packet) {
	p.mu.RLock()
	state := p.states[pkt.SessionID]
	p.mu.RUnlock()

	if state == nil {
		p.logger.Debug("unknown session for CLOSE (already cleaned up)", "session", pkt.SessionID)
		return
	}
	p.logger.Info("client closed session", "session", pkt.SessionID)
	state.cancel()
}

func (p *Proxy) readFromTCP(ctx context.Context, sess *session.Session, conn net.Conn, stream *reliable.Stream, logger *slog.Logger) {
	buf := make([]byte, protocol.MaxPayloadBytes)
	for {
		n, err := conn.Read(buf)
		if n > 0 {
			sess.Touch()
			if sendErr := stream.Send(ctx, buf[:n]); sendErr != nil {
				logger.Debug("stream send failed", "err", sendErr)
				_ = p.bot.SendWait(ctx, &protocol.Packet{SessionID: sess.ID, Type: protocol.TypeClose})
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				logger.Debug("TCP read error", "err", err)
			}
			_ = p.bot.SendWait(ctx, &protocol.Packet{
				SessionID: sess.ID,
				Type:      protocol.TypeClose,
			})
			return
		}
	}
}

func (p *Proxy) writeToTCP(ctx context.Context, conn net.Conn, stream *reliable.Stream, logger *slog.Logger) {
	for {
		data, err := stream.Read(ctx)
		if err != nil {
			logger.Debug("stream read stopped", "err", err)
			return
		}
		if _, err := conn.Write(data); err != nil {
			logger.Debug("TCP write error", "err", err)
			return
		}
	}
}
