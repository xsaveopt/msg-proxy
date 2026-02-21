package stats

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

type S struct {
	MsgSent     atomic.Int64
	MsgRecv     atomic.Int64
	Retransmits atomic.Int64
	RateLimits  atomic.Int64
	ActiveConns atomic.Int64
	TotalConns  atomic.Int64
}

var Global = &S{}

func (s *S) ConnOpen() {
	s.ActiveConns.Add(1)
	s.TotalConns.Add(1)
}

func (s *S) ConnClose() {
	s.ActiveConns.Add(-1)
}

func (s *S) LogPeriodically(ctx context.Context, logger *slog.Logger, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var lastSent, lastRecv, lastRetx, lastRL int64

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sent := s.MsgSent.Load()
				recv := s.MsgRecv.Load()
				retx := s.Retransmits.Load()
				rl := s.RateLimits.Load()
				active := s.ActiveConns.Load()
				total := s.TotalConns.Load()

				logger.Info("stats",
					"sent", sent-lastSent,
					"recv", recv-lastRecv,
					"retransmits", retx-lastRetx,
					"rate_limits", rl-lastRL,
					"active_conns", active,
					"total_conns", total,
				)

				lastSent, lastRecv, lastRetx, lastRL = sent, recv, retx, rl
			}
		}
	}()
}
