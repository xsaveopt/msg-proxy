package stats

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type record struct {
	msg   string
	attrs map[string]int64
}

type captureHandler struct {
	mu      sync.Mutex
	records []record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := record{msg: r.Message, attrs: make(map[string]int64)}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.Int64()
		return true
	})
	h.mu.Lock()
	h.records = append(h.records, rec)
	h.mu.Unlock()
	return nil
}

func (h *captureHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(_ string) slog.Handler { return h }

func (h *captureHandler) len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.records)
}

func (h *captureHandler) at(i int) record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.records[i]
}

func (h *captureHandler) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.len() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records, got %d", n, h.len())
}

func TestConnCounters(t *testing.T) {
	s := &S{}

	s.ConnOpen()
	s.ConnOpen()
	if got := s.ActiveConns.Load(); got != 2 {
		t.Errorf("ActiveConns: got %d, want 2", got)
	}
	if got := s.TotalConns.Load(); got != 2 {
		t.Errorf("TotalConns: got %d, want 2", got)
	}

	s.ConnClose()
	if got := s.ActiveConns.Load(); got != 1 {
		t.Errorf("ActiveConns after close: got %d, want 1", got)
	}
	if got := s.TotalConns.Load(); got != 2 {
		t.Errorf("TotalConns should not drop on close: got %d, want 2", got)
	}
}

func TestConnCountersConcurrent(t *testing.T) {
	s := &S{}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.ConnOpen()
			s.ConnClose()
		}()
	}
	wg.Wait()

	if got := s.ActiveConns.Load(); got != 0 {
		t.Errorf("ActiveConns: got %d, want 0", got)
	}
	if got := s.TotalConns.Load(); got != 50 {
		t.Errorf("TotalConns: got %d, want 50", got)
	}
}

func TestGlobalIsUsable(t *testing.T) {
	if Global == nil {
		t.Fatal("Global must not be nil")
	}
	before := Global.TotalConns.Load()
	Global.ConnOpen()
	Global.ConnClose()
	if got := Global.TotalConns.Load(); got != before+1 {
		t.Errorf("Global.TotalConns: got %d, want %d", got, before+1)
	}
}

func TestLogPeriodicallyReportsDeltas(t *testing.T) {
	s := &S{}
	s.MsgSent.Add(5)
	s.MsgRecv.Add(3)
	s.Retransmits.Add(2)
	s.RateLimits.Add(1)
	s.ConnOpen()

	h := &captureHandler{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.LogPeriodically(ctx, slog.New(h), 5*time.Millisecond)
	h.waitFor(t, 1)

	first := h.at(0)
	if first.msg != "stats" {
		t.Errorf("message: got %q, want %q", first.msg, "stats")
	}
	want := map[string]int64{
		"sent":         5,
		"recv":         3,
		"retransmits":  2,
		"rate_limits":  1,
		"active_conns": 1,
		"total_conns":  1,
	}
	for k, v := range want {
		if first.attrs[k] != v {
			t.Errorf("first record %s: got %d, want %d", k, first.attrs[k], v)
		}
	}

	h.waitFor(t, 2)
	second := h.at(1)
	for _, k := range []string{"sent", "recv", "retransmits", "rate_limits"} {
		if second.attrs[k] != 0 {
			t.Errorf("second record %s: got %d, want 0 (deltas must reset)", k, second.attrs[k])
		}
	}
	if second.attrs["active_conns"] != 1 {
		t.Errorf("second record active_conns: got %d, want 1 (gauge, not delta)", second.attrs["active_conns"])
	}
}

func TestLogPeriodicallyStopsOnContextCancel(t *testing.T) {
	s := &S{}
	h := &captureHandler{}
	ctx, cancel := context.WithCancel(context.Background())

	s.LogPeriodically(ctx, slog.New(h), 5*time.Millisecond)
	h.waitFor(t, 1)

	cancel()
	time.Sleep(30 * time.Millisecond)
	settled := h.len()
	time.Sleep(60 * time.Millisecond)

	if got := h.len(); got != settled {
		t.Errorf("logging continued after cancel: %d records, then %d", settled, got)
	}
}
