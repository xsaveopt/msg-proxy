package session

import (
	"sync"
	"time"

	"msg-proxy/internal/protocol"
)

type State int

const (
	StateConnecting State = iota
	StateConnected
	StateClosing
	StateClosed
)

type Session struct {
	ID       string
	State    State
	Target   string
	Incoming chan *protocol.Packet
	done     chan struct{}
	once     sync.Once
	LastSeen time.Time
	mu       sync.Mutex
}

func New(id string) *Session {
	return &Session{
		ID:       id,
		State:    StateConnecting,
		Incoming: make(chan *protocol.Packet, 64),
		done:     make(chan struct{}),
		LastSeen: time.Now(),
	}
}

func (s *Session) Done() <-chan struct{} {
	return s.done
}

func (s *Session) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.State = StateClosed
		s.mu.Unlock()
		close(s.done)
		close(s.Incoming)
	})
}

func (s *Session) Touch() {
	s.mu.Lock()
	s.LastSeen = time.Now()
	s.mu.Unlock()
}

func (s *Session) SetState(st State) {
	s.mu.Lock()
	s.State = st
	s.mu.Unlock()
}

func (s *Session) GetState() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.State
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager() *Manager {
	return &Manager{sessions: make(map[string]*Session)}
}

func (m *Manager) Add(s *Session) (existing *Session, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok = m.sessions[s.ID]; ok {
		return existing, false
	}
	m.sessions[s.ID] = s
	return nil, true
}

func (m *Manager) Get(id string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[id]
}

func (m *Manager) Delete(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

func (m *Manager) Reap(idleTimeout time.Duration) {
	now := time.Now()
	m.mu.Lock()
	var stale []*Session
	for _, s := range m.sessions {
		s.mu.Lock()
		idle := now.Sub(s.LastSeen) > idleTimeout
		s.mu.Unlock()
		if idle {
			stale = append(stale, s)
			delete(m.sessions, s.ID)
		}
	}
	m.mu.Unlock()

	for _, s := range stale {
		s.Close()
	}
}

func (m *Manager) StartReaper(interval, idleTimeout time.Duration, stop <-chan struct{}) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				m.Reap(idleTimeout)
			case <-stop:
				return
			}
		}
	}()
}
