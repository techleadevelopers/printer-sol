package tokenmonitor

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/url"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/strategy"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Mint     string `json:"mint"`
	Creator  string `json:"creator"`
	User     string `json:"user"`
	Received time.Time
	Raw      string
}
type Monitor struct {
	ws, key   string
	q         chan Event
	db        *database.DB
	strategy  *strategy.Strategy
	bus       *events.Bus
	log       *zap.Logger
	seen      sync.Map
	connected atomic.Bool
}

func New(ws, key string, size int, db *database.DB, s *strategy.Strategy, b *events.Bus, l *zap.Logger) *Monitor {
	return &Monitor{ws: ws, key: key, q: make(chan Event, size), db: db, strategy: s, bus: b, log: l}
}
func (m *Monitor) Run(ctx context.Context) {
	go m.consume(ctx)
	backoff := time.Second
	for ctx.Err() == nil {
		if err := m.connect(ctx); err != nil {
			m.log.Warn("token websocket disconnected", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		} else {
			backoff = time.Second
		}
	}
}
func (m *Monitor) Connected() bool { return m.connected.Load() }
func (m *Monitor) connect(ctx context.Context) error {
	u, _ := url.Parse(m.ws)
	q := u.Query()
	if m.key != "" {
		q.Set("api-key", m.key)
	}
	u.RawQuery = q.Encode()
	c, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return err
	}
	defer c.Close()
	if err = c.WriteJSON(map[string]string{"method": "subscribeNewToken"}); err != nil {
		return err
	}
	m.connected.Store(true)
	defer m.connected.Store(false)
	for {
		_, b, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var d struct{ Mint, Creator, User string }
		if err = json.Unmarshal(b, &d); err != nil || d.Mint == "" {
			m.log.Warn("invalid token event", zap.Error(err))
			continue
		}
		creator := d.Creator
		if creator == "" {
			creator = d.User
		}
		e := Event{Mint: d.Mint, Creator: creator, Received: time.Now(), Raw: string(b)}
		if _, loaded := m.seen.LoadOrStore(e.Mint, true); loaded {
			continue
		}
		m.bus.Publish("token_detected", map[string]any{"mint": e.Mint, "creator": e.Creator, "receivedAt": e.Received.UTC()})
		select {
		case m.q <- e:
		default:
			m.log.Warn("token queue full", zap.String("mint", e.Mint))
			m.bus.Publish("operational_error", map[string]string{"component": "token_monitor", "message": "token queue full"})
			m.seen.Delete(e.Mint)
		}
	}
}
func (m *Monitor) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-m.q:
			latency := time.Since(e.Received).Milliseconds()
			if err := m.db.RecordEvent(e.Mint, e.Creator, e.Raw, e.Received, latency); err != nil {
				m.log.Warn("record event", zap.Error(err))
				m.bus.Publish("operational_error", map[string]string{"component": "database", "message": "unable to record token"})
			}
			ok, reason := m.strategy.Evaluate(ctx, e.Mint)
			kind := "token_rejected"
			if ok {
				kind = "token_approved"
			}
			m.bus.Publish(kind, map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
			m.log.Info("token evaluated", zap.String("mint", e.Mint), zap.Bool("eligible", ok), zap.String("reason", reason), zap.Int64("latency_ms", latency))
		}
	}
}
