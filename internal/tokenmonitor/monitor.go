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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Mint     string `json:"mint"`
	Creator  string `json:"creator"`
	User     string `json:"user"`
	Received time.Time
	Created  time.Time
	Raw      string
}
// Entrier is implemented by the execution engine. Keeping the dependency as
// an interface lets discovery remain independent from transaction signing.
type Entrier interface {
	BuyNewTokenUSD(context.Context, string, float64, float64, float64, string, string) (string, error)
}
type Monitor struct {
	ws, key   string
	q         chan Event
	db        *database.DB
	strategy  *strategy.Strategy
	bus       *events.Bus
	log       *zap.Logger
	entry     Entrier
	tradeUSD, slippage, priorityFee float64
	seen      sync.Map
	connected atomic.Bool
}

func New(ws, key string, size int, db *database.DB, s *strategy.Strategy, entry Entrier, tradeUSD, slippage, priorityFee float64, b *events.Bus, l *zap.Logger) *Monitor {
	return &Monitor{ws: ws, key: key, q: make(chan Event, size), db: db, strategy: s, entry: entry, tradeUSD: tradeUSD, slippage: slippage, priorityFee: priorityFee, bus: b, log: l}
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
		var d struct {
			Mint, Creator, User string
			CreatedTimestamp, Timestamp, BlockTime json.RawMessage
		}
		if err = json.Unmarshal(b, &d); err != nil || d.Mint == "" {
			m.log.Warn("invalid token event", zap.Error(err))
			continue
		}
		creator := d.Creator
		if creator == "" {
			creator = d.User
		}
		received := time.Now().UTC()
		created := timestampFromEvent(d.CreatedTimestamp, d.Timestamp, d.BlockTime)
		if created.IsZero() { created = received }
		e := Event{Mint: d.Mint, Creator: creator, Received: received, Created: created, Raw: string(b)}
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
			ok, reason := m.strategy.Evaluate(ctx, e.Mint, e.Created)
			m.log.Info("token evaluated", zap.String("mint", e.Mint), zap.Bool("eligible", ok), zap.String("reason", reason), zap.Int64("latency_ms", latency))
			if !ok || m.entry == nil {
				_ = m.db.RecordAssessment(e.Mint, false, reason)
				m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
				continue
			}
			id, entryErr := m.entry.BuyNewTokenUSD(ctx, e.Mint, m.tradeUSD, m.slippage, m.priorityFee, "pump", reason)
			if entryErr != nil {
				_ = m.db.RecordAssessment(e.Mint, false, entryErr.Error())
				m.bus.Publish("entry_rejected", map[string]any{"mint": e.Mint, "reason": entryErr.Error()})
				m.log.Info("approved token not entered", zap.String("mint", e.Mint), zap.Error(entryErr))
				continue
			}
			if assessmentErr := m.db.RecordAssessment(e.Mint, true, reason); assessmentErr != nil {
				m.log.Warn("record entered-token assessment", zap.Error(assessmentErr))
			}
			m.bus.Publish("token_approved", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
			m.bus.Publish("entry_submitted", map[string]any{"mint": e.Mint, "tradeId": id, "simulation": true})
		}
	}
}

// timestampFromEvent accepts the timestamp variants emitted by PumpPortal. A
// missing timestamp is safe here because subscribeNewToken is a live-only
// stream; its receipt time then becomes the creation-time upper bound.
func timestampFromEvent(values ...json.RawMessage) time.Time {
	for _, raw := range values {
		if len(raw) == 0 || string(raw) == "null" { continue }
		var numeric float64
		if json.Unmarshal(raw, &numeric) == nil && numeric > 0 {
			if numeric > 1e12 { numeric /= 1000 }
			return time.Unix(int64(numeric), 0).UTC()
		}
		var text string
		if json.Unmarshal(raw, &text) != nil { continue }
		if numeric, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err == nil && numeric > 0 {
			if numeric > 1e12 { numeric /= 1000 }
			return time.Unix(int64(numeric), 0).UTC()
		}
		if parsed, err := time.Parse(time.RFC3339, text); err == nil { return parsed.UTC() }
	}
	return time.Time{}
}
