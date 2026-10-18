package rpcmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"sync"
	"time"
)

type Endpoint struct {
	URL        string `json:"url"`
	Healthy    bool   `json:"healthy"`
	LatencyMS  int64  `json:"latencyMs"`
	LastError  string `json:"lastError,omitempty"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
}
type Manager struct {
	mu        sync.RWMutex
	endpoints []Endpoint
	current   int
	client    *http.Client
	log       *zap.Logger
	db        *database.DB
	bus       *events.Bus
}

func New(urls []string, l *zap.Logger, db *database.DB, b *events.Bus) *Manager {
	e := make([]Endpoint, len(urls))
	for i, u := range urls {
		e[i].URL = u
	}
	return &Manager{endpoints: e, current: -1, client: &http.Client{Timeout: 8 * time.Second}, log: l, db: db, bus: b}
}
func (m *Manager) Run(ctx context.Context, every time.Duration) {
	m.Check(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Check(ctx)
		}
	}
}
func (m *Manager) Check(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	best := -1
	for i := range m.endpoints {
		start := time.Now()
		err := m.call(ctx, m.endpoints[i].URL, "getLatestBlockhash", []any{map[string]any{"commitment": "processed"}}, nil)
		m.endpoints[i].LatencyMS = time.Since(start).Milliseconds()
		m.endpoints[i].Healthy = err == nil
		m.endpoints[i].LastError = ""
		if err != nil {
			m.endpoints[i].LastError = err.Error()
		}
		m.endpoints[i].HTTPStatus = httpStatus(err)
		if m.db != nil {
			_ = m.db.RecordRPC(m.endpoints[i].URL, err == nil, m.endpoints[i].LatencyMS, m.endpoints[i].HTTPStatus, m.endpoints[i].LastError)
		}
		if err == nil && (best < 0 || m.endpoints[i].LatencyMS < m.endpoints[best].LatencyMS) {
			best = i
		}
	}
	if best >= 0 && best != m.current {
		m.log.Info("rpc selected", zap.String("endpoint", m.endpoints[best].URL), zap.Int64("latency_ms", m.endpoints[best].LatencyMS))
		m.current = best
		if m.bus != nil {
			m.bus.Publish("rpc_changed", map[string]any{"endpoint": m.endpoints[best].URL, "latencyMs": m.endpoints[best].LatencyMS})
		}
	} else if best < 0 {
		m.current = -1
	}
}
func (m *Manager) Endpoints() []Endpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Endpoint(nil), m.endpoints...)
}
func (m *Manager) Available() bool { _, e := m.currentURL(); return e == nil }
func (m *Manager) Select(url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.endpoints {
		if m.endpoints[i].URL == url {
			if !m.endpoints[i].Healthy {
				return fmt.Errorf("selected RPC is not healthy")
			}
			m.current = i
			return nil
		}
	}
	return fmt.Errorf("RPC endpoint not found")
}
func (m *Manager) Test(ctx context.Context, url string) Endpoint {
	start := time.Now()
	err := m.call(ctx, url, "getLatestBlockhash", []any{map[string]any{"commitment": "processed"}}, nil)
	e := Endpoint{URL: url, Healthy: err == nil, LatencyMS: time.Since(start).Milliseconds()}
	if err != nil {
		e.LastError = err.Error()
	}
	e.HTTPStatus = httpStatus(err)
	return e
}
func (m *Manager) currentURL() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current < 0 {
		return "", fmt.Errorf("no healthy RPC")
	}
	return m.endpoints[m.current].URL, nil
}
func (m *Manager) call(ctx context.Context, url, method string, params, result any) error {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	res, e := m.client.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusForbidden || res.StatusCode/100 != 2 {
		return &httpError{status: res.StatusCode}
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if e = json.NewDecoder(res.Body).Decode(&out); e != nil {
		return e
	}
	if out.Error != nil {
		return fmt.Errorf("RPC: %s", out.Error.Message)
	}
	if result != nil {
		return json.Unmarshal(out.Result, result)
	}
	return nil
}

type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("RPC HTTP %d", e.status) }
func httpStatus(err error) int {
	var target *httpError
	if errors.As(err, &target) {
		return target.status
	}
	return 0
}
func (m *Manager) Send(ctx context.Context, raw []byte) (string, error) {
	u, e := m.currentURL()
	if e != nil {
		return "", e
	}
	var sig string
	e = m.call(ctx, u, "sendTransaction", []any{base64.StdEncoding.EncodeToString(raw), map[string]any{"encoding": "base64", "skipPreflight": false, "preflightCommitment": "confirmed", "maxRetries": 0}}, &sig)
	return sig, e
}
func (m *Manager) Status(ctx context.Context, sig string) (string, error) {
	u, e := m.currentURL()
	if e != nil {
		return "", e
	}
	var r struct {
		Value []struct {
			ConfirmationStatus string `json:"confirmationStatus"`
			Err                any    `json:"err"`
		} `json:"value"`
	}
	if e = m.call(ctx, u, "getSignatureStatuses", []any{[]string{sig}, map[string]any{"searchTransactionHistory": true}}, &r); e != nil {
		return "", e
	}
	if len(r.Value) == 0 || r.Value[0].ConfirmationStatus == "" {
		return "unknown", nil
	}
	if r.Value[0].Err != nil {
		return "failed", nil
	}
	return r.Value[0].ConfirmationStatus, nil
}
func (m *Manager) Balance(ctx context.Context, address string) (uint64, error) {
	u, e := m.currentURL()
	if e != nil {
		return 0, e
	}
	var r struct {
		Value uint64 `json:"value"`
	}
	if e = m.call(ctx, u, "getBalance", []any{address, map[string]any{"commitment": "confirmed"}}, &r); e != nil {
		return 0, e
	}
	return r.Value, nil
}
