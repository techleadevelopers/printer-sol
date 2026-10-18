// Package pricefeed provides the small, fail-closed SOL/USD conversion used
// to size a USD-denominated risk limit before a new entry.
package pricefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"
)

type Provider interface {
	SOLUSD(context.Context) (float64, error)
}

type HTTP struct {
	url    string
	client *http.Client
	mu     sync.Mutex
	lastAt time.Time
	lastV  float64
	ttl    time.Duration
}

func New(url string) *HTTP {
	return &HTTP{url: url, client: &http.Client{Timeout: 5 * time.Second}, ttl: 30 * time.Second}
}

// SOLUSD uses a recent validated quote for at most 30 seconds to avoid
// serializing every entry behind the price HTTP request. Once it expires, a
// failed refresh still blocks entries; expired values are never served stale.
func (h *HTTP) SOLUSD(ctx context.Context) (float64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lastV > 0 && time.Since(h.lastAt) < h.ttl {
		return h.lastV, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.url, nil)
	if err != nil {
		return 0, err
	}
	res, err := h.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("SOL/USD price HTTP %d", res.StatusCode)
	}
	var payload struct {
		Solana struct {
			USD float64 `json:"usd"`
		} `json:"solana"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 8192)).Decode(&payload); err != nil {
		return 0, err
	}
	if payload.Solana.USD <= 0 || math.IsNaN(payload.Solana.USD) || math.IsInf(payload.Solana.USD, 0) {
		return 0, fmt.Errorf("invalid SOL/USD price")
	}
	h.lastV = payload.Solana.USD
	h.lastAt = time.Now()
	return payload.Solana.USD, nil
}
