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
	"time"
)

type Provider interface {
	SOLUSD(context.Context) (float64, error)
}

type HTTP struct {
	url    string
	client *http.Client
}

func New(url string) *HTTP {
	return &HTTP{url: url, client: &http.Client{Timeout: 5 * time.Second}}
}

// SOLUSD returns only a response obtained for this request. It deliberately
// does not cache a price: a failed or stale quote must block new entries.
func (h *HTTP) SOLUSD(ctx context.Context) (float64, error) {
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
	return payload.Solana.USD, nil
}
