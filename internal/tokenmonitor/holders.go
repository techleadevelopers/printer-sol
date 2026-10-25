package tokenmonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// HolderSource devolve a contagem de holders de um mint.
type HolderSource interface {
	HolderCount(ctx context.Context, mint string) (int, error)
}

// NoopHolderSource devolve 0 sem erro; usar enquanto não existir fonte real.
type NoopHolderSource struct{}

func (NoopHolderSource) HolderCount(ctx context.Context, mint string) (int, error) {
	return 0, nil
}

type holderCacheEntry struct {
	count     int
	fetchedAt time.Time
}

// CachedHolderSource envolve uma HolderSource com cache TTL por mint.
type CachedHolderSource struct {
	source HolderSource
	ttl    time.Duration
	mu     sync.Mutex
	cache  map[string]holderCacheEntry
}

func NewCachedHolderSource(source HolderSource, ttl time.Duration) *CachedHolderSource {
	if source == nil {
		source = NoopHolderSource{}
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &CachedHolderSource{source: source, ttl: ttl, cache: make(map[string]holderCacheEntry)}
}

func (c *CachedHolderSource) HolderCount(ctx context.Context, mint string) (int, error) {
	c.mu.Lock()
	if entry, ok := c.cache[mint]; ok && time.Since(entry.fetchedAt) < c.ttl {
		c.mu.Unlock()
		return entry.count, nil
	}
	c.mu.Unlock()

	count, err := c.source.HolderCount(ctx, mint)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.cache[mint] = holderCacheEntry{count: count, fetchedAt: time.Now().UTC()}
	c.mu.Unlock()
	return count, nil
}

func (c *CachedHolderSource) Forget(mint string) {
	c.mu.Lock()
	delete(c.cache, mint)
	c.mu.Unlock()
}

// SolscanHolderSource consulta o endpoint /v2.0/token/holders da Solscan Pro API.
// A resposta inclui o campo `total`, que é o número de holders do mint [citation:1].
type SolscanHolderSource struct {
	apiKey string
	client *http.Client
}

func NewSolscanHolderSource(apiKey string) *SolscanHolderSource {
	return &SolscanHolderSource{
		apiKey: apiKey,
		client: &http.Client{Timeout: 8 * time.Second},
	}
}

func (s *SolscanHolderSource) HolderCount(ctx context.Context, mint string) (int, error) {
	if s.apiKey == "" {
		return 0, fmt.Errorf("solscan API key is empty")
	}
	endpoint := fmt.Sprintf(
		"https://pro-api.solscan.io/v2.0/token/holders?address=%s&page=1&page_size=1",
		url.QueryEscape(mint),
	)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("token", s.apiKey)

	res, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusTooManyRequests {
		return 0, fmt.Errorf("solscan rate limited")
	}
	if res.StatusCode/100 != 2 {
		return 0, fmt.Errorf("solscan HTTP %d", res.StatusCode)
	}

	var out struct {
		Success bool `json:"success"`
		Total   int  `json:"total"`
	}
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		return 0, fmt.Errorf("decode solscan response: %w", err)
	}
	if !out.Success {
		return 0, fmt.Errorf("solscan reported failure for mint %s", mint)
	}
	return out.Total, nil
}