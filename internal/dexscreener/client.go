package dexscreener

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultTokenAPIURL = "https://api.dexscreener.com/latest/dex/tokens"

// CoinSnapshot contains activity and market-cap data for one Solana token.
// DexScreener reports volume and market cap in USD; the monitor converts them
// with its current SOL/USD quote before applying the entry gate or storing marks.
type CoinSnapshot struct {
	Transactions    int
	VolumeUSD       float64
	MarketCapUSD    float64
	HasTransactions bool
	HasVolumeUSD    bool
	HasMarketCapUSD bool
}

type Client struct {
	baseURL     string
	http        *http.Client
	mu          sync.Mutex
	retryUntil  time.Time
	baseBackoff time.Duration
	backoff     time.Duration
}

func NewClient() *Client {
	return NewClientWithBaseURL(defaultTokenAPIURL)
}

func NewClientWithBaseURL(baseURL string) *Client {
	return &Client{
		baseURL:     strings.TrimRight(baseURL, "/"),
		http:        &http.Client{Timeout: 5 * time.Second},
		baseBackoff: 20 * time.Second,
	}
}

func (c *Client) SetRateLimitBase(interval time.Duration) {
	if interval <= 0 {
		return
	}
	c.mu.Lock()
	c.baseBackoff = interval
	c.mu.Unlock()
}

func (c *Client) RateLimitDelay() time.Duration {
	return c.rateLimitRemaining(time.Now())
}

func (c *Client) Coin(ctx context.Context, mint string) (CoinSnapshot, error) {
	if strings.TrimSpace(mint) == "" {
		return CoinSnapshot{}, fmt.Errorf("mint is required")
	}
	if delay := c.rateLimitRemaining(time.Now()); delay > 0 {
		return CoinSnapshot{}, fmt.Errorf("DexScreener API rate-limit backoff active for %s", delay.Round(time.Second))
	}
	endpoint := c.baseURL + "/" + url.PathEscape(mint)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return CoinSnapshot{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "solana-sniper-backend/1.0")
	res, err := c.http.Do(req)
	if err != nil {
		return CoinSnapshot{}, err
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests {
		delay := c.beginRateLimitBackoff(time.Now())
		return CoinSnapshot{}, fmt.Errorf("DexScreener API HTTP 429; backing off for %s", delay)
	}
	if res.StatusCode != http.StatusOK {
		return CoinSnapshot{}, fmt.Errorf("DexScreener API HTTP %d", res.StatusCode)
	}
	c.clearRateLimitBackoff()

	var response struct {
		Pairs []struct {
			ChainID     string `json:"chainId"`
			BaseToken   struct {
				Address string `json:"address"`
			} `json:"baseToken"`
			Liquidity struct {
				USD json.RawMessage `json:"usd"`
			} `json:"liquidity"`
			Txns      map[string]json.RawMessage `json:"txns"`
			Volume    map[string]json.RawMessage `json:"volume"`
			MarketCap json.RawMessage            `json:"marketCap"`
		} `json:"pairs"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 2<<20)).Decode(&response); err != nil {
		return CoinSnapshot{}, fmt.Errorf("decode DexScreener response: %w", err)
	}

	selected := -1
	selectedLiquidity := -1.0
	for i := range response.Pairs {
		pair := &response.Pairs[i]
		if !strings.EqualFold(pair.ChainID, "solana") || !strings.EqualFold(pair.BaseToken.Address, mint) {
			continue
		}
		liquidity, _ := jsonNumber(pair.Liquidity.USD)
		if selected < 0 || liquidity > selectedLiquidity {
			selected = i
			selectedLiquidity = liquidity
		}
	}
	if selected < 0 {
		return CoinSnapshot{}, nil
	}

	pair := response.Pairs[selected]
	var snapshot CoinSnapshot
	if raw, ok := firstPeriod(pair.Txns); ok {
		if value, valid := metricValue(raw); valid && value >= 0 && finite(value) {
			snapshot.Transactions = int(value)
			snapshot.HasTransactions = true
		}
	}
	if raw, ok := firstPeriod(pair.Volume); ok {
		if value, valid := jsonNumber(raw); valid && value >= 0 {
			snapshot.VolumeUSD = value
			snapshot.HasVolumeUSD = true
		}
	}
	if value, valid := jsonNumber(pair.MarketCap); valid && value >= 0 {
		snapshot.MarketCapUSD = value
		snapshot.HasMarketCapUSD = true
	}
	return snapshot, nil
}

func firstPeriod(metrics map[string]json.RawMessage) (json.RawMessage, bool) {
	for _, period := range []string{"m5", "h1", "h6", "h24"} {
		if value, ok := metrics[period]; ok && len(value) > 0 && string(value) != "null" {
			return value, true
		}
	}
	return nil, false
}

func (c *Client) rateLimitRemaining(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.retryUntil.After(now) {
		return 0
	}
	return c.retryUntil.Sub(now)
}

func (c *Client) beginRateLimitBackoff(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.retryUntil.After(now) {
		return c.retryUntil.Sub(now)
	}
	if c.backoff == 0 {
		c.backoff = c.baseBackoff * 2
		if c.backoff <= 0 {
			c.backoff = 40 * time.Second
		}
	} else {
		c.backoff *= 2
	}
	if c.backoff > 5*time.Minute {
		c.backoff = 5 * time.Minute
	}
	c.retryUntil = now.Add(c.backoff)
	return c.backoff
}

func (c *Client) clearRateLimitBackoff() {
	c.mu.Lock()
	c.retryUntil = time.Time{}
	c.backoff = 0
	c.mu.Unlock()
}

func metricValue(raw json.RawMessage, preferred ...string) (float64, bool) {
	if value, ok := jsonNumber(raw); ok {
		return value, true
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return 0, false
	}
	for _, key := range preferred {
		if value, ok := object[key]; ok && len(value) > 0 && string(value) != "null" {
			return metricValue(value, preferred...)
		}
	}
	var total float64
	found := false
	for _, key := range []string{"buy", "buys", "sell", "sells"} {
		if value, ok := object[key]; ok {
			if parsed, valid := metricValue(value, preferred...); valid {
				total += parsed
				found = true
			}
		}
	}
	return total, found
}

func jsonNumber(raw json.RawMessage) (float64, bool) {
	var value float64
	if json.Unmarshal(raw, &value) == nil && finite(value) {
		return value, true
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		parsed, err := strconv.ParseFloat(strings.TrimSpace(text), 64)
		return parsed, err == nil && finite(parsed)
	}
	return 0, false
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

