package pumpportal

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

const defaultCoinAPIBaseURL = "https://frontend-api.pump.fun/coins"

// CoinSnapshot contains the public activity and market-cap data used to gate
// entries and to refresh open-position marks. MarketCapUSD must be converted
// with a current SOL/USD quote before it is stored in the position ledger.
type CoinSnapshot struct {
	Transactions       int
	Volume             float64
	MarketCapSOL       float64
	MarketCapUSD       float64
	HasTransactions    bool
	HasVolume          bool
	HasMarketCapSOL    bool
	HasMarketCapUSD    bool
}

type CoinDataClient struct {
	baseURL string
	http    *http.Client
	mu      sync.Mutex
	retryUntil time.Time
	baseBackoff time.Duration
	backoff time.Duration
}

func NewCoinDataClient() *CoinDataClient {
	return NewCoinDataClientWithBaseURL(defaultCoinAPIBaseURL)
}

func NewCoinDataClientWithBaseURL(baseURL string) *CoinDataClient {
	return &CoinDataClient{
		baseURL:     strings.TrimRight(baseURL, "/"),
		http:        &http.Client{Timeout: 5 * time.Second},
		baseBackoff: 20 * time.Second,
	}
}

func (c *CoinDataClient) SetRateLimitBase(interval time.Duration) {
	if interval <= 0 {
		return
	}
	c.mu.Lock()
	c.baseBackoff = interval
	c.mu.Unlock()
}

func (c *CoinDataClient) RateLimitDelay() time.Duration {
	return c.rateLimitRemaining(time.Now())
}

func (c *CoinDataClient) Coin(ctx context.Context, mint string) (CoinSnapshot, error) {
	if strings.TrimSpace(mint) == "" {
		return CoinSnapshot{}, fmt.Errorf("mint is required")
	}
	if delay := c.rateLimitRemaining(time.Now()); delay > 0 {
		return CoinSnapshot{}, fmt.Errorf("Pump.fun coin API rate-limit backoff active for %s", delay.Round(time.Second))
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
		return CoinSnapshot{}, fmt.Errorf("Pump.fun coin API HTTP 429; backing off for %s", delay)
	}
	if res.StatusCode != http.StatusOK {
		return CoinSnapshot{}, fmt.Errorf("Pump.fun coin API HTTP %d", res.StatusCode)
	}
	c.clearRateLimitBackoff()

	var payload map[string]json.RawMessage
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&payload); err != nil {
		return CoinSnapshot{}, err
	}
	if nested, ok := firstRaw(payload, "data", "coin"); ok {
		var coin map[string]json.RawMessage
		if json.Unmarshal(nested, &coin) == nil && len(coin) > 0 {
			payload = coin
		}
	}

	var snapshot CoinSnapshot
	if raw, ok := firstRaw(payload, "txns", "transactions", "transactionCount", "transaction_count", "totalTransactions", "total_transactions"); ok {
		if value, valid := metricValue(raw, "total", "count", "txns", "transactions", "h24", "24h", "24hr"); valid && value >= 0 {
			snapshot.Transactions = int(value)
			snapshot.HasTransactions = true
		}
	}
	if raw, ok := firstRaw(payload, "volume", "volume24h", "volume_24h", "volumeUsd", "volume_usd", "volume_usd_24h"); ok {
		if value, valid := metricValue(raw, "h24", "24h", "24hr", "total", "volume", "usd"); valid && value >= 0 && finite(value) {
			snapshot.Volume = value
			snapshot.HasVolume = true
		}
	}
	if raw, ok := firstRaw(payload, "marketCapSol", "market_cap_sol", "market_cap", "marketCap"); ok {
		if value, valid := jsonNumber(raw); valid && value >= 0 && finite(value) {
			snapshot.MarketCapSOL = value
			snapshot.HasMarketCapSOL = true
		}
	}
	if raw, ok := firstRaw(payload, "usd_market_cap", "usdMarketCap", "marketCapUsd", "market_cap_usd"); ok {
		if value, valid := jsonNumber(raw); valid && value >= 0 && finite(value) {
			snapshot.MarketCapUSD = value
			snapshot.HasMarketCapUSD = true
		}
	}
	return snapshot, nil
}

func (c *CoinDataClient) rateLimitRemaining(now time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.retryUntil.After(now) {
		return 0
	}
	return c.retryUntil.Sub(now)
}

func (c *CoinDataClient) beginRateLimitBackoff(now time.Time) time.Duration {
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

func (c *CoinDataClient) clearRateLimitBackoff() {
	c.mu.Lock()
	c.retryUntil = time.Time{}
	c.backoff = 0
	c.mu.Unlock()
}

func firstRaw(payload map[string]json.RawMessage, keys ...string) (json.RawMessage, bool) {
	for _, key := range keys {
		if raw, ok := payload[key]; ok && len(raw) > 0 && string(raw) != "null" {
			return raw, true
		}
	}
	return nil, false
}

func metricValue(raw json.RawMessage, preferred ...string) (float64, bool) {
	if value, ok := jsonNumber(raw); ok {
		return value, true
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil {
		return 0, false
	}
	if value, ok := firstRaw(object, preferred...); ok {
		return metricValue(value, preferred...)
	}
	var buySell float64
	foundBuySell := false
	for _, key := range []string{"buy", "buys", "sell", "sells"} {
		if value, ok := object[key]; ok {
			if parsed, valid := metricValue(value, preferred...); valid {
				buySell += parsed
				foundBuySell = true
			}
		}
	}
	if foundBuySell {
		return buySell, true
	}
	return 0, false
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
