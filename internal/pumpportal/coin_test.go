package pumpportal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoinReadsPublicActivityAndSOLMarketCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/coins/test-mint" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"txns":3,"volume":5.5,"marketCapSol":42}`))
	}))
	defer server.Close()

	snapshot, err := NewCoinDataClientWithBaseURL(server.URL + "/coins").Coin(context.Background(), "test-mint")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.HasTransactions || snapshot.Transactions != 3 {
		t.Fatalf("transaction metric = (%d, %v), want (3, true)", snapshot.Transactions, snapshot.HasTransactions)
	}
	if !snapshot.HasVolume || snapshot.Volume != 5.5 {
		t.Fatalf("volume metric = (%f, %v), want (5.5, true)", snapshot.Volume, snapshot.HasVolume)
	}
	if !snapshot.HasMarketCapSOL || snapshot.MarketCapSOL != 42 {
		t.Fatalf("SOL market cap = (%f, %v), want (42, true)", snapshot.MarketCapSOL, snapshot.HasMarketCapSOL)
	}
}

func TestCoinReadsNestedMetricsAndUSDMarketCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"txns":{"total":4},"volume":{"h24":8.25},"usd_market_cap":1200}}`))
	}))
	defer server.Close()

	snapshot, err := NewCoinDataClientWithBaseURL(server.URL + "/coins").Coin(context.Background(), "test-mint")
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.HasTransactions || snapshot.Transactions != 4 || !snapshot.HasVolume || snapshot.Volume != 8.25 {
		t.Fatalf("unexpected nested activity snapshot: %+v", snapshot)
	}
	if !snapshot.HasMarketCapUSD || snapshot.MarketCapUSD != 1200 {
		t.Fatalf("USD market cap = (%f, %v), want (1200, true)", snapshot.MarketCapUSD, snapshot.HasMarketCapUSD)
	}
}

func TestCoinRejectsNonSuccessHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if _, err := NewCoinDataClientWithBaseURL(server.URL).Coin(context.Background(), "test-mint"); err == nil {
		t.Fatal("non-success API response was accepted")
	}
}

func TestCoinRateLimitBackoffIsExponentialAndDoesNotSleep(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := NewCoinDataClientWithBaseURL(server.URL)
	if _, err := client.Coin(context.Background(), "test-mint"); err == nil {
		t.Fatal("HTTP 429 did not return an error")
	}
	if client.backoff != 40*time.Second {
		t.Fatalf("first rate-limit backoff = %s, want 40s", client.backoff)
	}
	if _, err := client.Coin(context.Background(), "test-mint"); err == nil {
		t.Fatal("request during backoff did not return an error")
	}
	if requests.Load() != 1 {
		t.Fatalf("backoff issued another HTTP request: got %d, want 1", requests.Load())
	}

	client.mu.Lock()
	client.retryUntil = time.Now().Add(-time.Second)
	client.mu.Unlock()
	if _, err := client.Coin(context.Background(), "test-mint"); err == nil {
		t.Fatal("second HTTP 429 did not return an error")
	}
	if client.backoff != 80*time.Second || requests.Load() != 2 {
		t.Fatalf("second backoff = %s after %d requests, want 80s after 2", client.backoff, requests.Load())
	}
}
