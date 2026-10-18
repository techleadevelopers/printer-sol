package pricefeed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSOLUSDUsesOnlyUnexpiredValidatedCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"solana":{"usd":150}}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	provider := New(server.URL)
	price, err := provider.SOLUSD(context.Background())
	if err != nil || price != 150 {
		t.Fatalf("first quote = %v, err=%v", price, err)
	}
	price, err = provider.SOLUSD(context.Background())
	if err != nil || price != 150 || requests.Load() != 1 {
		t.Fatalf("cached quote = %v, err=%v, requests=%d", price, err, requests.Load())
	}

	provider.mu.Lock()
	provider.lastAt = time.Now().Add(-2 * provider.ttl)
	provider.mu.Unlock()
	if _, err = provider.SOLUSD(context.Background()); err == nil {
		t.Fatal("expired quote was served after refresh failed")
	}
	if requests.Load() != 2 {
		t.Fatalf("expired quote refresh requests = %d, want 2", requests.Load())
	}
}
