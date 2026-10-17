package rpcmanager

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRPCRejectsRateLimitAndForbidden(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusForbidden} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
		err := (&Manager{client: s.Client()}).call(context.Background(), s.URL, "getLatestBlockhash", nil, nil)
		s.Close()
		if err == nil || !strings.Contains(err.Error(), "RPC HTTP") {
			t.Fatalf("status %d: %v", status, err)
		}
		if got := httpStatus(err); got != status {
			t.Fatalf("status was not preserved: got %d, want %d", got, status)
		}
	}
}

func TestRPCConnectionFailure(t *testing.T) {
	err := (&Manager{client: &http.Client{}}).call(context.Background(), "http://127.0.0.1:1", "getLatestBlockhash", nil, nil)
	if err == nil {
		t.Fatal("expected connection failure")
	}
}

func TestCheckMarksRPCUnavailableWhenEveryEndpointFails(t *testing.T) {
	m := &Manager{
		endpoints: []Endpoint{{URL: "http://127.0.0.1:1", Healthy: true}},
		current:   0,
		client:    &http.Client{Timeout: 50 * time.Millisecond},
	}
	m.Check(context.Background())
	if m.Available() {
		t.Fatal("an endpoint that failed its latest health check must not remain available")
	}
}
