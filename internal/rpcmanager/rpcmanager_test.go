package rpcmanager

import (
	"context"
	"encoding/json"
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

func TestSendWithMaxRetriesRetainsPreflightAndCapsRetries(t *testing.T) {
	var skipPreflight bool
	var retries int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode send request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(request.Params) != 2 {
			t.Errorf("send params = %d; want 2", len(request.Params))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var options struct {
			SkipPreflight bool `json:"skipPreflight"`
			MaxRetries    int  `json:"maxRetries"`
		}
		if err := json.Unmarshal(request.Params[1], &options); err != nil {
			t.Errorf("decode send options: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		skipPreflight, retries = options.SkipPreflight, options.MaxRetries
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"signature"}`))
	}))
	defer server.Close()

	manager := &Manager{
		endpoints: []Endpoint{{URL: server.URL}},
		current:   0,
		client:    server.Client(),
	}
	signature, err := manager.SendWithMaxRetries(context.Background(), []byte{1, 2, 3}, 99)
	if err != nil || signature != "signature" {
		t.Fatalf("send returned signature=%q err=%v", signature, err)
	}
	if skipPreflight {
		t.Fatal("preflight must remain enabled")
	}
	if retries != 5 {
		t.Fatalf("maxRetries = %d; want capped value 5", retries)
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
