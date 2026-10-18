package rpcmanager

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
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

func TestSendWithMaxRetriesCanSkipPreflightAndCapsRetries(t *testing.T) {
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
	signature, err := manager.SendWithMaxRetries(context.Background(), []byte{1, 2, 3}, 99, true)
	if err != nil || signature != "signature" {
		t.Fatalf("send returned signature=%q err=%v", signature, err)
	}
	if !skipPreflight {
		t.Fatal("skipPreflight option was not forwarded")
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

func TestGetBondingCurveStateReadsAndDecodesRPCAccount(t *testing.T) {
	discriminator := sha256.Sum256([]byte("account:BondingCurve"))
	data := make([]byte, 49)
	copy(data[:8], discriminator[:8])
	binary.LittleEndian.PutUint64(data[8:16], 2_000_000)
	binary.LittleEndian.PutUint64(data[16:24], 1_000_000_000)
	binary.LittleEndian.PutUint64(data[24:32], 1_500_000)
	binary.LittleEndian.PutUint64(data[32:40], 750_000_000)
	binary.LittleEndian.PutUint64(data[40:48], 2_000_000)
	data[48] = 1

	var requestedMethod, requestedCurve string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode RPC request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		requestedMethod = request.Method
		if len(request.Params) != 2 {
			t.Errorf("getAccountInfo params = %d; want 2", len(request.Params))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(request.Params[0], &requestedCurve); err != nil {
			t.Errorf("decode bonding-curve address: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"value":{"owner":"` + pumpFunProgramID + `","data":["` + base64.StdEncoding.EncodeToString(data) + `","base64"]}}}`))
	}))
	defer server.Close()

	manager := &Manager{
		endpoints: []Endpoint{{URL: server.URL}},
		current:   0,
		client:    server.Client(),
	}
	mint := "So11111111111111111111111111111111111111112"
	state, err := manager.GetBondingCurveState(mint)
	if err != nil {
		t.Fatal(err)
	}
	if requestedMethod != "getAccountInfo" {
		t.Fatalf("RPC method = %q, want getAccountInfo", requestedMethod)
	}
	if requestedCurve == "" {
		t.Fatal("RPC did not receive the derived bonding-curve PDA")
	}
	if state.VirtualTokenReserves != 2_000_000 || state.VirtualSOLReserves != 1_000_000_000 ||
		state.RealTokenReserves != 1_500_000 || state.RealSOLReserves != 750_000_000 ||
		state.TokenSupply != 2_000_000 || !state.Complete || state.MarketCapSOL != 1 {
		t.Fatalf("decoded bonding curve state = %+v", state)
	}
}
