package rpcmanager

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
)

type Endpoint struct {
	URL        string `json:"url"`
	Healthy    bool   `json:"healthy"`
	LatencyMS  int64  `json:"latencyMs"`
	LastError  string `json:"lastError,omitempty"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
}
type Manager struct {
	mu        sync.RWMutex
	endpoints []Endpoint
	current   int
	client    *http.Client
	log       *zap.Logger
	db        *database.DB
	bus       *events.Bus
}

func New(urls []string, l *zap.Logger, db *database.DB, b *events.Bus) *Manager {
	e := make([]Endpoint, len(urls))
	for i, u := range urls {
		e[i].URL = u
	}
	return &Manager{endpoints: e, current: -1, client: &http.Client{Timeout: 8 * time.Second}, log: l, db: db, bus: b}
}
func (m *Manager) Run(ctx context.Context, every time.Duration) {
	m.Check(ctx)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Check(ctx)
		}
	}
}
func (m *Manager) Check(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	best := -1
	for i := range m.endpoints {
		start := time.Now()
		err := m.call(ctx, m.endpoints[i].URL, "getLatestBlockhash", []any{map[string]any{"commitment": "processed"}}, nil)
		m.endpoints[i].LatencyMS = time.Since(start).Milliseconds()
		m.endpoints[i].Healthy = err == nil
		m.endpoints[i].LastError = ""
		if err != nil {
			m.endpoints[i].LastError = err.Error()
		}
		m.endpoints[i].HTTPStatus = httpStatus(err)
		if m.db != nil {
			_ = m.db.RecordRPC(m.endpoints[i].URL, err == nil, m.endpoints[i].LatencyMS, m.endpoints[i].HTTPStatus, m.endpoints[i].LastError)
		}
		if err == nil && (best < 0 || m.endpoints[i].LatencyMS < m.endpoints[best].LatencyMS) {
			best = i
		}
	}
	if best >= 0 && best != m.current {
		m.log.Info("rpc selected", zap.String("endpoint", m.endpoints[best].URL), zap.Int64("latency_ms", m.endpoints[best].LatencyMS))
		m.current = best
		if m.bus != nil {
			m.bus.Publish("rpc_changed", map[string]any{"endpoint": m.endpoints[best].URL, "latencyMs": m.endpoints[best].LatencyMS})
		}
	} else if best < 0 {
		m.current = -1
	}
}
func (m *Manager) Endpoints() []Endpoint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Endpoint(nil), m.endpoints...)
}
func (m *Manager) Available() bool { _, e := m.currentURL(); return e == nil }
func (m *Manager) Select(url string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.endpoints {
		if m.endpoints[i].URL == url {
			if !m.endpoints[i].Healthy {
				return fmt.Errorf("selected RPC is not healthy")
			}
			m.current = i
			return nil
		}
	}
	return fmt.Errorf("RPC endpoint not found")
}
func (m *Manager) Test(ctx context.Context, url string) Endpoint {
	start := time.Now()
	err := m.call(ctx, url, "getLatestBlockhash", []any{map[string]any{"commitment": "processed"}}, nil)
	e := Endpoint{URL: url, Healthy: err == nil, LatencyMS: time.Since(start).Milliseconds()}
	if err != nil {
		e.LastError = err.Error()
	}
	e.HTTPStatus = httpStatus(err)
	return e
}
func (m *Manager) currentURL() (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.current < 0 {
		return "", fmt.Errorf("no healthy RPC")
	}
	return m.endpoints[m.current].URL, nil
}
func (m *Manager) call(ctx context.Context, url, method string, params, result any) error {
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	res, e := m.client.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusForbidden || res.StatusCode/100 != 2 {
		return &httpError{status: res.StatusCode}
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if e = json.NewDecoder(res.Body).Decode(&out); e != nil {
		return e
	}
	if out.Error != nil {
		return fmt.Errorf("RPC: %s", out.Error.Message)
	}
	if result != nil {
		return json.Unmarshal(out.Result, result)
	}
	return nil
}

type httpError struct{ status int }

func (e *httpError) Error() string { return fmt.Sprintf("RPC HTTP %d", e.status) }
func httpStatus(err error) int {
	var target *httpError
	if errors.As(err, &target) {
		return target.status
	}
	return 0
}
func (m *Manager) Send(ctx context.Context, raw []byte) (string, error) {
	return m.SendWithMaxRetries(ctx, raw, 0, false)
}

// SendWithMaxRetries asks the RPC node to rebroadcast the same signed payload.
// It never constructs or signs a replacement transaction, so retries retain
// the original signature and are idempotent with respect to duplicate buys.
func (m *Manager) SendWithMaxRetries(ctx context.Context, raw []byte, maxRetries int, skipPreflight bool) (string, error) {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if maxRetries > 5 {
		maxRetries = 5
	}
	u, e := m.currentURL()
	if e != nil {
		return "", e
	}
	var sig string
	e = m.call(ctx, u, "sendTransaction", []any{base64.StdEncoding.EncodeToString(raw), map[string]any{"encoding": "base64", "skipPreflight": skipPreflight, "preflightCommitment": "confirmed", "maxRetries": maxRetries}}, &sig)
	return sig, e
}
func (m *Manager) Status(ctx context.Context, sig string) (string, error) {
	u, e := m.currentURL()
	if e != nil {
		return "", e
	}
	var r struct {
		Value []struct {
			ConfirmationStatus string `json:"confirmationStatus"`
			Err                any    `json:"err"`
		} `json:"value"`
	}
	if e = m.call(ctx, u, "getSignatureStatuses", []any{[]string{sig}, map[string]any{"searchTransactionHistory": true}}, &r); e != nil {
		return "", e
	}
	if len(r.Value) == 0 || r.Value[0].ConfirmationStatus == "" {
		return "unknown", nil
	}
	if r.Value[0].Err != nil {
		return "failed", nil
	}
	return r.Value[0].ConfirmationStatus, nil
}
func (m *Manager) Balance(ctx context.Context, address string) (uint64, error) {
	u, e := m.currentURL()
	if e != nil {
		return 0, e
	}
	var r struct {
		Value uint64 `json:"value"`
	}
	if e = m.call(ctx, u, "getBalance", []any{address, map[string]any{"commitment": "confirmed"}}, &r); e != nil {
		return 0, e
	}
	return r.Value, nil
}

// TokenBalance returns the raw (integer) SPL token amount held by owner for
// the given mint, by reading the associated token account. It is strictly
// read-only and returns 0 when the ATA does not exist (mint never bought, or
// already fully sold). RPC/transport errors are returned so callers can decide
// whether to proceed; a confirmed zero is what triggers orphan-position
// quarantine in execution.CloseLivePosition.
func (m *Manager) TokenBalance(ctx context.Context, owner solana.PublicKey, mint string) (uint64, error) {
	mintKey, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return 0, fmt.Errorf("parse mint public key: %w", err)
	}
	ata, _, err := solana.FindAssociatedTokenAddress(owner, mintKey)
	if err != nil {
		return 0, fmt.Errorf("derive associated token account: %w", err)
	}
	u, err := m.currentURL()
	if err != nil {
		return 0, err
	}
	var r struct {
		Value *struct {
			Amount string `json:"amount"`
		} `json:"value"`
	}
	if err = m.call(ctx, u, "getTokenAccountBalance", []any{ata.String(), map[string]any{"commitment": "confirmed"}}, &r); err != nil {
		// A missing ATA surfaces as an RPC error ("could not find account").
		// Treat it as a confirmed zero balance: the wallet holds no tokens.
		return 0, nil
	}
	if r.Value == nil || r.Value.Amount == "" {
		return 0, nil
	}
	var amount uint64
	if _, err = fmt.Sscanf(r.Value.Amount, "%d", &amount); err != nil {
		return 0, fmt.Errorf("decode token account balance: %w", err)
	}
	return amount, nil
}

const pumpFunProgramID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

type BondingCurveState struct {
	VirtualTokenReserves uint64
	VirtualSOLReserves   uint64
	RealTokenReserves    uint64
	RealSOLReserves      uint64
	TokenSupply          uint64
	Complete             bool
	MarketCapSOL         float64
}

// GetBondingCurveState reads Pump.fun's bonding-curve PDA via getAccountInfo
// and decodes its Anchor account data.
func (m *Manager) GetBondingCurveState(mint string) (BondingCurveState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return m.GetBondingCurveStateWithContext(ctx, mint)
}

func (m *Manager) GetBondingCurveStateWithContext(ctx context.Context, mint string) (BondingCurveState, error) {
	mintKey, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return BondingCurveState{}, fmt.Errorf("parse mint public key: %w", err)
	}
	programID, err := solana.PublicKeyFromBase58(pumpFunProgramID)
	if err != nil {
		return BondingCurveState{}, fmt.Errorf("parse Pump.fun program ID: %w", err)
	}
	curveAddress, _, err := solana.FindProgramAddress(
		[][]byte{[]byte("bonding-curve"), mintKey.Bytes()},
		programID,
	)
	if err != nil {
		return BondingCurveState{}, fmt.Errorf("derive Pump.fun bonding-curve address: %w", err)
	}
	endpoint, err := m.currentURL()
	if err != nil {
		return BondingCurveState{}, err
	}
	var account struct {
		Value *struct {
			Owner string   `json:"owner"`
			Data  []string `json:"data"`
		} `json:"value"`
	}
	if err := m.call(ctx, endpoint, "getAccountInfo", []any{
		curveAddress.String(),
		map[string]any{"encoding": "base64", "commitment": "processed"},
	}, &account); err != nil {
		return BondingCurveState{}, fmt.Errorf("read Pump.fun bonding-curve account: %w", err)
	}
	if account.Value == nil || len(account.Value.Data) == 0 {
		return BondingCurveState{}, fmt.Errorf("Pump.fun bonding-curve account is unavailable")
	}
	if account.Value.Owner != programID.String() {
		return BondingCurveState{}, fmt.Errorf("bonding-curve account owner does not match Pump.fun program")
	}
	data, err := base64.StdEncoding.DecodeString(account.Value.Data[0])
	if err != nil {
		return BondingCurveState{}, fmt.Errorf("decode Pump.fun bonding-curve account: %w", err)
	}
	if len(data) < 49 {
		return BondingCurveState{}, fmt.Errorf("Pump.fun bonding-curve account data is incomplete")
	}
	discriminator := sha256.Sum256([]byte("account:BondingCurve"))
	if !bytes.Equal(data[:8], discriminator[:8]) {
		return BondingCurveState{}, fmt.Errorf("account data is not a Pump.fun bonding curve")
	}
	state := BondingCurveState{
		VirtualTokenReserves: binary.LittleEndian.Uint64(data[8:16]),
		VirtualSOLReserves:   binary.LittleEndian.Uint64(data[16:24]),
		RealTokenReserves:    binary.LittleEndian.Uint64(data[24:32]),
		RealSOLReserves:      binary.LittleEndian.Uint64(data[32:40]),
		TokenSupply:          binary.LittleEndian.Uint64(data[40:48]),
		Complete:             data[48] != 0,
	}
	if state.RealSOLReserves == 0 && m.log != nil {
		m.log.Debug("bonding-curve real_sol_reserves decoded as zero",
			zap.String("mint", mint),
			zap.Int("data_len", len(data)),
			zap.String("first_48_bytes_hex", fmt.Sprintf("%x", data[:48])))
	}
	if state.VirtualTokenReserves == 0 || state.VirtualSOLReserves == 0 || state.TokenSupply == 0 {
		return BondingCurveState{}, fmt.Errorf("Pump.fun bonding-curve reserves or supply are zero")
	}
	priceFromReserves := float64(state.VirtualSOLReserves) / float64(state.VirtualTokenReserves)
	state.MarketCapSOL = priceFromReserves * float64(state.TokenSupply) / 1e9
	if state.MarketCapSOL <= 0 || math.IsNaN(state.MarketCapSOL) || math.IsInf(state.MarketCapSOL, 0) {
		return BondingCurveState{}, fmt.Errorf("derived Pump.fun market cap is not positive and finite")
	}
	return state, nil
}

// BondingCurveMarketCapSOL preserves the market-cap interface used by entry
// validation and position mark refreshes.
func (m *Manager) BondingCurveMarketCapSOL(ctx context.Context, mint string) (float64, error) {
	state, err := m.GetBondingCurveStateWithContext(ctx, mint)
	if err != nil {
		return 0, err
	}
	return state.MarketCapSOL, nil
}

// BondingCurveEntryMetrics returns the RPC-derived market cap and current
// real SOL reserves. The reserves are a liquidity proxy, not traded volume.
func (m *Manager) BondingCurveEntryMetrics(ctx context.Context, mint string) (float64, float64, error) {
	state, err := m.GetBondingCurveStateWithContext(ctx, mint)
	if err != nil {
		return 0, 0, err
	}
	return state.MarketCapSOL, float64(state.RealSOLReserves) / 1e9, nil
}

// Execution reads the confirmed transaction receipt and returns the SOL delta
// for the operational wallet plus the network fee. Live P&L must use this
// ledger data rather than a market-cap estimate.
func (m *Manager) Execution(ctx context.Context, signature, address string) (float64, float64, error) {
	u, err := m.currentURL()
	if err != nil {
		return 0, 0, err
	}
	var receipt struct {
		Meta *struct {
			Fee          uint64   `json:"fee"`
			PreBalances  []uint64 `json:"preBalances"`
			PostBalances []uint64 `json:"postBalances"`
		} `json:"meta"`
		Transaction struct {
			Message struct {
				AccountKeys []string `json:"accountKeys"`
			} `json:"message"`
		} `json:"transaction"`
	}
	err = m.call(ctx, u, "getTransaction", []any{signature, map[string]any{
		"encoding": "json", "commitment": "confirmed", "maxSupportedTransactionVersion": 0,
	}}, &receipt)
	if err != nil {
		return 0, 0, err
	}
	if receipt.Meta == nil || len(receipt.Meta.PreBalances) != len(receipt.Meta.PostBalances) || len(receipt.Meta.PreBalances) != len(receipt.Transaction.Message.AccountKeys) {
		return 0, 0, fmt.Errorf("incomplete confirmed transaction metadata")
	}
	for i, key := range receipt.Transaction.Message.AccountKeys {
		if key == address {
			delta := int64(receipt.Meta.PostBalances[i]) - int64(receipt.Meta.PreBalances[i])
			return float64(delta) / 1e9, float64(receipt.Meta.Fee) / 1e9, nil
		}
	}
	return 0, 0, fmt.Errorf("operational wallet missing from confirmed transaction")
}