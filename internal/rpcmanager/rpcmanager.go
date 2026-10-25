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
	"math/big"
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
			Code    int             `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		} `json:"error"`
	}
	if e = json.NewDecoder(res.Body).Decode(&out); e != nil {
		return e
	}
	if out.Error != nil {
		return fmt.Errorf(
			"RPC error code=%d message=%q data=%s",
			out.Error.Code,
			out.Error.Message,
			string(out.Error.Data),
		)
	}
	if result != nil {
		return json.Unmarshal(out.Result, result)
	}
	return nil
}

type httpError struct{ status int }

func (e *httpError) Error() string   { return fmt.Sprintf("RPC HTTP %d", e.status) }
func (e *httpError) StatusCode() int { return e.status }
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
//
// skipPreflight is a risk control selected by the execution engine.  It must
// never be silently forced on: doing so bypasses Solana's simulation and can
// charge priority fees for transactions that would have failed deterministically
// (for example Pump.fun UnsupportedQuoteMint).  Live trading defaults to
// preflight; an explicit, audited caller may opt out for a narrow latency case.
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
	e = m.call(ctx, u, "sendTransaction", []any{
		base64.StdEncoding.EncodeToString(raw),
		map[string]any{
			"encoding":            "base64",
			"skipPreflight":       skipPreflight,
			"preflightCommitment": "processed",
			"maxRetries":          maxRetries,
		},
	}, &sig)
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
func (m *Manager) TokenBalance(ctx context.Context, owner solana.PublicKey, mint string) (uint64, uint8, error) {
	mintKey, err := solana.PublicKeyFromBase58(mint)
	if err != nil {
		return 0, 0, fmt.Errorf("parse mint public key: %w", err)
	}
	ata, _, err := solana.FindAssociatedTokenAddress(owner, mintKey)
	if err != nil {
		return 0, 0, fmt.Errorf("derive associated token account: %w", err)
	}
	u, err := m.currentURL()
	if err != nil {
		return 0, 0, err
	}
	var r struct {
		Value *struct {
			Amount   string `json:"amount"`
			Decimals uint8  `json:"decimals"`
		} `json:"value"`
	}
	if err = m.call(ctx, u, "getTokenAccountBalance", []any{ata.String(), map[string]any{"commitment": "confirmed"}}, &r); err != nil {
		return 0, 0, fmt.Errorf("read confirmed token balance: %w", err)
	}
	if r.Value == nil || r.Value.Amount == "" {
		return 0, 0, fmt.Errorf("RPC returned no confirmed token balance")
	}
	var amount uint64
	if _, err = fmt.Sscanf(r.Value.Amount, "%d", &amount); err != nil {
		return 0, 0, fmt.Errorf("decode token account balance: %w", err)
	}
	return amount, r.Value.Decimals, nil
}

const pumpFunProgramID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

type BondingCurveState struct {
	VirtualTokenReserves uint64
	VirtualSOLReserves   uint64
	RealTokenReserves    uint64
	RealSOLReserves      uint64
	TokenSupply          uint64
	Complete             bool
	QuoteMint            solana.PublicKey
	MarketCapSOL         float64
	SpotPriceSOL         float64
	ObservedAt           time.Time
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

	// Decode quote_mint tolerantly. Accounts created before the field existed
	// have len(data) <= 83 and are implicitly SOL-quoted (zero public key).
	var quoteMint solana.PublicKey
	switch {
	case len(data) <= 83:
		// Conta antiga: quote_mint ausente significa moeda cotada em SOL.
	case len(data) < 115:
		return BondingCurveState{}, fmt.Errorf(
			"Pump.fun bonding-curve quote_mint field is incomplete: data_len=%d",
			len(data),
		)
	default:
		copy(quoteMint[:], data[83:115])
	}

	state := BondingCurveState{
		VirtualTokenReserves: binary.LittleEndian.Uint64(data[8:16]),
		VirtualSOLReserves:   binary.LittleEndian.Uint64(data[16:24]),
		RealTokenReserves:    binary.LittleEndian.Uint64(data[24:32]),
		RealSOLReserves:      binary.LittleEndian.Uint64(data[32:40]),
		TokenSupply:          binary.LittleEndian.Uint64(data[40:48]),
		Complete:             data[48] != 0,
		QuoteMint:            quoteMint,
	}
	if state.RealSOLReserves == 0 && m.log != nil {
		m.log.Debug("bonding-curve real_sol_reserves decoded as zero",
			zap.String("mint", mint),
			zap.Int("data_len", len(data)),
			zap.String("first_48_bytes_hex", fmt.Sprintf("%x", data[:48])))
	}

	// CORRIGIDO: NÃO REJEITAR quando reserves ou supply são zero.
	// Se VirtualTokenReserves ou VirtualSOLReserves forem zero, retorna MarketCapSOL = 0 e RealSOLReserves = 0 (sem erro).
	// O marketdata.go vai usar o WebSocket como fallback.
	if state.VirtualTokenReserves == 0 || state.VirtualSOLReserves == 0 || state.TokenSupply == 0 {
		return BondingCurveState{MarketCapSOL: 0, RealSOLReserves: 0, QuoteMint: quoteMint}, nil
	}

	// RPC reserves are stored as lamports and 6-decimal token base units.
	// Convert both reserves and supply to human units before calculating SOL market cap.
	virtualSOLReservesSOL := float64(state.VirtualSOLReserves) / 1e9
	virtualTokenReserves := float64(state.VirtualTokenReserves) / 1e6
	tokenSupply := float64(state.TokenSupply) / 1e6
	priceFromReserves := virtualSOLReservesSOL / virtualTokenReserves
	state.SpotPriceSOL = priceFromReserves
	state.MarketCapSOL = priceFromReserves * tokenSupply

	if state.SpotPriceSOL <= 0 || math.IsNaN(state.SpotPriceSOL) || math.IsInf(state.SpotPriceSOL, 0) ||
		state.MarketCapSOL <= 0 || math.IsNaN(state.MarketCapSOL) || math.IsInf(state.MarketCapSOL, 0) {
		return BondingCurveState{MarketCapSOL: 0, RealSOLReserves: 0, QuoteMint: quoteMint}, nil
	}
	state.ObservedAt = time.Now().UTC()
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

// BondingCurvePositionMetrics returns the market-cap risk metric and the
// marginal SOL/token spot price derived from the same on-chain reserve snapshot.
func (m *Manager) BondingCurvePositionMetrics(ctx context.Context, mint string) (float64, float64, error) {
	state, err := m.GetBondingCurveStateWithContext(ctx, mint)
	if err != nil {
		return 0, 0, err
	}
	if state.SpotPriceSOL <= 0 || math.IsNaN(state.SpotPriceSOL) || math.IsInf(state.SpotPriceSOL, 0) {
		return 0, 0, fmt.Errorf("bonding-curve spot price is not positive and finite")
	}
	return state.MarketCapSOL, state.SpotPriceSOL, nil
}

type ExecutionTokenBalance struct {
	Mint   string `json:"mint"`
	Owner  string `json:"owner"`
	Amount struct {
		Amount   string `json:"amount"`
		Decimals uint8  `json:"decimals"`
	} `json:"uiTokenAmount"`
}

// ExecutionReceipt contains wallet SOL movement and the token balance delta
// reported by the confirmed transaction metadata.
type ExecutionReceipt struct {
	SOLDelta             float64
	FeesSOL              float64
	TokenDeltaRaw        string
	TokenBalanceAfterRaw string
	TokenDecimals        uint8
}

// Execution reads confirmed transaction metadata. Callers use the wallet delta
// and fee independently, and persist the token amount actually settled.
func (m *Manager) Execution(ctx context.Context, signature, address, mint string) (ExecutionReceipt, error) {
	u, err := m.currentURL()
	if err != nil {
		return ExecutionReceipt{}, err
	}
	var receipt struct {
		Meta *struct {
			Fee               uint64                  `json:"fee"`
			PreBalances       []uint64                `json:"preBalances"`
			PostBalances      []uint64                `json:"postBalances"`
			PreTokenBalances  []ExecutionTokenBalance `json:"preTokenBalances"`
			PostTokenBalances []ExecutionTokenBalance `json:"postTokenBalances"`
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
		return ExecutionReceipt{}, err
	}
	if receipt.Meta == nil || len(receipt.Meta.PreBalances) != len(receipt.Meta.PostBalances) || len(receipt.Meta.PreBalances) != len(receipt.Transaction.Message.AccountKeys) {
		return ExecutionReceipt{}, fmt.Errorf("incomplete confirmed transaction metadata")
	}
	var solDelta float64
	foundWallet := false
	for i, key := range receipt.Transaction.Message.AccountKeys {
		if key == address {
			delta := int64(receipt.Meta.PostBalances[i]) - int64(receipt.Meta.PreBalances[i])
			solDelta = float64(delta) / 1e9
			foundWallet = true
			break
		}
	}
	if !foundWallet {
		return ExecutionReceipt{}, fmt.Errorf("operational wallet missing from confirmed transaction")
	}
	pre, preDecimals, preFound, err := sumTokenBalances(receipt.Meta.PreTokenBalances, address, mint)
	if err != nil {
		return ExecutionReceipt{}, err
	}
	post, postDecimals, postFound, err := sumTokenBalances(receipt.Meta.PostTokenBalances, address, mint)
	if err != nil {
		return ExecutionReceipt{}, err
	}
	if preFound && postFound && preDecimals != postDecimals {
		return ExecutionReceipt{}, fmt.Errorf("token decimals changed within confirmed transaction")
	}
	decimals := postDecimals
	if !postFound {
		decimals = preDecimals
	}
	if !preFound && !postFound {
		return ExecutionReceipt{}, fmt.Errorf("confirmed transaction metadata contains no token account for the mint")
	}
	delta := new(big.Int).Sub(post, pre)
	return ExecutionReceipt{
		SOLDelta:             solDelta,
		FeesSOL:              float64(receipt.Meta.Fee) / 1e9,
		TokenDeltaRaw:        delta.String(),
		TokenBalanceAfterRaw: post.String(),
		TokenDecimals:        decimals,
	}, nil
}

func sumTokenBalances(balances []ExecutionTokenBalance, owner, mint string) (*big.Int, uint8, bool, error) {
	total := new(big.Int)
	var decimals uint8
	found := false
	for _, balance := range balances {
		if balance.Owner != owner || balance.Mint != mint {
			continue
		}
		amount, ok := new(big.Int).SetString(balance.Amount.Amount, 10)
		if !ok || amount.Sign() < 0 {
			return nil, 0, false, fmt.Errorf("invalid confirmed token amount in transaction metadata")
		}
		if found && decimals != balance.Amount.Decimals {
			return nil, 0, false, fmt.Errorf("inconsistent token decimals in transaction metadata")
		}
		decimals = balance.Amount.Decimals
		found = true
		total.Add(total, amount)
	}
	return total, decimals, found, nil
}

// ResolveCreationTime returns the token's creation time using the currently
// selected healthy RPC endpoint. It is used by the token monitor to backfill
// a creation timestamp when the PumpPortal websocket payload does not carry
// one. It never falls back to time.Now(): if the RPC cannot provide a
// blockTime, it returns an error and the caller must keep the token rejected.
//
// Strategy:
//  1. If a signature is provided, call getTransaction and use blockTime.
//  2. Otherwise, call getSignaturesForAddress for the mint (oldest first),
//     then getTransaction on that signature.
func (m *Manager) ResolveCreationTime(ctx context.Context, mint, signature string) (time.Time, error) {
	if signature != "" {
		if t, err := m.blockTimeOf(ctx, signature); err == nil && !t.IsZero() {
			return t, nil
		}
	}
	if mint == "" {
		return time.Time{}, fmt.Errorf("no signature and no mint to resolve creation time")
	}
	sig, err := m.firstSignatureForMint(ctx, mint)
	if err != nil {
		return time.Time{}, err
	}
	return m.blockTimeOf(ctx, sig)
}

func (m *Manager) firstSignatureForMint(ctx context.Context, mint string) (string, error) {
	u, err := m.currentURL()
	if err != nil {
		return "", err
	}
	var r []struct {
		Signature string `json:"signature"`
	}
	if err := m.call(ctx, u, "getSignaturesForAddress", []any{
		mint,
		map[string]any{"limit": 1, "commitment": "confirmed"},
	}, &r); err != nil {
		return "", fmt.Errorf("getSignaturesForAddress: %w", err)
	}
	if len(r) == 0 || r[0].Signature == "" {
		return "", fmt.Errorf("no signatures for mint %s", mint)
	}
	return r[0].Signature, nil
}

func (m *Manager) blockTimeOf(ctx context.Context, signature string) (time.Time, error) {
	u, err := m.currentURL()
	if err != nil {
		return time.Time{}, err
	}
	var r struct {
		BlockTime *int64 `json:"blockTime"`
	}
	if err := m.call(ctx, u, "getTransaction", []any{
		signature,
		map[string]any{
			"encoding":                       "json",
			"commitment":                     "confirmed",
			"maxSupportedTransactionVersion": 0,
		},
	}, &r); err != nil {
		return time.Time{}, fmt.Errorf("getTransaction %s: %w", signature, err)
	}
	if r.BlockTime == nil || *r.BlockTime <= 0 {
		return time.Time{}, fmt.Errorf("getTransaction %s: missing blockTime", signature)
	}
	return time.Unix(*r.BlockTime, 0).UTC(), nil
}

// AccountInfoResult mirrors the getAccountInfo RPC response. The Data field
// keeps the raw base64 payload so callers can decode it for ALT resolution.
type AccountInfoResult struct {
	Value *struct {
		Data       solana.Data      `json:"data"`
		Executable bool             `json:"executable"`
		Lamports   uint64           `json:"lamports"`
		Owner      solana.PublicKey `json:"owner"`
	} `json:"value"`
}

// GetAccountInfo reads a single account via getAccountInfo. It is used by the
// execution engine to resolve Address Lookup Tables (ALT) before signing a
// PumpPortal prepared transaction.
func (m *Manager) GetAccountInfo(ctx context.Context, account solana.PublicKey) (*AccountInfoResult, error) {
	u, e := m.currentURL()
	if e != nil {
		return nil, e
	}
	var r AccountInfoResult
	if e = m.call(ctx, u, "getAccountInfo", []any{
		account.String(),
		map[string]any{"encoding": "base64", "commitment": "confirmed"},
	}, &r); e != nil {
		return nil, e
	}
	return &r, nil
}