package execution

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"go.uber.org/zap"
	"solana-sniper-backend/internal/bot"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/pricefeed"
	"solana-sniper-backend/internal/pumpportal"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/wallet"
)

type Request struct {
	Mint, Side            string
	Amount                float64
	DenominatedInSOL      bool
	Slippage, PriorityFee float64
	Pool, Reason          string
	SellAll               bool
	RequestedValueSOL     float64
}
type Limits struct {
	MaxTradeSOL, MaxExposureSOL, MaxDailyLossSOL, MaxSlippagePct, MaxPriorityFeeSOL, FeeReserveSOL float64
	MaxLossPerTradePct                                                                            float64
	MaxSlippageBPS, MaxQuoteAgeMS, MaxCurveImpactBPS                                               int
	SimulationBaseFeeSOL                                                                         float64
	MaxOpenPositions, MaxWaitSeconds, MaxAttempts                                                  int
	SkipPreflight                                                                                 bool
}
type Engine struct {
	db                *database.DB
	wallet            *wallet.Wallet
	portal            *pumpportal.Client
	rpcs              *rpcmanager.Manager
	prices            pricefeed.Provider
	log               *zap.Logger
	simulation        bool
	simulationBalance uint64
	controller        *bot.Controller
	bus               *events.Bus
	rootCtx           context.Context
	executionGate     sync.RWMutex
	mu                sync.RWMutex
	limits            Limits
	reserved          map[string]uint64
}

type livePreparation struct {
	amount              any
	denominatedInSOL    bool
	slippage            float64
	quotedAt            time.Time
	quoteAgeLimit       int
	curveImpactBPS      float64
	minimumOutputSOL    float64
	requestedValueSOL   float64
}

func New(db *database.DB, w *wallet.Wallet, p *pumpportal.Client, r *rpcmanager.Manager, prices pricefeed.Provider, l *zap.Logger, sim bool, simulationBalanceSOL float64, c *bot.Controller, b *events.Bus, limits Limits) *Engine {
	balance, _ := wallet.SOLToLamports(simulationBalanceSOL)
	return &Engine{db: db, wallet: w, portal: p, rpcs: r, prices: prices, log: l, simulation: sim, simulationBalance: balance, controller: c, bus: b, rootCtx: context.Background(), limits: limits, reserved: make(map[string]uint64)}
}
func (e *Engine) Limits() Limits     { e.mu.RLock(); defer e.mu.RUnlock(); return e.limits }
func (e *Engine) SetLimits(l Limits) { e.mu.Lock(); e.limits = l; e.mu.Unlock() }
func (e *Engine) SetRootContext(ctx context.Context) {
	if ctx != nil {
		e.mu.Lock()
		e.rootCtx = ctx
		e.mu.Unlock()
	}
}
func (e *Engine) SimulationBalanceSOL() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return float64(e.simulationBalance) / 1e9
}

// WithSimulationReset serializes a paper reset against execution. The callback
// is expected to perform the database transaction; in-memory state is cleared
// only after it commits successfully.
func (e *Engine) WithSimulationReset(balanceSOL float64, reset func() error) error {
	if !e.simulation {
		return fmt.Errorf("simulation reset is unavailable in live mode")
	}
	balance, err := wallet.SOLToLamports(balanceSOL)
	if err != nil {
		return err
	}
	e.executionGate.Lock()
	defer e.executionGate.Unlock()
	if reset != nil {
		if err = reset(); err != nil {
			return err
		}
	}
	e.ResetInMemoryState()
	e.mu.Lock()
	e.simulationBalance = balance
	e.mu.Unlock()
	return nil
}

func (e *Engine) ResetInMemoryState() {
	e.mu.Lock()
	for mint := range e.reserved {
		delete(e.reserved, mint)
	}
	e.mu.Unlock()
}
func (e *Engine) emit(kind string, data any) {
	if e.bus != nil {
		e.bus.Publish(kind, data)
	}
}

// BuyNewTokenUSD sizes one one-shot entry from a fresh SOL/USD quote. The
// reservation prevents concurrent new-token events from allocating the same
// wallet balance before a submitted transaction becomes visible on-chain.
func (e *Engine) BuyNewTokenUSD(ctx context.Context, mint string, usd, slippage, priorityFee float64, pool, reason string) (string, error) {
	if usd <= 0 || e.prices == nil {
		return "", fmt.Errorf("USD sizing is unavailable")
	}
	price, err := e.prices.SOLUSD(ctx)
	if err != nil {
		return "", fmt.Errorf("fresh SOL/USD price required before entry: %w", err)
	}
	amount := usd / price
	spend, err := wallet.SOLToLamports(amount)
	if err != nil || spend == 0 {
		return "", fmt.Errorf("invalid USD-sized entry")
	}
	fee, err := wallet.SOLToLamports(priorityFee)
	if err != nil {
		return "", err
	}
	if e.simulation {
		baseFee, feeErr := wallet.SOLToLamports(e.Limits().SimulationBaseFeeSOL)
		if feeErr != nil {
			return "", feeErr
		}
		fee += baseFee
	}
	if bought, err := e.db.HasPurchaseForMint(mint); err != nil {
		return "", err
	} else if bought {
		return "", fmt.Errorf("mint is permanently blocked after its first buy attempt")
	}
	if err := e.reserve(ctx, mint, spend+fee); err != nil {
		return "", err
	}
	id, err := e.Execute(ctx, Request{Mint: mint, Side: "buy", Amount: amount, DenominatedInSOL: true, Slippage: slippage, PriorityFee: priorityFee, Pool: pool, Reason: reason})
	if err != nil {
		// A persisted submitted/unknown trade blocks all new buys until
		// reconciliation; retaining this in-memory reservation would only leak.
		e.release(mint)
		return id, err
	}
	if e.simulation {
		e.release(mint)
	}
	return id, nil
}

// ClosePaperPosition records a virtual sale at the live market-marked value.
// It is intentionally unavailable outside simulation mode.
func (e *Engine) ClosePaperPosition(ctx context.Context, mint string, returnPct float64, reason string) (string, error) {
	if !e.simulation {
		return "", fmt.Errorf("paper exits are only available in simulation")
	}
	cost, err := e.db.PositionCost(mint)
	if err != nil {
		return "", err
	}
	value := cost * (1 + returnPct/100)
	if value <= 0 {
		return "", fmt.Errorf("invalid paper exit value")
	}
	return e.Execute(ctx, Request{Mint: mint, Side: "sell", Amount: value, DenominatedInSOL: true, PriorityFee: e.Limits().MaxPriorityFeeSOL, Reason: reason})
}

// CloseLivePosition sells the full balance of a confirmed live position.
func (e *Engine) CloseLivePosition(ctx context.Context, mint string, slippage, priorityFee float64, reason string) (string, error) {
	if e.simulation {
		return "", fmt.Errorf("live exits are unavailable in simulation")
	}
	if _, err := e.db.PositionCost(mint); err != nil {
		return "", fmt.Errorf("live position is not confirmed: %w", err)
	}
	return e.Execute(ctx, Request{Mint: mint, Side: "sell", Amount: 100, DenominatedInSOL: false, Slippage: slippage, PriorityFee: priorityFee, Pool: "auto", Reason: reason, SellAll: true})
}

func (e *Engine) reserve(ctx context.Context, mint string, needed uint64) error {
	var balance uint64
	if e.simulation {
		available, err := e.db.SimulationAvailableBalanceSOL(e.SimulationBalanceSOL())
		if err != nil {
			return err
		}
		if available > 0 {
			balance, err = wallet.SOLToLamports(available)
			if err != nil {
				return err
			}
		}
	} else {
		var err error
		balance, err = e.rpcs.Balance(ctx, e.wallet.Address())
		if err != nil {
			return fmt.Errorf("wallet balance unavailable: %w", err)
		}
	}
	reserve, err := wallet.SOLToLamports(e.Limits().FeeReserveSOL)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.reserved[mint]; exists {
		return fmt.Errorf("mint already has a reserved entry")
	}
	var reserved uint64
	for _, v := range e.reserved {
		reserved += v
	}
	required := reserve + reserved + needed
	if !e.simulation {
		exposure, err := e.db.OpenExposureSOL()
		if err != nil {
			return err
		}
		openCost, err := wallet.SOLToLamports(exposure)
		if err != nil {
			return err
		}
		required += openCost
	}
	if balance < required {
		return fmt.Errorf("insufficient free wallet balance after fee reserve and pending entries")
	}
	e.reserved[mint] = needed
	return nil
}

func (e *Engine) release(mint string) {
	e.mu.Lock()
	delete(e.reserved, mint)
	e.mu.Unlock()
}

// Reconcile recovers persisted in-flight submissions after a restart. It never submits a new transaction.
func (e *Engine) Reconcile(ctx context.Context) error {
	rows, err := e.db.UnreconciledTrades()
	if err != nil {
		return err
	}
	return e.reconcileRows(ctx, rows)
}

// ReconcileUnknown revisits outcomes that were uncertain while the service
// stayed online. It only reads persisted signatures and never submits trades.
func (e *Engine) ReconcileUnknown(ctx context.Context) error {
	if e.simulation {
		return nil
	}
	rows, err := e.db.UnknownTrades()
	if err != nil {
		return err
	}
	return e.reconcileRows(ctx, rows)
}

func (e *Engine) reconcileRows(ctx context.Context, rows *sql.Rows) error {
	type pendingTrade struct {
		id, signature, mint, side string
	}
	pending := make([]pendingTrade, 0)
	for rows.Next() {
		var trade pendingTrade
		if scanErr := rows.Scan(&trade.id, &trade.signature, &trade.mint, &trade.side); scanErr != nil {
			_ = rows.Close()
			return scanErr
		}
		pending = append(pending, trade)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		_ = rows.Close()
		return rowsErr
	}
	if closeErr := rows.Close(); closeErr != nil {
		return closeErr
	}
	for _, trade := range pending {
		if trade.signature == "" {
			_ = e.db.UpdateTradeState(trade.id, "unknown", "", false)
			e.emit("risk_alert", map[string]any{"tradeId": trade.id, "reason": "persisted operation has no signature; manual reconciliation required"})
			continue
		}
		if e.simulation {
			_ = e.db.UpdateTradeState(trade.id, "unknown", trade.signature, false)
			e.emit("risk_alert", map[string]any{"tradeId": trade.id, "reason": "live signature found in simulation ledger"})
			continue
		}
		status, statusErr := e.rpcs.Status(ctx, trade.signature)
		if statusErr != nil {
			e.emit("operational_error", map[string]any{"component": "reconciliation", "tradeId": trade.id, "message": "RPC status unavailable"})
			continue
		}
		switch status {
		case "confirmed", "finalized":
			receipt, executionErr := e.rpcs.Execution(ctx, trade.signature, e.wallet.Address(), trade.mint)
			if executionErr != nil {
				_ = e.db.UpdateTradeState(trade.id, "unknown", trade.signature, false)
				e.emit("operational_error", map[string]any{"component": "reconciliation", "tradeId": trade.id, "message": "confirmed transaction execution unavailable"})
				continue
			}
			var finalizeErr error
			switch trade.side {
			case "buy":
				quantity, quantityValue, quantityErr := confirmedTokenQuantity(receipt.TokenDeltaRaw, receipt.TokenDecimals, 1)
				if receipt.SOLDelta >= 0 || quantityErr != nil {
					finalizeErr = fmt.Errorf("confirmed buy did not debit operational wallet")
				} else {
					cost := -receipt.SOLDelta - receipt.FeesSOL
					if cost <= 0 || quantityValue <= 0 {
						finalizeErr = fmt.Errorf("confirmed buy has no positive swap cost or token receipt")
					} else {
						finalizeErr = e.db.FinalizeLiveBuy(trade.id, trade.mint, status, trade.signature, cost, receipt.FeesSOL, quantity)
					}
				}
			case "sell":
				quantity, quantityValue, quantityErr := confirmedTokenQuantity(receipt.TokenDeltaRaw, receipt.TokenDecimals, -1)
				if receipt.SOLDelta <= 0 || quantityErr != nil {
					finalizeErr = fmt.Errorf("confirmed sell did not credit operational wallet")
				} else {
					value := receipt.SOLDelta + receipt.FeesSOL
					if value <= 0 || quantityValue <= 0 {
						finalizeErr = fmt.Errorf("confirmed sell has no positive SOL or token amount")
					} else {
						finalizeErr = e.db.FinalizeLiveSell(trade.id, trade.mint, status, trade.signature, value, receipt.FeesSOL, quantity)
					}
				}
			default:
				finalizeErr = fmt.Errorf("unsupported reconciled trade side %q", trade.side)
			}
			if finalizeErr != nil {
				_ = e.db.UpdateTradeState(trade.id, "unknown", trade.signature, false)
				e.emit("risk_alert", map[string]any{"tradeId": trade.id, "reason": finalizeErr.Error()})
			} else {
				e.release(trade.mint)
			}
		case "failed":
			if updateErr := e.db.UpdateTradeState(trade.id, "failed", trade.signature, true); updateErr == nil {
				e.release(trade.mint)
			} else {
				e.emit("risk_alert", map[string]any{"tradeId": trade.id, "reason": "failed transaction state could not be persisted"})
			}
		default:
			_ = e.db.UpdateTradeState(trade.id, "unknown", trade.signature, false)
		}
		_ = e.db.LogOperational("info", "reconciliation", "persisted transaction checked", fmt.Sprintf(`{"tradeId":%q,"state":%q}`, trade.id, status))
	}
	return nil
}
func (e *Engine) Execute(ctx context.Context, r Request) (string, error) {
	e.executionGate.RLock()
	defer e.executionGate.RUnlock()

	if r.Mint == "" || (r.Side != "buy" && r.Side != "sell") || r.Amount <= 0 || r.Slippage < 0 || r.PriorityFee < 0 {
		return "", fmt.Errorf("invalid trade request")
	}
	if r.Side == "sell" {
		if _, err := e.db.PositionCost(r.Mint); err != nil {
			return "", fmt.Errorf("sell requires an open position: %w", err)
		}
	}
	if r.Side == "buy" {
		if e.controller != nil && !e.controller.AllowsEntries() {
			return "", fmt.Errorf("bot is not accepting new entries")
		}
		if !e.simulation && !e.rpcs.Available() {
			return "", fmt.Errorf("no healthy RPC; purchases blocked")
		}
		if needed, err := e.db.ReconciliationRequired(); err != nil {
			return "", err
		} else if needed {
			return "", fmt.Errorf("reconciliation required before purchases")
		}
		l := e.Limits()
		if l.MaxTradeSOL > 0 && r.Amount > l.MaxTradeSOL {
			return "", fmt.Errorf("maximum capital per trade exceeded")
		}
		if l.MaxSlippagePct > 0 && r.Slippage > l.MaxSlippagePct {
			return "", fmt.Errorf("maximum slippage exceeded")
		}
		if l.MaxSlippageBPS > 0 && r.Slippage*100 > float64(l.MaxSlippageBPS) {
			return "", fmt.Errorf("maximum slippage in basis points exceeded")
		}
		if l.MaxPriorityFeeSOL > 0 && r.PriorityFee > l.MaxPriorityFeeSOL {
			return "", fmt.Errorf("maximum priority fee exceeded")
		}
		open, err := e.db.OpenPositionCount()
		if err != nil {
			return "", err
		}
		pending, err := e.db.PendingBuyCount()
		if err != nil {
			return "", err
		}
		if l.MaxOpenPositions > 0 && open+pending >= l.MaxOpenPositions {
			return "", fmt.Errorf("maximum simultaneous positions reached")
		}
		exposure, err := e.db.OpenExposureSOL()
		if err != nil {
			return "", err
		}
		pendingExposure, err := e.db.PendingBuyExposureSOL()
		if err != nil {
			return "", err
		}
		if l.MaxExposureSOL > 0 && exposure+pendingExposure+r.Amount > l.MaxExposureSOL {
			return "", fmt.Errorf("maximum exposure exceeded")
		}
		if loss, err := e.db.DailyRealizedLossSOL(e.simulation); err != nil {
			return "", err
		} else if l.MaxDailyLossSOL > 0 && loss >= l.MaxDailyLossSOL {
			return "", fmt.Errorf("daily loss limit reached")
		}
		if !e.simulation {
			spend, err := wallet.SOLToLamports(r.Amount)
			if err != nil {
				return "", err
			}
			fee, err := wallet.SOLToLamports(r.PriorityFee)
			if err != nil {
				return "", err
			}
			balance, err := e.rpcs.Balance(ctx, e.wallet.Address())
			if err != nil {
				return "", fmt.Errorf("wallet balance unavailable: %w", err)
			}
			if !e.wallet.CanAfford(balance, spend, fee) {
				return "", fmt.Errorf("insufficient wallet balance after fee reserve")
			}
		}
	}
	if active, err := e.db.ActiveForMint(r.Mint); err != nil {
		return "", err
	} else if active {
		return "", fmt.Errorf("duplicate or unreconciled transaction already exists for mint")
	}
	if r.Side == "buy" {
		if exists, err := e.db.HasPurchaseForMint(r.Mint); err != nil {
			return "", err
		} else if exists {
			return "", fmt.Errorf("duplicate purchase for mint is blocked")
		}
	}
	id := fmt.Sprintf("tr_%d", time.Now().UTC().UnixNano())
	simulationFees := 0.0
	if e.simulation {
		// Each simulated transaction pays its configured priority bid plus
		// one base network fee. A round trip contains one buy and one sell.
		simulationFees = r.PriorityFee + e.Limits().SimulationBaseFeeSOL
	}
	requestedValue := r.Amount
	if r.RequestedValueSOL > 0 {
		requestedValue = r.RequestedValueSOL
	}
	t := database.Trade{ID: id, Mint: r.Mint, Side: r.Side, Quantity: fmt.Sprint(r.Amount), Requested: requestedValue, Fees: simulationFees, Slippage: r.Slippage, State: "prepared", EntryReason: r.Reason, ExitReason: r.Reason}
	if err := e.db.InsertTrade(t); err != nil {
		return "", err
	}
	if e.simulation {
		if r.Side == "buy" {
			if err := e.db.FinalizeSimulationBuy(id, r.Mint, r.Amount, simulationFees); err != nil {
				return id, fmt.Errorf("record simulated position: %w", err)
			}
		} else {
			if finalizeErr := e.db.FinalizeSimulationSell(id, r.Mint, r.Amount, simulationFees); finalizeErr != nil {
				if resolutionErr := e.resolveSimulationSellFinalizationError(id, r, simulationFees, finalizeErr); resolutionErr != nil {
					return id, resolutionErr
				}
			}
		}
		e.emit(r.Side+"_confirmed", map[string]any{"tradeId": id, "mint": r.Mint, "simulation": true})
		e.emit("position_updated", map[string]any{"mint": r.Mint, "sourceTradeId": id, "action": r.Side, "state": "simulated"})
		return id, nil
	}
	prepared, err := e.prepareLiveRequest(ctx, id, r)
	if err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, err
	}
	amount := any(prepared.amount)
	if prepared.denominatedInSOL {
		amount = r.Amount
	}
	txBytes, err := e.portal.Prepare(ctx, pumpportal.Request{PublicKey: e.wallet.Address(), Action: r.Side, Mint: r.Mint, Amount: amount, DenominatedInSOL: fmt.Sprint(prepared.denominatedInSOL), Slippage: prepared.slippage, PriorityFee: r.PriorityFee, Pool: r.Pool})
	if err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, err
	}
	tx, err := solana.TransactionFromBytes(txBytes)
	if err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, fmt.Errorf("unsupported PumpPortal transaction encoding: %w", err)
	}
	if err = tx.Sanitize(); err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, fmt.Errorf("unsafe transaction: %w", err)
	}
	if err = e.validatePreparedTransaction(tx); err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, fmt.Errorf("unsafe prepared transaction: %w", err)
	}
	if prepared.quoteAgeLimit > 0 {
		quoteAge := time.Since(prepared.quotedAt)
		if quoteAge < 0 || quoteAge > time.Duration(prepared.quoteAgeLimit)*time.Millisecond {
			_ = e.db.MarkTradeFailed(id)
			return id, fmt.Errorf("bonding-curve quote exceeded MAX_QUOTE_AGE_MS before submission")
		}
		if err = e.db.UpdateTradeQuote(id, prepared.quotedAt, quoteAge.Milliseconds(), prepared.curveImpactBPS, prepared.minimumOutputSOL, prepared.requestedValueSOL); err != nil {
			_ = e.db.MarkTradeFailed(id)
			return id, fmt.Errorf("persist execution quote: %w", err)
		}
	}
	if !tx.IsSigner(e.wallet.PublicKey()) {
		_ = e.db.MarkTradeFailed(id)
		return id, fmt.Errorf("prepared transaction does not require operational wallet")
	}
	if err = e.wallet.Sign(tx); err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, err
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		_ = e.db.MarkTradeFailed(id)
		return id, err
	}
	signature := tx.Signatures[0].String()
	_ = e.db.UpdateTradeState(id, "signed", signature, false)
	attempts := e.Limits().MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	limits := e.Limits()
	submitted, err := e.rpcs.SendWithMaxRetries(ctx, raw, attempts-1, limits.SkipPreflight)
	if err != nil {
		_ = e.db.UpdateTradeState(id, "unknown", signature, false)
		e.emit(r.Side+"_failed", map[string]any{"tradeId": id, "mint": r.Mint, "reason": "submission outcome unknown"})
		return id, fmt.Errorf("submission outcome unknown; reconcile before retry: %w", err)
	}
	if submitted != "" && submitted != signature {
		_ = e.db.UpdateTradeState(id, "unknown", signature, false)
		e.emit(r.Side+"_failed", map[string]any{"tradeId": id, "mint": r.Mint, "reason": "RPC signature mismatch"})
		return id, fmt.Errorf("RPC returned a different signature")
	}
	_ = e.db.UpdateTradeState(id, "submitted", signature, false)
	e.emit(r.Side+"_sent", map[string]any{"tradeId": id, "mint": r.Mint, "signature": signature})
	go e.confirm(id, signature, r)
	return id, nil
}

func (e *Engine) prepareLiveRequest(ctx context.Context, id string, request Request) (livePreparation, error) {
	limits := e.Limits()
	if limits.MaxSlippageBPS < 1 || limits.MaxQuoteAgeMS < 1 || limits.MaxCurveImpactBPS < 1 {
		return livePreparation{}, fmt.Errorf("live execution requires explicit slippage, quote-age, and curve-impact limits")
	}
	if request.Slippage*100 > float64(limits.MaxSlippageBPS) {
		return livePreparation{}, fmt.Errorf("requested slippage exceeds MAX_SLIPPAGE_BPS")
	}
	state, err := e.rpcs.GetBondingCurveStateWithContext(ctx, request.Mint)
	if err != nil {
		return livePreparation{}, fmt.Errorf("fresh bonding-curve quote required: %w", err)
	}
	if state.Complete || state.VirtualSOLReserves == 0 || state.VirtualTokenReserves == 0 || state.ObservedAt.IsZero() {
		return livePreparation{}, fmt.Errorf("bonding-curve quote is not usable for protected execution")
	}
	prepared := livePreparation{
		denominatedInSOL: request.Side == "buy",
		slippage:         request.Slippage,
		quotedAt:         state.ObservedAt,
		quoteAgeLimit:    limits.MaxQuoteAgeMS,
		requestedValueSOL: request.Amount,
	}
	virtualSOL := float64(state.VirtualSOLReserves) / 1e9
	virtualTokens := float64(state.VirtualTokenReserves) / 1e6
	expectedSOL := 0.0

	if request.Side == "buy" {
		if request.Amount <= 0 || virtualSOL <= 0 {
			return livePreparation{}, fmt.Errorf("invalid buy quote input")
		}
		prepared.amount = request.Amount
		prepared.curveImpactBPS = request.Amount / (virtualSOL + request.Amount) * 10000
		expectedTokens := virtualTokens * request.Amount / (virtualSOL + request.Amount)
		if expectedTokens <= 0 || math.IsNaN(expectedTokens) || math.IsInf(expectedTokens, 0) {
			return livePreparation{}, fmt.Errorf("bonding-curve buy quote is invalid")
		}
	} else {
		raw, decimals, balanceErr := e.rpcs.TokenBalance(ctx, e.wallet.PublicKey(), request.Mint)
		if balanceErr != nil {
			return livePreparation{}, fmt.Errorf("confirmed wallet token balance required before selling: %w", balanceErr)
		}
		if raw == 0 {
			return livePreparation{}, fmt.Errorf("wallet holds no confirmed tokens to sell")
		}
		if decimals != 6 {
			return livePreparation{}, fmt.Errorf("unsupported token decimals for bonding-curve quote: %d", decimals)
		}
		tokenAmount := float64(raw) / math.Pow10(int(decimals))
		if tokenAmount <= 0 || virtualTokens <= 0 {
			return livePreparation{}, fmt.Errorf("invalid token amount or bonding-curve reserves")
		}
		prepared.amount = formatRawTokenAmount(raw, decimals)
		prepared.curveImpactBPS = tokenAmount / (virtualTokens + tokenAmount) * 10000
		expectedSOL = virtualSOL * tokenAmount / (virtualTokens + tokenAmount)
		if expectedSOL <= 0 || math.IsNaN(expectedSOL) || math.IsInf(expectedSOL, 0) {
			return livePreparation{}, fmt.Errorf("bonding-curve sell quote is invalid")
		}
		prepared.requestedValueSOL = expectedSOL

		cost, costErr := e.db.PositionCost(request.Mint)
		if costErr != nil {
			return livePreparation{}, fmt.Errorf("confirmed entry cost required to protect the sell: %w", costErr)
		}
		if cost <= 0 {
			return livePreparation{}, fmt.Errorf("confirmed entry cost must be positive to protect the sell")
		}
		maxLossFloor := cost * (1 - limits.MaxLossPerTradePct/100)
		// Network fees are separate from the swap output; reserve the configured
		// priority cap and base fee when enforcing the per-position loss floor.
		maxLossFloor += limits.MaxPriorityFeeSOL + 0.000005
		maxSlipBPS := math.Min(request.Slippage*100, float64(limits.MaxSlippageBPS))
		minimumOutput := expectedSOL * (1 - maxSlipBPS/10000)
		if maxLossFloor > minimumOutput {
			minimumOutput = maxLossFloor
		}
		if minimumOutput <= 0 || minimumOutput >= expectedSOL {
			return livePreparation{}, fmt.Errorf("quoted sale cannot satisfy the per-trade loss floor; position remains open")
		}
		prepared.minimumOutputSOL = minimumOutput
		prepared.slippage = (expectedSOL - minimumOutput) / expectedSOL * 100
		if prepared.slippage*100 > float64(limits.MaxSlippageBPS)+1e-6 {
			return livePreparation{}, fmt.Errorf("loss floor would require slippage above MAX_SLIPPAGE_BPS")
		}
	}

	if prepared.curveImpactBPS > float64(limits.MaxCurveImpactBPS) {
		return livePreparation{}, fmt.Errorf("estimated curve impact %.1f bps exceeds MAX_CURVE_IMPACT_BPS", prepared.curveImpactBPS)
	}
	return prepared, nil
}

func formatRawTokenAmount(raw uint64, decimals uint8) string {
	value := strconv.FormatUint(raw, 10)
	if decimals == 0 {
		return value
	}
	for len(value) <= int(decimals) {
		value = "0" + value
	}
	point := len(value) - int(decimals)
	whole, fraction := value[:point], strings.TrimRight(value[point:], "0")
	if fraction == "" {
		return whole
	}
	return whole + "." + fraction
}

// resolveSimulationSellFinalizationError classifies the outcome of a failed
// paper-sell finalization. It MUST leave the system in a terminal, retry-safe
// state. Two terminal outcomes exist:
//
//  1. Complete: trade/transaction/position all persisted as simulated/closed.
//  2. Definitive failure: trade/transaction failed AND position closed by the
//     dedicated MarkSimulationSellFailed helper, which also releases the slot.
//
// Anything else is marked unknown so ActiveForMint blocks a second sell, and
// ReconcileStaleSimulationSells later converts it to failed. Only when the
// unknown state cannot be persisted at all is an error returned to the caller.
func (e *Engine) resolveSimulationSellFinalizationError(id string, request Request, fees float64, finalizeErr error) error {
	return e.resolveSellFinalizationError(
		id, request, fees, finalizeErr,
		e.db.SimulationSellSnapshot,
		e.db.MarkSimulationSellFailed,
		e.db.MarkSimulationSellUnknown,
		true,
	)
}

// resolveLiveSellFinalizationError mirrors resolveSimulationSellFinalizationError
// for live sells. The state contract is identical: a failed finalization must
// end in either (a) complete (trade/transaction finalized + position closed) or
// (b) definitive failure (trade/transaction failed + position closed by
// MarkLiveSellFailed) or (c) unknown, which blocks a second sell until
// ReconcileStaleLiveSells or QuarantineStuckLiveMint resolves it.
func (e *Engine) resolveLiveSellFinalizationError(id string, request Request, fees float64, finalizeErr error) error {
	return e.resolveSellFinalizationError(
		id, request, fees, finalizeErr,
		e.db.LiveSellSnapshot,
		e.db.MarkLiveSellFailed,
		e.db.MarkLiveSellUnknown,
		false,
	)
}

func (e *Engine) resolveSellFinalizationError(
	id string,
	request Request,
	fees float64,
	finalizeErr error,
	snapshotFn func(string, string) (database.SimulationSellSnapshot, error),
	markFailedFn func(string, string) error,
	markUnknownFn func(string) error,
	isSimulation bool,
) error {
	snapshot, snapshotErr := snapshotFn(id, request.Mint)
	if snapshotErr == nil &&
		snapshot.TradeState == "simulated" &&
		snapshot.TransactionState == "simulated" &&
		snapshot.PositionState == "closed" {
		if e.log != nil {
			e.log.Warn("sell returned a finalization error but persisted as complete",
				zap.String("trade_id", id),
				zap.String("mint", request.Mint),
				zap.Bool("simulation", isSimulation),
				zap.Float64("requested_value_sol", request.Amount),
				zap.Error(finalizeErr))
		}
		return nil
	}
	if snapshotErr == nil &&
		snapshot.TradeState == "confirmed" &&
		snapshot.TransactionState == "confirmed" &&
		snapshot.PositionState == "closed" {
		if e.log != nil {
			e.log.Warn("sell returned a finalization error but persisted as complete",
				zap.String("trade_id", id),
				zap.String("mint", request.Mint),
				zap.Bool("simulation", isSimulation),
				zap.Float64("requested_value_sol", request.Amount),
				zap.Error(finalizeErr))
		}
		return nil
	}

	definitiveFailure := snapshotErr == nil &&
		(snapshot.TradeState == "prepared" || snapshot.TradeState == "confirmed") &&
		(snapshot.TransactionState == "prepared" || snapshot.TransactionState == "confirmed") &&
		snapshot.PositionState == "open"
	var stateErr error
	if definitiveFailure {
		stateErr = markFailedFn(id, request.Mint)
	} else {
		stateErr = markUnknownFn(id)
	}

	if stateErr != nil {
		after, verifyErr := snapshotFn(id, request.Mint)
		if verifyErr == nil &&
			((after.TradeState == "simulated" && after.TransactionState == "simulated") ||
				(after.TradeState == "confirmed" && after.TransactionState == "confirmed")) &&
			after.PositionState == "closed" {
			if e.log != nil {
				e.log.Warn("sell state resolved as complete after a persistence error",
					zap.String("trade_id", id),
					zap.String("mint", request.Mint),
					zap.Bool("simulation", isSimulation),
					zap.Error(finalizeErr),
					zap.Error(stateErr))
			}
			return nil
		}
		if verifyErr == nil &&
			after.TradeState == "failed" &&
			after.TransactionState == "failed" &&
			after.PositionState == "closed" {
			definitiveFailure = true
		} else {
			fallbackErr := markUnknownFn(id)
			if fallbackErr != nil {
				stateErr = fmt.Errorf("%w; unknown-state fallback also failed: %v", stateErr, fallbackErr)
			}
			if snapshotErr != nil {
				stateErr = fmt.Errorf("initial state inspection failed: %v; state update failed: %w", snapshotErr, stateErr)
			}
			if verifyErr != nil {
				stateErr = fmt.Errorf("%w; verification after state update failed: %v", stateErr, verifyErr)
			}
			if e.log != nil {
				e.log.Error("sell outcome is unresolved",
					zap.String("trade_id", id),
					zap.String("mint", request.Mint),
					zap.String("exit_reason", request.Reason),
					zap.Bool("simulation", isSimulation),
					zap.Float64("requested_value_sol", request.Amount),
					zap.Float64("modeled_fees_sol", fees),
					zap.Error(finalizeErr),
					zap.Error(stateErr))
			}
			e.emit("risk_alert", map[string]any{
				"tradeId": id, "mint": request.Mint, "simulation": isSimulation,
				"reason": "sell finalization is unresolved; automatic retry is blocked until reconciliation",
				"error":  finalizeErr.Error(),
			})
			return fmt.Errorf("close position: %w (failure-state persistence: %v)", finalizeErr, stateErr)
		}
	}

	if e.log != nil {
		e.log.Error("sell finalization failed",
			zap.String("trade_id", id),
			zap.String("mint", request.Mint),
			zap.String("exit_reason", request.Reason),
			zap.Bool("simulation", isSimulation),
			zap.Float64("requested_value_sol", request.Amount),
			zap.Float64("modeled_fees_sol", fees),
			zap.Bool("failure_state_persisted", definitiveFailure),
			zap.Error(finalizeErr))
	}
	if !definitiveFailure {
		e.emit("risk_alert", map[string]any{
			"tradeId": id, "mint": request.Mint, "simulation": isSimulation,
			"reason": "sell outcome is unknown; automatic retry is blocked until reconciliation",
			"error":  finalizeErr.Error(),
		})
	}
	// The failure/unknown state is persisted and ActiveForMint will block a
	// duplicate sell. Returning nil here is what stops the monitor from
	// re-evaluating this mint in a tight loop. ReconcileStale*Sells will close
	// the position on the next monitor pass.
	return nil
}

func confirmedTokenQuantity(raw string, decimals uint8, expectedSign int) (string, float64, error) {
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok || value.Sign() == 0 || (expectedSign > 0 && value.Sign() < 0) || (expectedSign < 0 && value.Sign() > 0) {
		return "", 0, fmt.Errorf("confirmed transaction has no expected token balance delta")
	}
	value.Abs(value)
	digits := value.String()
	if decimals == 0 {
		amount, err := strconv.ParseFloat(digits, 64)
		return digits, amount, err
	}
	for len(digits) <= int(decimals) {
		digits = "0" + digits
	}
	point := len(digits) - int(decimals)
	whole, fraction := digits[:point], strings.TrimRight(digits[point:], "0")
	quantity := whole
	if fraction != "" {
		quantity += "." + fraction
	}
	amount, err := strconv.ParseFloat(quantity, 64)
	if err != nil || amount <= 0 {
		return "", 0, fmt.Errorf("confirmed token quantity cannot be represented")
	}
	return quantity, amount, nil
}

func (e *Engine) confirm(id, sig string, r Request) {
	defer e.release(r.Mint)
	wait := e.Limits().MaxWaitSeconds
	if wait < 1 {
		wait = 12
	}
	e.mu.RLock()
	rootCtx := e.rootCtx
	e.mu.RUnlock()
	if rootCtx == nil {
		rootCtx = context.Background()
	}
	ctx, c := context.WithTimeout(rootCtx, time.Duration(wait)*time.Second)
	defer c()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		state, err := e.rpcs.Status(ctx, sig)
		if err == nil {
			switch state {
			case "confirmed", "finalized":
				receipt, executionErr := e.rpcs.Execution(ctx, sig, e.wallet.Address(), r.Mint)
				if executionErr != nil {
					_ = e.db.UpdateTradeState(id, "unknown", sig, false)
					e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmed transaction lacks verified execution data"})
					return
				}
				if r.Side == "buy" {
					quantity, quantityValue, quantityErr := confirmedTokenQuantity(receipt.TokenDeltaRaw, receipt.TokenDecimals, 1)
					if receipt.SOLDelta >= 0 || quantityErr != nil { // buy must debit SOL and receive tokens
						_ = e.db.UpdateTradeState(id, "unknown", sig, false)
						e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "buy confirmation lacks a verified SOL debit or token receipt"})
						return
					}
					cost := -receipt.SOLDelta - receipt.FeesSOL
					if cost <= 0 || quantityValue <= 0 {
						_ = e.db.UpdateTradeState(id, "unknown", sig, false)
						e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmed buy has invalid swap cost or token quantity"})
						return
					}
					if err = e.db.FinalizeLiveBuy(id, r.Mint, state, sig, cost, receipt.FeesSOL, quantity); err != nil {
						_ = e.db.UpdateTradeState(id, "unknown", sig, false)
						e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmed buy could not be committed to the live ledger"})
						return
					}
				} else {
					quantity, quantityValue, quantityErr := confirmedTokenQuantity(receipt.TokenDeltaRaw, receipt.TokenDecimals, -1)
					if receipt.SOLDelta <= 0 || quantityErr != nil {
						_ = e.db.UpdateTradeState(id, "unknown", sig, false)
						e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "sell confirmation lacks verified SOL proceeds or token debit"})
						return
					}
					value := receipt.SOLDelta + receipt.FeesSOL
					if value <= 0 || quantityValue <= 0 {
						_ = e.db.UpdateTradeState(id, "unknown", sig, false)
						e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmed sell has invalid SOL proceeds or token quantity"})
						return
					}
					if err = e.db.FinalizeLiveSell(id, r.Mint, state, sig, value, receipt.FeesSOL, quantity); err != nil {
						if resolutionErr := e.resolveLiveSellFinalizationError(id, r, receipt.FeesSOL, err); resolutionErr != nil {
							e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmed sell could not be committed to the live ledger"})
						}
						return
					}
				}
				e.emit(r.Side+"_confirmed", map[string]any{"tradeId": id, "mint": r.Mint, "signature": sig, "state": state})
				e.emit("position_updated", map[string]any{"mint": r.Mint, "sourceTradeId": id, "action": r.Side, "state": state})
				return
			case "failed":
				if updateErr := e.db.UpdateTradeState(id, "failed", sig, true); updateErr != nil {
					e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "failed transaction state could not be persisted"})
					return
				}
				e.emit(r.Side+"_failed", map[string]any{"tradeId": id, "mint": r.Mint, "signature": sig})
				return
			}
			_ = e.db.UpdateTradeState(id, "pending", sig, false)
		}
		select {
		case <-ctx.Done():
			_ = e.db.UpdateTradeState(id, "unknown", sig, false)
			e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "confirmation timeout; reconciliation required"})
			return
		case <-tick.C:
		}
	}
}

// validatePreparedTransaction is deliberately conservative: PumpPortal may
// construct the transaction, but only the configured wallet may authorize it
// and that wallet must be the fee payer. Address-table transactions are
// rejected until their tables are resolved and inspected locally.
func (e *Engine) validatePreparedTransaction(tx *solana.Transaction) error {
	if len(tx.Message.AccountKeys) == 0 || tx.Message.AccountKeys[0] != e.wallet.PublicKey() {
		return fmt.Errorf("operational wallet is not the fee payer")
	}
	if tx.Message.Header.NumRequiredSignatures != 1 || len(tx.Signatures) != 1 {
		return fmt.Errorf("transaction must require exactly one signer")
	}
	if len(tx.Message.AddressTableLookups) != 0 {
		return fmt.Errorf("address lookup transactions are not permitted without local lookup resolution")
	}
	if tx.Message.RecentBlockhash.IsZero() || len(tx.Message.Instructions) == 0 {
		return fmt.Errorf("missing recent blockhash or instructions")
	}
	for _, ix := range tx.Message.Instructions {
		if int(ix.ProgramIDIndex) >= len(tx.Message.AccountKeys) || len(ix.Data) == 0 {
			return fmt.Errorf("malformed instruction")
		}
		for _, account := range ix.Accounts {
			if int(account) >= len(tx.Message.AccountKeys) {
				return fmt.Errorf("instruction account index out of range")
			}
		}
	}
	return nil
}