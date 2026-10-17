package execution

import (
	"context"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"go.uber.org/zap"
	"solana-sniper-backend/internal/bot"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/pumpportal"
	"solana-sniper-backend/internal/pricefeed"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/wallet"
	"sync"
	"time"
)

type Request struct {
	Mint, Side            string
	Amount                float64
	DenominatedInSOL      bool
	Slippage, PriorityFee float64
	Pool, Reason          string
}
type Limits struct {
	MaxTradeSOL, MaxExposureSOL, MaxDailyLossSOL, MaxSlippagePct, MaxPriorityFeeSOL, FeeReserveSOL float64
	MaxOpenPositions, MaxWaitSeconds, MaxAttempts                                                  int
}
type Engine struct {
	db         *database.DB
	wallet     *wallet.Wallet
	portal     *pumpportal.Client
	rpcs       *rpcmanager.Manager
	prices     pricefeed.Provider
	log        *zap.Logger
	simulation bool
	controller *bot.Controller
	bus        *events.Bus
	mu         sync.RWMutex
	limits     Limits
	reserved   map[string]uint64
}

func New(db *database.DB, w *wallet.Wallet, p *pumpportal.Client, r *rpcmanager.Manager, prices pricefeed.Provider, l *zap.Logger, sim bool, c *bot.Controller, b *events.Bus, limits Limits) *Engine {
	return &Engine{db: db, wallet: w, portal: p, rpcs: r, prices: prices, log: l, simulation: sim, controller: c, bus: b, limits: limits, reserved: make(map[string]uint64)}
}
func (e *Engine) Limits() Limits     { e.mu.RLock(); defer e.mu.RUnlock(); return e.limits }
func (e *Engine) SetLimits(l Limits) { e.mu.Lock(); e.limits = l; e.mu.Unlock() }
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
		active, activeErr := e.db.ActiveForMint(mint)
		if activeErr != nil || !active {
			e.release(mint)
		}
		return id, err
	}
	if e.simulation {
		e.release(mint)
	}
	return id, nil
}

func (e *Engine) reserve(ctx context.Context, mint string, needed uint64) error {
	balance, err := e.rpcs.Balance(ctx, e.wallet.Address())
	if err != nil {
		return fmt.Errorf("wallet balance unavailable: %w", err)
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
	if balance < reserve+reserved+needed {
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
	defer rows.Close()
	for rows.Next() {
		var id, sig, state string
		if err = rows.Scan(&id, &sig, &state); err != nil {
			return err
		}
		if sig == "" {
			_ = e.db.UpdateTradeState(id, "unknown", "", false)
			e.emit("risk_alert", map[string]any{"tradeId": id, "reason": "persisted operation has no signature; manual reconciliation required"})
			continue
		}
		status, statusErr := e.rpcs.Status(ctx, sig)
		if statusErr != nil {
			e.emit("operational_error", map[string]any{"component": "reconciliation", "tradeId": id, "message": "RPC status unavailable"})
			continue
		}
		switch status {
		case "confirmed", "finalized":
			_ = e.db.UpdateTradeState(id, status, sig, true)
		case "failed":
			_ = e.db.UpdateTradeState(id, "failed", sig, true)
		default:
			_ = e.db.UpdateTradeState(id, "unknown", sig, false)
		}
		_ = e.db.LogOperational("info", "reconciliation", "persisted transaction checked", fmt.Sprintf(`{"tradeId":%q,"state":%q}`, id, status))
	}
	return rows.Err()
}
func (e *Engine) Execute(ctx context.Context, r Request) (string, error) {
	if r.Mint == "" || (r.Side != "buy" && r.Side != "sell") || r.Amount <= 0 || r.Slippage < 0 || r.PriorityFee < 0 {
		return "", fmt.Errorf("invalid trade request")
	}
	if r.Side == "buy" {
		if e.controller != nil && !e.controller.AllowsEntries() {
			return "", fmt.Errorf("bot is not accepting new entries")
		}
		if !e.rpcs.Available() {
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
		if l.MaxPriorityFeeSOL > 0 && r.PriorityFee > l.MaxPriorityFeeSOL {
			return "", fmt.Errorf("maximum priority fee exceeded")
		}
		if n, err := e.db.OpenPositionCount(); err != nil {
			return "", err
		} else if l.MaxOpenPositions > 0 && n >= l.MaxOpenPositions {
			return "", fmt.Errorf("maximum simultaneous positions reached")
		}
		if exposure, err := e.db.OpenExposureSOL(); err != nil {
			return "", err
		} else if l.MaxExposureSOL > 0 && exposure+r.Amount > l.MaxExposureSOL {
			return "", fmt.Errorf("maximum exposure exceeded")
		}
		if loss, err := e.db.DailyRealizedLossSOL(); err != nil {
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
	t := database.Trade{ID: id, Mint: r.Mint, Side: r.Side, Quantity: fmt.Sprint(r.Amount), Requested: r.Amount, Slippage: r.Slippage, State: "prepared", EntryReason: r.Reason}
	if err := e.db.InsertTrade(t); err != nil {
		return "", err
	}
	if e.simulation {
		_ = e.db.UpdateTradeState(id, "simulated", "", true)
		e.emit(r.Side+"_confirmed", map[string]any{"tradeId": id, "mint": r.Mint, "simulation": true})
		e.emit("position_updated", map[string]any{"mint": r.Mint, "sourceTradeId": id, "action": r.Side, "state": "simulated"})
		return id, nil
	}
	txBytes, err := e.portal.Prepare(ctx, pumpportal.Request{PublicKey: e.wallet.Address(), Action: r.Side, Mint: r.Mint, Amount: r.Amount, DenominatedInSOL: fmt.Sprint(r.DenominatedInSOL), Slippage: r.Slippage, PriorityFee: r.PriorityFee, Pool: r.Pool})
	if err != nil {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, err
	}
	tx, err := solana.TransactionFromBytes(txBytes)
	if err != nil {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, fmt.Errorf("unsupported PumpPortal transaction encoding: %w", err)
	}
	if err = tx.Sanitize(); err != nil {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, fmt.Errorf("unsafe transaction: %w", err)
	}
	if !tx.IsSigner(e.wallet.PublicKey()) {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, fmt.Errorf("prepared transaction does not require operational wallet")
	}
	if err = e.wallet.Sign(tx); err != nil {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, err
	}
	raw, err := tx.MarshalBinary()
	if err != nil {
		_ = e.db.UpdateTradeState(id, "failed", "", false)
		return id, err
	}
	signature := tx.Signatures[0].String()
	_ = e.db.UpdateTradeState(id, "signed", signature, false)
	submitted, err := e.rpcs.Send(ctx, raw)
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
	go e.confirm(id, signature, r.Side, r.Mint)
	return id, nil
}
func (e *Engine) confirm(id, sig, side, mint string) {
	wait := e.Limits().MaxWaitSeconds
	if wait < 1 {
		wait = 45
	}
	ctx, c := context.WithTimeout(context.Background(), time.Duration(wait)*time.Second)
	defer c()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		state, err := e.rpcs.Status(ctx, sig)
		if err == nil {
			switch state {
			case "confirmed", "finalized":
				_ = e.db.UpdateTradeState(id, state, sig, true)
				e.release(mint)
				e.emit(side+"_confirmed", map[string]any{"tradeId": id, "mint": mint, "signature": sig, "state": state})
				e.emit("position_updated", map[string]any{"mint": mint, "sourceTradeId": id, "action": side, "state": state})
				return
			case "failed":
				_ = e.db.UpdateTradeState(id, "failed", sig, true)
				e.release(mint)
				e.emit(side+"_failed", map[string]any{"tradeId": id, "mint": mint, "signature": sig})
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
