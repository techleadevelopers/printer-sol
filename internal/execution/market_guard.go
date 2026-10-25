package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/gagliardetto/solana-go"
)

// MarketKind classifica o mercado atual de um mint.
type MarketKind string

const (
	MarketBondingCurve MarketKind = "pump_curve"
	MarketPumpSwap     MarketKind = "pump_amm"
	MarketTransition   MarketKind = "transition"
	MarketUnsupported  MarketKind = "unsupported"
)

// MarketDecision é a decisão explícita de rota para uma operação.
type MarketDecision struct {
	Kind       MarketKind
	Pool       string // "pump", "pump-amm", ou "" (transição/bloqueado)
	Reason     string
	ObservedAt time.Time
}

// ResolveMarketRoute consulta o estado on-chain e decide a rota.
// Não envia transações; apenas classifica.
func (e *Engine) ResolveMarketRoute(ctx context.Context, mint string) (MarketDecision, error) {
	if mint == "" {
		return MarketDecision{}, fmt.Errorf("mint is required")
	}

	state, err := e.rpcs.GetBondingCurveStateWithContext(ctx, mint)
	if err != nil {
		return MarketDecision{
			Kind:       MarketTransition,
			Reason:     fmt.Sprintf("bonding-curve read failed: %v", err),
			ObservedAt: time.Now().UTC(),
		}, nil
	}

	// Cotação não-SOL: a rota directa Pump.fun não é válida.
	if state.QuoteMint != (solana.PublicKey{}) {
		return MarketDecision{
			Kind:       MarketUnsupported,
			Reason:     fmt.Sprintf("non-SOL quote mint %s is not supported by the direct route", state.QuoteMint.String()),
			ObservedAt: time.Now().UTC(),
		}, nil
	}

	// Curva ainda activa: rota Pump.fun.
	if !state.Complete {
		if state.VirtualSOLReserves == 0 || state.VirtualTokenReserves == 0 {
			return MarketDecision{
				Kind:       MarketUnsupported,
				Reason:     "active bonding curve has zero reserves",
				ObservedAt: time.Now().UTC(),
			}, nil
		}
		return MarketDecision{
			Kind:       MarketBondingCurve,
			Pool:       "pump",
			Reason:     "bonding curve is active and SOL-quoted",
			ObservedAt: state.ObservedAt,
		}, nil
	}

	// Curva concluída: tentar confirmar o pool PumpSwap.
	pool, poolErr := e.rpcs.FindPumpSwapPool(ctx, mint)
	if poolErr != nil {
		return MarketDecision{
			Kind:       MarketTransition,
			Reason:     fmt.Sprintf("bonding curve complete; PumpSwap pool not confirmed: %v", poolErr),
			ObservedAt: time.Now().UTC(),
		}, nil
	}
	if pool == nil {
		return MarketDecision{
			Kind:       MarketTransition,
			Reason:     "bonding curve complete; PumpSwap pool not yet indexed",
			ObservedAt: time.Now().UTC(),
		}, nil
	}
	return MarketDecision{
		Kind:       MarketPumpSwap,
		Pool:       "pump-amm",
		Reason:     fmt.Sprintf("PumpSwap pool %s confirmed", pool.PoolAddress.String()),
		ObservedAt: time.Now().UTC(),
	}, nil
}

// pumpSwapPool é o mínimo necessário para validar a rota AMM.
type pumpSwapPool struct {
	PoolAddress solana.PublicKey
	BaseMint    solana.PublicKey
	QuoteMint   solana.PublicKey
}

// validateMarketDecision garante que a decisão é utilizável para a operação.
func validateMarketDecision(d MarketDecision, side string) error {
	switch d.Kind {
	case MarketBondingCurve:
		if d.Pool != "pump" {
			return fmt.Errorf("bonding-curve decision has unexpected pool %q", d.Pool)
		}
		return nil
	case MarketPumpSwap:
		if d.Pool != "pump-amm" {
			return fmt.Errorf("PumpSwap decision has unexpected pool %q", d.Pool)
		}
		return nil
	case MarketTransition:
		return fmt.Errorf("market is in transition: %s", d.Reason)
	default:
		return fmt.Errorf("market route is not supported: %s", d.Reason)
	}
}