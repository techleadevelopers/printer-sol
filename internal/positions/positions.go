// Package positions keeps exit decisions separate from transaction execution.
package positions

import "time"

const (
	// trailingArmProfitPct is the profit level at which the trailing stop
	// becomes armed. Kept as a constant (not a Limits field) so the exit
	// behaviour is deterministic across restarts.
	trailingArmProfitPct = 10.0
)

type Limits struct {
	TakeProfitPct, StopLossPct, TrailingStopPct, MaxSlippagePct, MaxExposureSOL, MaxDailyLossSOL float64
	MaxHold                                                                                      time.Duration
}
type Position struct {
	Mint                       string
	Quantity, CostSOL, PeakSOL float64
	OpenedAt                   time.Time
}

// ExitReason returns only deterministic limit triggers. Price and liquidity
// must come from a verified source.
//
// Ordering matters and is part of the state machine contract:
//
//  1. insufficient liquidity / cost pending reconciliation — no decision
//  2. stop loss            — the only reason that triggers creator blacklist
//  3. trailing stop        — only after the peak armed the trailing arm
//  4. take profit
//  5. max hold             — only exits NEGATIVE (or flat) positions.
//     Positive positions are left open so take profit / trailing stop
//     can still run. This prevents cutting winners short.
func ExitReason(p Position, currentSOL float64, liquid bool, l Limits, now time.Time) string {
	if !liquid {
		return "insufficient verified liquidity"
	}
	if p.CostSOL <= 0 {
		return "cost pending reconciliation"
	}
	change := (currentSOL - p.CostSOL) / p.CostSOL * 100

	// Stop loss takes precedence over every other trigger so the persisted
	// exit reason is what the stop-loss creator blacklist expects to match.
	// TOLERÂNCIA DE 0.5%: vende quando o preço chega a 0.5% antes do gatilho,
	// compensando o atraso da última marca do RPC.
	if l.StopLossPct > 0 && change <= -l.StopLossPct+0.5 {
		return "stop loss"
	}

	if l.TrailingStopPct > 0 && p.PeakSOL > p.CostSOL && currentSOL > 0 {
		peakProfitPct := (p.PeakSOL - p.CostSOL) / p.CostSOL * 100
		if peakProfitPct >= trailingArmProfitPct {
			drawdownFromPeak := (p.PeakSOL - currentSOL) / p.PeakSOL * 100
			// TOLERÂNCIA DE 0.5%: vende quando a queda do pico chega a 0.5% antes do gatilho.
			if drawdownFromPeak >= l.TrailingStopPct-0.5 {
				return "trailing stop"
			}
		}
	}

	// TOLERÂNCIA DE 0.5%: vende quando o lucro chega a 0.5% antes do gatilho do Take Profit.
	if l.TakeProfitPct > 0 && change >= l.TakeProfitPct-0.5 {
		return "take profit"
	}

	// Max hold only forces an exit for NEGATIVE (or flat) positions.
	// Positive positions are left open so take profit and trailing stop
	// can still run; closing them early would cut winners short.
	if l.MaxHold > 0 && now.Sub(p.OpenedAt) >= l.MaxHold {
		if change <= 0 {
			return "max hold (position negative)"
		}
	}
	return ""
}