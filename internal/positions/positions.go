// Package positions keeps exit decisions separate from transaction execution.
package positions

import "time"

const (
	holdProfitExitPct = 5.0
	holdLossExitPct   = -10.0
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
//  5. max hold             — always exits, regardless of return, so an open
//     position can never pin MaxOpenPositions once its hold window expires.
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
	if l.StopLossPct > 0 && change <= -l.StopLossPct {
		return "stop loss"
	}

	if l.TrailingStopPct > 0 && p.PeakSOL > p.CostSOL && currentSOL > 0 {
		peakProfitPct := (p.PeakSOL - p.CostSOL) / p.CostSOL * 100
		if peakProfitPct >= trailingArmProfitPct {
			drawdownFromPeak := (p.PeakSOL - currentSOL) / p.PeakSOL * 100
			if drawdownFromPeak >= l.TrailingStopPct {
				return "trailing stop"
			}
		}
	}

	if l.TakeProfitPct > 0 && change >= l.TakeProfitPct {
		return "take profit"
	}

	if l.MaxHold > 0 {
		age := now.Sub(p.OpenedAt)
		if age >= l.MaxHold {
			// The hold window has expired. The position must leave the book
			// so MaxOpenPositions is not pinned by a token that never reaches
			// a hard trigger. The reason string distinguishes the sub-cases
			// for audit but every branch returns a non-empty reason.
			switch {
			case change == 0:
				return "max hold flat (liberar vaga)"
			case change >= holdProfitExitPct || change <= holdLossExitPct:
				return "max hold time"
			default:
				// Between the profit/loss thresholds: still force an exit so
				// the slot is released. This is the case the previous version
				// left open forever.
				return "max hold neutral (liberar vaga)"
			}
		}
	}
	return ""
}