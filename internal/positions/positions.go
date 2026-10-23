// Package positions keeps exit decisions separate from transaction execution.
package positions

import "time"

type Limits struct {
TakeProfitPct, StopLossPct, TrailingStopPct, TrailingStopArmPct, ExitTriggerBufferPct float64
MaxSlippagePct, MaxExposureSOL, MaxDailyLossSOL                                        float64
MaxHold                                                                               time.Duration
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
if l.StopLossPct > 0 && change <= -l.StopLossPct+l.ExitTriggerBufferPct {
		return "stop loss"
	}

	if l.TrailingStopPct > 0 && p.PeakSOL > p.CostSOL && currentSOL > 0 {
		peakProfitPct := (p.PeakSOL - p.CostSOL) / p.CostSOL * 100
if peakProfitPct >= l.TrailingStopArmPct {
			drawdownFromPeak := (p.PeakSOL - currentSOL) / p.PeakSOL * 100
if drawdownFromPeak >= l.TrailingStopPct-l.ExitTriggerBufferPct {
				return "trailing stop"
			}
		}
	}

if l.TakeProfitPct > 0 && change >= l.TakeProfitPct-l.ExitTriggerBufferPct {
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