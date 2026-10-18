// Package positions keeps exit decisions separate from transaction execution.
package positions

import "time"

const (
	holdProfitExitPct = 5.0
	holdLossExitPct   = -10.0
)

type Limits struct {
	TakeProfitPct, StopLossPct, TrailingStopPct, MaxSlippagePct, MaxExposureSOL, MaxDailyLossSOL float64
	MaxHold                                                                                      time.Duration
}
type Position struct {
	Mint                         string
	Quantity, CostSOL, PeakSOL   float64
	OpenedAt                     time.Time
}

// ExitReason returns only deterministic limit triggers. Price and liquidity must come from a verified source.
func ExitReason(p Position, currentSOL float64, liquid bool, l Limits, now time.Time) string {
	if !liquid {
		return "insufficient verified liquidity"
	}
	if p.CostSOL <= 0 {
		return "cost pending reconciliation"
	}
	change := (currentSOL - p.CostSOL) / p.CostSOL * 100
	if l.StopLossPct > 0 && change <= -l.StopLossPct {
		return "stop loss"
	}
	if l.TrailingStopPct > 0 && p.PeakSOL > p.CostSOL && currentSOL > 0 {
		const trailingArmProfitPct = 10.0
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
			if change >= holdProfitExitPct || change <= holdLossExitPct {
				return "max hold time"
			}
			if age-l.MaxHold >= l.MaxHold {
				return "max hold time (extended)"
			}
		}
	}
	return ""
}
