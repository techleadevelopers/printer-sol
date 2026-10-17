// Package positions keeps exit decisions separate from transaction execution.
package positions

import "time"

type Limits struct {
	TakeProfitPct, StopLossPct, MaxSlippagePct, MaxExposureSOL, MaxDailyLossSOL float64
	MaxHold                                                                     time.Duration
}
type Position struct {
	Mint              string
	Quantity, CostSOL float64
	OpenedAt          time.Time
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
	if l.TakeProfitPct > 0 && change >= l.TakeProfitPct {
		return "take profit"
	}
	if l.MaxHold > 0 && now.Sub(p.OpenedAt) >= l.MaxHold {
		return "max hold time"
	}
	return ""
}
