package positions

import (
	"testing"
	"time"
)

func TestExitUsesVerifiedLiquidity(t *testing.T) {
	why := ExitReason(Position{CostSOL: 1, OpenedAt: time.Now()}, 2, false, Limits{TakeProfitPct: 10}, time.Now())
	if why != "insufficient verified liquidity" {
		t.Fatal(why)
	}
}

func TestTrailingStopUsesPeakValueAndActualPositionCost(t *testing.T) {
	now := time.Now().UTC()
	position := Position{Mint: "mint", CostSOL: 0.25, PeakSOL: 0.5, OpenedAt: now}
	limits := Limits{TakeProfitPct: 80, StopLossPct: 20, TrailingStopPct: 12}

	if got := ExitReason(position, 0.42, true, limits, now); got != "trailing stop" {
		t.Fatalf("peak drawdown should trigger trailing stop; got %q", got)
	}
	if got := ExitReason(position, 0.45, true, limits, now); got != "" {
		t.Fatalf("drawdown below trailing threshold should not exit; got %q", got)
	}
}

func TestTrailingStopDoesNotArmBeforeTenPercentGain(t *testing.T) {
	now := time.Now().UTC()
	position := Position{CostSOL: 0.25, PeakSOL: 0.2625, OpenedAt: now}
	limits := Limits{StopLossPct: 20, TrailingStopPct: 12}
	if got := ExitReason(position, 0.24, true, limits, now); got != "" {
		t.Fatalf("trailing stop armed below the required peak gain: %q", got)
	}
}
