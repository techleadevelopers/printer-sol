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

func TestSimulationMaxHoldExtendsFlatPositionsOnce(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{TakeProfitPct: 50, StopLossPct: 25, TrailingStopPct: 12, MaxHold: 90 * time.Second, Simulation: true}

	if got := ExitReason(position, 1, true, limits, openedAt.Add(90*time.Second)); got != "" {
		t.Fatalf("flat simulation position should receive one hold extension; got %q", got)
	}
	if got := ExitReason(position, 1, true, limits, openedAt.Add(180*time.Second)); got != "max hold time (extended)" {
		t.Fatalf("flat simulation position should close after the extension; got %q", got)
	}
}

func TestSimulationMaxHoldStillExitsWhenOutsideFlatRange(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{TakeProfitPct: 50, StopLossPct: 25, TrailingStopPct: 12, MaxHold: 90 * time.Second, Simulation: true}

	for _, current := range []float64{1.06, 0.89} {
		if got := ExitReason(position, current, true, limits, openedAt.Add(90*time.Second)); got != "max hold time" {
			t.Fatalf("time stop should still apply at current value %.2f; got %q", current, got)
		}
	}
}

func TestLiveMaxHoldBehaviorIsUnchanged(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{MaxHold: 90 * time.Second}

	if got := ExitReason(position, 1, true, limits, openedAt.Add(90*time.Second)); got != "max hold time" {
		t.Fatalf("live time stop should remain unchanged; got %q", got)
	}
}
