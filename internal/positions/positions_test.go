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

func TestMaxHoldReleasesFlatPositionAfterLimit(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}

	for _, mode := range []string{"simulation", "live"} {
		t.Run(mode, func(t *testing.T) {
			limits := Limits{TakeProfitPct: 50, StopLossPct: 25, TrailingStopPct: 12, MaxHold: 90 * time.Second}
			if got := ExitReason(position, 1, true, limits, openedAt.Add(89*time.Second)); got != "" {
				t.Fatalf("flat position exited before its hold limit: %q", got)
			}
			for _, age := range []time.Duration{90 * time.Second, 180 * time.Second, 30 * time.Minute} {
				if got := ExitReason(position, 1, true, limits, openedAt.Add(age)); got != "max hold (position negative)" {
					t.Fatalf("flat position should release its slot after %s; got %q", age, got)
				}
			}
		})
	}
}

func TestMaxHoldKeepsPositivePositionsOpen(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{TakeProfitPct: 50, StopLossPct: 25, TrailingStopPct: 12, MaxHold: 90 * time.Second}

	for _, current := range []float64{1.01, 1.049, 1.30} {
		if got := ExitReason(position, current, true, limits, openedAt.Add(90*time.Second)); got != "" {
			t.Fatalf("positive return %.3f should remain open after max hold; got %q", current, got)
		}
	}
}

func TestMaxHoldExitsNegativeReturnAtLimit(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{TakeProfitPct: 50, StopLossPct: 25, TrailingStopPct: 12, MaxHold: 90 * time.Second}

	for _, current := range []float64{0.90, 0.89} {
		if got := ExitReason(position, current, true, limits, openedAt.Add(90*time.Second)); got != "max hold (position negative)" {
			t.Fatalf("negative return should max-hold close at %.2f; got %q", current, got)
		}
	}
}

func TestMaxHoldLeavesPositiveBoundaryOpenAndExitsNegativeBoundary(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{MaxHold: 90 * time.Second}

	if got := ExitReason(position, 1.05, true, limits, openedAt.Add(90*time.Second)); got != "" {
		t.Fatalf("positive boundary should remain open; got %q", got)
	}
	if got := ExitReason(position, 0.90, true, limits, openedAt.Add(90*time.Second)); got != "max hold (position negative)" {
		t.Fatalf("negative boundary should max-hold close; got %q", got)
	}
}

func TestMaxHoldTableForPositiveThirtyAndNegativeTenPercent(t *testing.T) {
	openedAt := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	position := Position{CostSOL: 1, PeakSOL: 1, OpenedAt: openedAt}
	limits := Limits{MaxHold: 90 * time.Second}
	now := openedAt.Add(91 * time.Second)

	if got := ExitReason(position, 1.30, true, limits, now); got != "" {
		t.Fatalf("+30%% return should remain open after max hold; got %q", got)
	}
	if got := ExitReason(position, 0.90, true, limits, now); got != "max hold (position negative)" {
		t.Fatalf("-10%% return should close after max hold; got %q", got)
	}
}
