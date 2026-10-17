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
