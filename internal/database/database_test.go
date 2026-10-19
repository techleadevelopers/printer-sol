package database

import (
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
)

func TestPaperMarkStatsCountsFlatMarks(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "paper-mark-stats.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	total, flat, pctFlat, err := db.PaperMarkStats()
	if err != nil || total != 0 || flat != 0 || pctFlat != 0 {
		t.Fatalf("empty mark stats = (%d, %d, %.2f), err=%v; want (0, 0, 0)", total, flat, pctFlat, err)
	}
	if err = db.OpenPosition("flat-mint", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("moving-mint", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("flat-mint", 100); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("moving-mint", 200); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdatePaperMark("moving-mint", 250); err != nil {
		t.Fatal(err)
	}

	total, flat, pctFlat, err = db.PaperMarkStats()
	if err != nil || total != 2 || flat != 1 || pctFlat != 50 {
		t.Fatalf("mark stats = (%d, %d, %.2f), err=%v; want (2, 1, 50)", total, flat, pctFlat, err)
	}
	if _, err = db.ClosePosition("moving-mint"); err != nil {
		t.Fatal(err)
	}
	total, flat, pctFlat, err = db.PaperMarkStats()
	if err != nil || total != 1 || flat != 1 || pctFlat != 100 {
		t.Fatalf("open mark stats after close = (%d, %d, %.2f), err=%v; want (1, 1, 100)", total, flat, pctFlat, err)
	}
}

func TestConcurrentPaperMarkUpdatesSerializeAndPreservePeak(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "paper-mark-concurrency.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("concurrent-mint", 100); err != nil {
		t.Fatal(err)
	}

	const updates = 40
	var wait sync.WaitGroup
	errors := make(chan error, updates)
	for value := 101; value <= 100+updates; value++ {
		wait.Add(1)
		go func(marketCap float64) {
			defer wait.Done()
			if updateErr := db.UpdatePaperMark("concurrent-mint", marketCap); updateErr != nil {
				errors <- updateErr
			}
		}(float64(value))
	}
	wait.Wait()
	close(errors)
	for updateErr := range errors {
		t.Fatal(updateErr)
	}

	if err = db.UpdatePaperMark("concurrent-mint", 90); err != nil {
		t.Fatal(err)
	}
	entry, current, peak, known, err := db.MarkMarketCaps("concurrent-mint", true)
	if err != nil || !known || entry != 100 || current != 90 || peak != 140 {
		t.Fatalf("marks = (%.0f, %.0f, %.0f, %v), err=%v; want (100, 90, 140, true)", entry, current, peak, known, err)
	}
}

func TestTradeAndTransactionAreAtomicAndRequireReconciliation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "trade-1", Mint: "mint", Side: "buy", Quantity: "1", Requested: .1, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err = db.QueryRow("SELECT COUNT(*) FROM transactions WHERE trade_id='trade-1'").Scan(&n); err != nil || n != 1 {
		t.Fatalf("transaction record missing: %d %v", n, err)
	}
	required, err := db.ReconciliationRequired()
	if err != nil || !required {
		t.Fatalf("expected reconciliation: %v %v", required, err)
	}
	if err = db.UpdateTradeState("trade-1", "confirmed", "sig", true); err != nil {
		t.Fatal(err)
	}
	required, err = db.ReconciliationRequired()
	if err != nil || required {
		t.Fatalf("unexpected reconciliation requirement: %v %v", required, err)
	}
	duplicate, err := db.HasPurchaseForMint("mint")
	if err != nil || !duplicate {
		t.Fatalf("expected duplicate purchase detection: %v %v", duplicate, err)
	}
}

func TestFailedBuyStillPermanentlyBlocksReentryForMint(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "failed-buy", Mint: "mint-once", Side: "buy", Quantity: "1", Requested: .01, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateTradeState("failed-buy", "failed", "", false); err != nil {
		t.Fatal(err)
	}
	blocked, err := db.HasPurchaseForMint("mint-once")
	if err != nil || !blocked {
		t.Fatalf("failed first attempt must block reentry: %v %v", blocked, err)
	}
}

func TestFailedSellRemainsVisibleForAutomaticRetry(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "failed-sell.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "failed-sell", Mint: "mint-to-close", Side: "sell", Quantity: "100", Requested: 1, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.MarkTradeFailed("failed-sell"); err != nil {
		t.Fatal(err)
	}

	failed, err := db.HasFailedSellForMint("mint-to-close")
	if err != nil || !failed {
		t.Fatalf("failed sell was not recorded for retry: failed=%v err=%v", failed, err)
	}
	failedAttempts, lastFailure, err := db.FailedSellRetryState("mint-to-close")
	if err != nil || failedAttempts != 1 || lastFailure.IsZero() {
		t.Fatalf("failed sell retry state = %d at %v, err=%v", failedAttempts, lastFailure, err)
	}
	attemptsInPeriod, failuresInPeriod, err := db.SellActivityStats(time.Now().UTC().Add(-time.Hour), time.Now().UTC().Add(time.Hour))
	if err != nil || attemptsInPeriod != 1 || failuresInPeriod != 1 {
		t.Fatalf("sell activity period = attempts:%d failures:%d err:%v", attemptsInPeriod, failuresInPeriod, err)
	}
	active, err := db.ActiveForMint("mint-to-close")
	if err != nil || active {
		t.Fatalf("confirmed failed sell should not remain active: active=%v err=%v", active, err)
	}
	reconciliationNeeded, err := db.ReconciliationRequired()
	if err != nil || reconciliationNeeded {
		t.Fatalf("definitively failed sell should not require reconciliation: needed=%v err=%v", reconciliationNeeded, err)
	}
}

func TestStaleSimulationSellCanBeReconciledWithoutClosingPosition(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stale-simulation-sell.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("stale-paper-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{
		ID: "stale-paper-sell", Mint: "stale-paper-mint", Side: "sell",
		Quantity: "0.04", Requested: 0.04, State: "prepared",
	}); err != nil {
		t.Fatal(err)
	}

	recentCutoff := time.Now().UTC().Add(-time.Minute)
	recovered, err := db.ReconcileStaleSimulationSells("stale-paper-mint", recentCutoff)
	if err != nil || recovered != 0 {
		t.Fatalf("recent paper sell should not be reconciled: recovered=%d err=%v", recovered, err)
	}

	oldTime := time.Now().UTC().Add(-2 * time.Minute)
	if _, err = db.Exec(`UPDATE trades SET updated_at=? WHERE id='stale-paper-sell'`, oldTime); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE transactions SET updated_at=? WHERE trade_id='stale-paper-sell'`, oldTime); err != nil {
		t.Fatal(err)
	}
	recovered, err = db.ReconcileStaleSimulationSells("stale-paper-mint", recentCutoff)
	if err != nil || recovered != 1 {
		t.Fatalf("stale paper sell was not reconciled: recovered=%d err=%v", recovered, err)
	}
	active, err := db.ActiveForMint("stale-paper-mint")
	if err != nil || active {
		t.Fatalf("reconciled paper sell should not remain active: active=%v err=%v", active, err)
	}
	if _, open, openErr := db.PositionOpenedAt("stale-paper-mint"); openErr != nil || !open {
		t.Fatalf("reconciliation must leave the position open: open=%v err=%v", open, openErr)
	}
	failedAttempts, _, err := db.FailedSellRetryState("stale-paper-mint")
	if err != nil || failedAttempts != 1 {
		t.Fatalf("reconciled failure retry count = %d, err=%v", failedAttempts, err)
	}
}

func TestOpenPnlAndPriceCoverageFollowSimulationMode(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "mode-specific-marks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("paper-marked", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("live-marked", 0.2); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("without-mark", 0.3); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("paper-marked", 100); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdatePaperMark("paper-marked", 150); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordLiveMark("live-marked", 200); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateLiveMark("live-marked", 100); err != nil {
		t.Fatal(err)
	}

	paperPnl, err := db.OpenPnlSOL(true)
	if err != nil || paperPnl < 0.049999999 || paperPnl > 0.050000001 {
		t.Fatalf("paper open P&L = %.9f, err=%v; want 0.05 SOL", paperPnl, err)
	}
	livePnl, err := db.OpenPnlSOL(false)
	if err != nil || livePnl < -0.100000001 || livePnl > -0.099999999 {
		t.Fatalf("live open P&L = %.9f, err=%v; want -0.1 SOL", livePnl, err)
	}
	paperUnmarked, err := db.UnmarkedOpenPositionCount(true)
	if err != nil || paperUnmarked != 2 {
		t.Fatalf("paper unmarked positions = %d, err=%v; want 2", paperUnmarked, err)
	}
	liveUnmarked, err := db.UnmarkedOpenPositionCount(false)
	if err != nil || liveUnmarked != 2 {
		t.Fatalf("live unmarked positions = %d, err=%v; want 2", liveUnmarked, err)
	}

	rows, err := db.ListPositionsForMode(false)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	foundLiveMark := false
	for rows.Next() {
		var mint, quantity, status, opened, updated, markUpdated string
		var cost, entry, current float64
		var realized sql.NullFloat64
		if err = rows.Scan(&mint, &quantity, &cost, &status, &opened, &updated, &entry, &current, &markUpdated, &realized); err != nil {
			t.Fatal(err)
		}
		if mint == "live-marked" {
			foundLiveMark = entry == 200 && current == 100 && markUpdated != ""
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !foundLiveMark {
		t.Fatal("real-mode position query did not return its live price mark")
	}
}

func TestPositionPeakMarkSurvivesRetracement(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "position-peak.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("peak-mint", 100); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdatePaperMark("peak-mint", 160); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdatePaperMark("peak-mint", 130); err != nil {
		t.Fatal(err)
	}
	peakReturn, known, err := db.PaperPeakReturnPct("peak-mint")
	if err != nil || !known || peakReturn != 60 {
		t.Fatalf("peak return = %.2f, known=%v, err=%v; want 60%%", peakReturn, known, err)
	}
	currentReturn, known, err := db.PaperReturnPct("peak-mint")
	if err != nil || !known || currentReturn != 30 {
		t.Fatalf("current return = %.2f, known=%v, err=%v; want 30%%", currentReturn, known, err)
	}
}

func TestPerformanceUsesClosedPositionResults(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "performance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "buy", Mint: "performance-mint", Side: "buy", Quantity: "0.1", Requested: 0.1, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationBuy("buy", "performance-mint", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "sell", Mint: "performance-mint", Side: "sell", Quantity: "0.15", Requested: 0.15, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationSell("sell", "performance-mint", 0.15); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{
		ID: "failed-sell-attempt", Mint: "performance-mint", Side: "sell",
		Quantity: "0.12", Requested: 0.12, Fees: 0.01, State: "prepared",
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.MarkTradeFailed("failed-sell-attempt"); err != nil {
		t.Fatal(err)
	}

	stats, err := db.Performance()
	if err != nil {
		t.Fatal(err)
	}
	if stats.SellAttempts != 2 || stats.FailedSellAttempts != 1 || stats.ClosedTrades != 1 ||
		stats.Winners != 1 || stats.Losers != 0 || stats.WinRatePct != 100 ||
		stats.NetRealizedPnlSOL < 0.049999999 || stats.NetRealizedPnlSOL > 0.050000001 {
		t.Fatalf("unexpected performance stats: %+v", stats)
	}
}

func TestUnknownTradesIncludesOnlyResolvableSignatures(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "unknown-trades.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	for _, trade := range []Trade{
		{ID: "unknown-with-signature", Mint: "mint-a", Side: "sell", Quantity: "100", Requested: 1, State: "prepared"},
		{ID: "unknown-without-signature", Mint: "mint-b", Side: "sell", Quantity: "100", Requested: 1, State: "prepared"},
	} {
		if err = db.InsertTrade(trade); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.UpdateTradeState("unknown-with-signature", "unknown", "signature-a", false); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateTradeState("unknown-without-signature", "unknown", "", false); err != nil {
		t.Fatal(err)
	}

	rows, err := db.UnknownTrades()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, signature, mint, side string
		if err = rows.Scan(&id, &signature, &mint, &side); err != nil {
			t.Fatal(err)
		}
		count++
		if id != "unknown-with-signature" || signature != "signature-a" || mint != "mint-a" || side != "sell" {
			t.Fatalf("unexpected unknown trade selected: id=%q signature=%q mint=%q side=%q", id, signature, mint, side)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("unknown trade query returned %d rows, want 1", count)
	}
}

func TestSimulationFinalizationUpdatesAvailableBankroll(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "simulation-bankroll.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	if err = db.OpenSimulationPosition("winning-mint", 0.10); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "sim-sell", Mint: "winning-mint", Side: "sell", Quantity: "0.12", Requested: 0.12, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	var pendingPnl sql.NullFloat64
	if err = db.QueryRow(`SELECT realized_pnl_sol FROM trades WHERE id='sim-sell'`).Scan(&pendingPnl); err != nil {
		t.Fatal(err)
	}
	if !pendingPnl.Valid || pendingPnl.Float64 != 0 {
		t.Fatalf("prepared simulated sell P&L = %+v; want non-NULL 0", pendingPnl)
	}
	if _, err = db.Exec(`UPDATE trades SET exit_reason='' WHERE id='sim-sell'`); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationSell("sim-sell", "winning-mint", 0.12); err != nil {
		t.Fatal(err)
	}

	if err = db.OpenSimulationPosition("open-mint", 0.40); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "sim-buy", Mint: "new-mint", Side: "buy", Quantity: "0.20", Requested: 0.20, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationBuy("sim-buy", "new-mint", 0.20); err != nil {
		t.Fatal(err)
	}

	available, err := db.SimulationAvailableBalanceSOL(1)
	if err != nil {
		t.Fatal(err)
	}
	// Initial capital + 0.02 realized gain - 0.40 and 0.20 open exposure.
	if diff := available - 0.42; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("available simulation balance = %.9f SOL, want 0.42 SOL", available)
	}

	var state, exitReason string
	var fees, pnl float64
	if err = db.QueryRow(`SELECT state,fees_sol,realized_pnl_sol,exit_reason FROM trades WHERE id='sim-sell'`).Scan(&state, &fees, &pnl, &exitReason); err != nil {
		t.Fatal(err)
	}
	if state != "simulated" || fees != 0 || pnl < 0.019999999 || pnl > 0.020000001 || exitReason != "unspecified exit" {
		t.Fatalf("simulated exit not finalized with non-NULL financial fields: state=%q fees=%.9f pnl=%.9f exitReason=%q", state, fees, pnl, exitReason)
	}
}

func TestLiveTradeFinalizationCommitsExecutionAndPositionAtomically(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "live-finalization.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	if err = db.InsertTrade(Trade{ID: "live-buy", Mint: "live-mint", Side: "buy", Quantity: "0.05", Requested: 0.05, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeLiveBuy("live-buy", "live-mint", "confirmed", "buy-signature", 0.05, 0.001); err != nil {
		t.Fatal(err)
	}
	open, err := db.OpenPositionCount()
	if err != nil || open != 1 {
		t.Fatalf("confirmed buy position missing: open=%d err=%v", open, err)
	}

	if err = db.InsertTrade(Trade{ID: "live-sell", Mint: "live-mint", Side: "sell", Quantity: "100", Requested: 100, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeLiveSell("live-sell", "live-mint", "finalized", "sell-signature", 0.075, 0.002); err != nil {
		t.Fatal(err)
	}
	open, err = db.OpenPositionCount()
	if err != nil || open != 0 {
		t.Fatalf("confirmed sell position remains open: open=%d err=%v", open, err)
	}

	var state, signature string
	var executed, fees, pnl float64
	if err = db.QueryRow(`SELECT state,signature,executed_value_sol,fees_sol,realized_pnl_sol FROM trades WHERE id='live-sell'`).Scan(&state, &signature, &executed, &fees, &pnl); err != nil {
		t.Fatal(err)
	}
	if state != "finalized" || signature != "sell-signature" || executed != 0.075 || fees != 0.002 || pnl < 0.024999999 || pnl > 0.025000001 {
		t.Fatalf("live sell ledger is inconsistent: state=%q signature=%q executed=%.9f fees=%.9f pnl=%.9f", state, signature, executed, fees, pnl)
	}
	if needed, err := db.ReconciliationRequired(); err != nil || needed {
		t.Fatalf("finalized live trades should not remain unreconciled: needed=%v err=%v", needed, err)
	}
}
