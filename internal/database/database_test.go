package database

import (
	"path/filepath"
	"testing"
)

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

	var state string
	var pnl float64
	if err = db.QueryRow(`SELECT state,realized_pnl_sol FROM trades WHERE id='sim-sell'`).Scan(&state, &pnl); err != nil {
		t.Fatal(err)
	}
	if state != "simulated" || pnl < 0.019999999 || pnl > 0.020000001 {
		t.Fatalf("simulated exit not finalized atomically: state=%q pnl=%.9f", state, pnl)
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
