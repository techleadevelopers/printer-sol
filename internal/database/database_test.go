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
	if err != nil { t.Fatal(err) }
	defer db.Close()
	if err = db.Migrate(); err != nil { t.Fatal(err) }
	if err = db.InsertTrade(Trade{ID: "failed-buy", Mint: "mint-once", Side: "buy", Quantity: "1", Requested: .01, State: "prepared"}); err != nil { t.Fatal(err) }
	if err = db.UpdateTradeState("failed-buy", "failed", "", false); err != nil { t.Fatal(err) }
	blocked, err := db.HasPurchaseForMint("mint-once")
	if err != nil || !blocked { t.Fatalf("failed first attempt must block reentry: %v %v", blocked, err) }
}
