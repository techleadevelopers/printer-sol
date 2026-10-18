package database

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLiveTradesReportUsesConfirmedWalletDeltasAndBothFees(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "live-report.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	if err = db.InsertTrade(Trade{ID: "live-buy", Mint: "live-report-mint", Side: "buy", Quantity: "0.05", Requested: 0.05, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeLiveBuy("live-buy", "live-report-mint", "confirmed", "buy-signature", 0.05, 0.001); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "live-sell", Mint: "live-report-mint", Side: "sell", Quantity: "100", Requested: 100, ExitReason: "trailing stop", State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeLiveSell("live-sell", "live-report-mint", "finalized", "sell-signature", 0.075, 0.002); err != nil {
		t.Fatal(err)
	}

	to := time.Now().UTC().Add(time.Second)
	trades, err := db.LiveTrades(time.Time{}, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 {
		t.Fatalf("live report returned %d trades, want 1", len(trades))
	}
	trade := trades[0]
	if trade.Side != "round_trip" || trade.EntrySOL != 0.05 || trade.ExitSOL != 0.075 ||
		trade.FeesSOL < 0.002999 || trade.FeesSOL > 0.003001 ||
		trade.GrossPnLSOL < 0.027999 || trade.GrossPnLSOL > 0.028001 ||
		trade.NetPnLSOL < 0.024999 || trade.NetPnLSOL > 0.025001 ||
		trade.ExitReason != "trailing stop" {
		t.Fatalf("unexpected live report row: %+v", trade)
	}

	fees, err := db.LiveFees(time.Time{}, to)
	if err != nil || fees < 0.002999 || fees > 0.003001 {
		t.Fatalf("confirmed live fees = %.9f, err=%v; want both transaction fees", fees, err)
	}
}
