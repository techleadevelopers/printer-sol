package database

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSimulationSessionFeesAndNetResultsPersist(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "simulation-report.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	run, err := db.EnsureSimulationRun(1.25)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := db.EnsureSimulationRun(9)
	if err != nil || !restarted.StartedAt.Equal(run.StartedAt) || restarted.BalanceStartSOL != 1.25 {
		t.Fatalf("simulation session changed on restart: %+v err=%v", restarted, err)
	}

	if err = db.InsertTrade(Trade{ID: "paper-buy", Mint: "paper-mint", Side: "buy", Quantity: "0.1", Requested: 0.1, Fees: 0.002, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationBuy("paper-buy", "paper-mint", 0.1, 0.002); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "paper-sell", Mint: "paper-mint", Side: "sell", Quantity: "0.15", Requested: 0.15, Fees: 0.003, ExitReason: "take profit", State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationSell("paper-sell", "paper-mint", 0.15, 0.003); err != nil {
		t.Fatal(err)
	}

	to := time.Now().UTC().Add(time.Second)
	trades, err := db.SimulationTrades(run.StartedAt, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(trades) != 1 || trades[0].EntrySOL != 0.1 || trades[0].ExitSOL != 0.15 || trades[0].GrossPnLSOL < 0.049999 || trades[0].FeesSOL < 0.004999 || trades[0].NetPnLSOL < 0.044999 || trades[0].ExitReason != "take profit" {
		t.Fatalf("unexpected paper trade record: %+v", trades)
	}
	fees, err := db.SimulationFees(run.StartedAt, to)
	if err != nil || fees < 0.004999 || fees > 0.005001 {
		t.Fatalf("simulation fees = %.9f err=%v", fees, err)
	}
	balance, err := db.SimulationAvailableBalanceSOL(run.BalanceStartSOL)
	if err != nil || balance < 1.294999 || balance > 1.295001 {
		t.Fatalf("paper balance = %.9f err=%v, want start + gross - fees", balance, err)
	}
	stats, err := db.Performance(true)
	if err != nil || stats.NetRealizedPnlSOL < 0.044999 || stats.NetRealizedPnlSOL > 0.045001 {
		t.Fatalf("net paper performance = %+v err=%v", stats, err)
	}
}

func TestResetSimulationClearsPaperDataAndStartsFreshSession(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "simulation-reset.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.EnsureSimulationRun(1.25); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "reset-trade", Mint: "reset-mint", Side: "buy", Quantity: "0.1", Requested: 0.1, State: "failed"}); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("reset-mint", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("reset-mint", 5); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordEvent("reset-mint", "creator", "{}", time.Now().UTC(), 10); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordAssessment("reset-mint", false, "test"); err != nil {
		t.Fatal(err)
	}

	deletedTrades, deletedPositions, err := db.ResetSimulation(context.Background(), 2.5)
	if err != nil || deletedTrades != 1 || deletedPositions != 1 {
		t.Fatalf("reset counts trades=%d positions=%d err=%v", deletedTrades, deletedPositions, err)
	}
	for _, table := range []string{"transactions", "trades", "positions", "paper_marks", "token_events", "risk_assessments"} {
		var count int
		if err = db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d err=%v after reset", table, count, err)
		}
	}
	run, err := db.SimulationRun()
	if err != nil || run.BalanceStartSOL != 2.5 || run.State != "running" {
		t.Fatalf("reset session = %+v err=%v", run, err)
	}
	var resetLogs int
	if err = db.QueryRow(`SELECT COUNT(*) FROM operational_logs WHERE event_type='simulation_reset'`).Scan(&resetLogs); err != nil || resetLogs != 1 {
		t.Fatalf("simulation reset audit logs=%d err=%v", resetLogs, err)
	}
}

func TestResetSimulationRejectsUnresolvedTradesWithoutDeletingData(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "simulation-reset-pending.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.EnsureSimulationRun(1); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(Trade{ID: "unresolved-trade", Mint: "pending-mint", Side: "buy", Quantity: "0.1", Requested: 0.1, State: "unknown"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err = db.ResetSimulation(context.Background(), 3); !errors.Is(err, ErrSimulationResetPending) {
		t.Fatalf("reset error=%v, want unresolved trade guard", err)
	}
	var remaining int
	if err = db.QueryRow(`SELECT COUNT(*) FROM trades WHERE id='unresolved-trade'`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("unresolved trade count=%d err=%v after rejected reset", remaining, err)
	}
	run, err := db.SimulationRun()
	if err != nil || run.BalanceStartSOL != 1 {
		t.Fatalf("session changed after rejected reset: %+v err=%v", run, err)
	}
}

func TestLiveUnlockRejectsFewerThanThirtyClosedPaperTrades(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "unlock.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.EnsureSimulationRun(1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 29; i++ {
		mint := fmt.Sprintf("mint-%d", i)
		buyID, sellID := fmt.Sprintf("buy-%d", i), fmt.Sprintf("sell-%d", i)
		if err = db.InsertTrade(Trade{ID: buyID, Mint: mint, Side: "buy", Quantity: "0.1", Requested: 0.1, State: "prepared"}); err != nil {
			t.Fatal(err)
		}
		if err = db.FinalizeSimulationBuy(buyID, mint, 0.1); err != nil {
			t.Fatal(err)
		}
		if err = db.InsertTrade(Trade{ID: sellID, Mint: mint, Side: "sell", Quantity: "0.12", Requested: 0.12, ExitReason: "take profit", State: "prepared"}); err != nil {
			t.Fatal(err)
		}
		if err = db.FinalizeSimulationSell(sellID, mint, 0.12); err != nil {
			t.Fatal(err)
		}
	}
	summary, err := db.RequireLiveUnlock()
	if err == nil || !strings.Contains(err.Error(), "at least 30") || summary.TotalTrades != 29 {
		t.Fatalf("29 paper trades must not unlock live: summary=%+v err=%v", summary, err)
	}
}
