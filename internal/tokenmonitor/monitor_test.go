package tokenmonitor

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/execution"
)

func TestSimulationMarksTriggerVirtualTakeProfitAndStopLoss(t *testing.T) {
	tests := []struct {
		name          string
		takeProfitPct float64
		stopLossPct   float64
		currentCap    float64
		wantPnL       float64
	}{
		{name: "take profit", takeProfitPct: 70, stopLossPct: 30, currentCap: 170, wantPnL: 0.035},
		{name: "stop loss", takeProfitPct: 70, stopLossPct: 30, currentCap: 70, wantPnL: -0.015},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := database.Open(filepath.Join(t.TempDir(), "paper-exit.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Migrate(); err != nil {
				t.Fatal(err)
			}
			if err = db.OpenSimulationPosition("paper-mint", 0.05); err != nil {
				t.Fatal(err)
			}
			if err = db.RecordPaperMark("paper-mint", 100); err != nil {
				t.Fatal(err)
			}

			bus := events.New()
			engine := execution.New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, bus, execution.Limits{})
			monitor := New("", "", 8, db, nil, engine, 0, 0, 0, test.takeProfitPct, test.stopLossPct, 0, true, bus, zap.NewNop())
			monitor.markTrade(context.Background(), "paper-mint", test.currentCap)

			open, err := db.OpenPositionCount()
			if err != nil {
				t.Fatal(err)
			}
			if open != 0 {
				t.Fatalf("virtual %s left %d positions open", test.name, open)
			}
			var side, state string
			var pnl float64
			if err = db.QueryRow(`SELECT side,state,realized_pnl_sol FROM trades WHERE side='sell'`).Scan(&side, &state, &pnl); err != nil {
				t.Fatalf("virtual %s did not record a sell: %v", test.name, err)
			}
			if side != "sell" || state != "simulated" {
				t.Fatalf("unexpected virtual exit: side=%q state=%q", side, state)
			}
			if diff := pnl - test.wantPnL; diff < -1e-9 || diff > 1e-9 {
				t.Fatalf("virtual %s P&L = %.9f SOL, want %.9f SOL", test.name, pnl, test.wantPnL)
			}
			available, err := db.SimulationAvailableBalanceSOL(1)
			if err != nil {
				t.Fatal(err)
			}
			if diff := available - (1 + test.wantPnL); diff < -1e-9 || diff > 1e-9 {
				t.Fatalf("bankroll after virtual %s = %.9f SOL, want %.9f SOL", test.name, available, 1+test.wantPnL)
			}
		})
	}
}

func TestSimulationExitUsesConfiguredMaxHold(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "paper-max-hold.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("paper-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("paper-mint", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-time.Hour), "paper-mint"); err != nil {
		t.Fatal(err)
	}

	bus := events.New()
	engine := execution.New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, bus, execution.Limits{})
	monitor := New("", "", 8, db, nil, engine, 0, 0, 0, 70, 30, time.Minute, true, bus, zap.NewNop())
	monitor.markTrade(context.Background(), "paper-mint", 100)

	open, err := db.OpenPositionCount()
	if err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatal("simulation max-hold exit left the position open")
	}
}
