package execution

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gagliardetto/solana-go"
	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/wallet"
)

func TestSimulationReserveUsesRealizedResultsAndOpenCapital(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "simulation-reserve.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("open-mint", 0.40); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("closed-mint", 0.10); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(database.Trade{ID: "closed-sell", Mint: "closed-mint", Side: "sell", Quantity: "0.12", Requested: 0.12, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationSell("closed-sell", "closed-mint", 0.12); err != nil {
		t.Fatal(err)
	}

	engine := New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, nil, Limits{FeeReserveSOL: 0.01})
	tooMuch, err := wallet.SOLToLamports(0.62)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.reserve(context.Background(), "too-much", tooMuch); err == nil || !strings.Contains(err.Error(), "insufficient free wallet balance") {
		t.Fatalf("reserve should account for open capital, gain, and fee reserve; got %v", err)
	}

	withinBalance, err := wallet.SOLToLamports(0.60)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.reserve(context.Background(), "within-balance", withinBalance); err != nil {
		t.Fatalf("reserve within the remaining virtual bankroll: %v", err)
	}
	engine.release("within-balance")
}

func TestSimulationChargesPriorityAndBaseFeePerTransaction(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "simulation-fees.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	engine := New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, nil, Limits{
		MaxPriorityFeeSOL: .001, SimulationBaseFeeSOL: .000005,
	})
	if _, err = engine.Execute(context.Background(), Request{
		Mint: "fee-mint", Side: "buy", Amount: .05, DenominatedInSOL: true, PriorityFee: .001,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.Execute(context.Background(), Request{
		Mint: "fee-mint", Side: "sell", Amount: .05, DenominatedInSOL: true, PriorityFee: .001,
	}); err != nil {
		t.Fatal(err)
	}

	var fees float64
	if err = db.QueryRow(`SELECT SUM(fees_sol) FROM trades WHERE state='simulated'`).Scan(&fees); err != nil {
		t.Fatal(err)
	}
	if fees < .002009999 || fees > .002010001 {
		t.Fatalf("round-trip simulated fees = %.9f SOL, want 0.00201 SOL", fees)
	}
}

func TestFailedSimulationSellClosesPositionWithoutChargingIt(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "failed-simulation-sell.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("failed-paper-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TRIGGER reject_simulated_sell
		BEFORE UPDATE OF state ON trades
		WHEN NEW.side='sell' AND NEW.state='simulated'
		BEGIN SELECT RAISE(FAIL, 'forced simulated sell failure'); END`); err != nil {
		t.Fatal(err)
	}

	engine := New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, nil, Limits{
		SimulationBaseFeeSOL: 0.001,
	})
	id, err := engine.Execute(context.Background(), Request{
		Mint: "failed-paper-mint", Side: "sell", Amount: 0.04, DenominatedInSOL: true,
		Reason: "stop loss reached in simulation",
	})
	if err == nil || !strings.Contains(err.Error(), "forced simulated sell failure") {
		t.Fatalf("expected the injected finalization error, got trade=%q err=%v", id, err)
	}
	if id == "" {
		t.Fatal("failed finalization did not retain its trade id")
	}
	if _, open, openErr := db.PositionOpenedAt("failed-paper-mint"); openErr != nil || open {
		t.Fatalf("failed simulated sell should close the position and release its slot: open=%v err=%v", open, openErr)
	}

	var state string
	if err = db.QueryRow(`SELECT state FROM trades WHERE id=?`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "failed" {
		t.Fatalf("failed simulated sell state = %q; want failed", state)
	}
	var transactionState string
	if err = db.QueryRow(`SELECT state FROM transactions WHERE trade_id=?`, id).Scan(&transactionState); err != nil {
		t.Fatal(err)
	}
	if transactionState != "failed" {
		t.Fatalf("failed simulated sell transaction state = %q; want failed", transactionState)
	}
	var realizedPnl sql.NullFloat64
	if err = db.QueryRow(`SELECT realized_pnl_sol FROM trades WHERE id=?`, id).Scan(&realizedPnl); err != nil {
		t.Fatal(err)
	}
	if realizedPnl.Valid {
		t.Fatalf("failed simulated sell realized P&L = %+v; want NULL", realizedPnl)
	}
	stats, err := db.Performance(true)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SellAttempts != 1 || stats.FailedSellAttempts != 1 || stats.ClosedTrades != 0 ||
		stats.FeesSOL != 0 || stats.GrossRealizedPnlSOL != 0 || stats.NetRealizedPnlSOL != 0 {
		t.Fatalf("failed sell contaminated performance: %+v", stats)
	}
	balance, err := db.SimulationAvailableBalanceSOL(1)
	if err != nil {
		t.Fatal(err)
	}
	if balance < 0.949999999 || balance > 0.950000001 {
		t.Fatalf("failed sell changed virtual balance to %.9f SOL; want 0.95 SOL", balance)
	}
}

func TestValidateSOLQuoteMint(t *testing.T) {
	tests := []struct {
		name      string
		quoteMint solana.PublicKey
		wantErr   bool
	}{
		{
			name:      "SOL quote is allowed",
			quoteMint: solana.PublicKey{},
			wantErr:   false,
		},
		{
			name:      "non-SOL quote is blocked",
			quoteMint: solana.PublicKey{1},
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSOLQuoteMint(tt.quoteMint)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected non-SOL quote mint to be rejected")
				}
				if !strings.Contains(err.Error(), "unsupported non-SOL quote mint") {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("SOL quote mint should be accepted: %v", err)
			}
		})
	}
}

func TestValidateDirectPumpRouteRejectsUnknownRouter(t *testing.T) {
	unknown := solana.MustPublicKeyFromBase58("FAdo9NCw1ssek6Z6yeWzWjhLVsr8uiCwcWNUnKgzTnHe")
	tx := &solana.Transaction{}
	tx.Message.AccountKeys = solana.PublicKeySlice{unknown}
	tx.Message.Instructions = []solana.CompiledInstruction{{ProgramIDIndex: 0, Data: []byte{1}}}
	if err := validateDirectPumpRoute(tx); err == nil || !strings.Contains(err.Error(), "unapproved program") {
		t.Fatalf("unapproved router was accepted: %v", err)
	}
}
