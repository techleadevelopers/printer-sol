package execution

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

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
