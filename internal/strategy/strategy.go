package strategy

import (
	"context"
	"fmt"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/wallet"
)

// Strategy deliberately does not infer liquidity or exitability from a creation event.
type Strategy struct {
	db                     *database.DB
	wallet                 *wallet.Wallet
	maxTrade, maxDailyLoss float64
}

func New(db *database.DB, w *wallet.Wallet, max, maxLoss float64) *Strategy {
	return &Strategy{db: db, wallet: w, maxTrade: max, maxDailyLoss: maxLoss}
}
func (s *Strategy) Evaluate(ctx context.Context, mint string) (bool, string) {
	if mint == "" {
		return false, "missing mint"
	}
	active, e := s.db.ActiveForMint(mint)
	if e != nil {
		return false, "database unavailable"
	}
	if active {
		return false, "existing unreconciled position"
	}
	return false, "simulation/observation only: creation event lacks verified liquidity, market and exit data"
}
func (s *Strategy) String() string { return fmt.Sprintf("max trade %.9f SOL", s.maxTrade) }
