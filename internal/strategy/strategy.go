package strategy

import (
	"context"
	"fmt"
	"solana-sniper-backend/internal/database"
	"time"
)

// Strategy deliberately does not infer liquidity or exitability from a creation event.
type Strategy struct {
	db                     *database.DB
	maxAge time.Duration
}

func New(db *database.DB, maxAge time.Duration) *Strategy {
	return &Strategy{db: db, maxAge: maxAge}
}
func (s *Strategy) Evaluate(ctx context.Context, mint string, receivedAt time.Time) (bool, string) {
	if mint == "" {
		return false, "missing mint"
	}
	if s.maxAge > 0 && time.Since(receivedAt) > s.maxAge {
		return false, "token event exceeded maximum entry age"
	}
	active, e := s.db.ActiveForMint(mint)
	if e != nil {
		return false, "database unavailable"
	}
	if active {
		return false, "existing unreconciled position"
	}
	return true, "fresh PumpPortal token accepted by entry filters"
}
func (s *Strategy) String() string { return fmt.Sprintf("maximum event age %s", s.maxAge) }
