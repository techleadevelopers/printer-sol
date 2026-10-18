package strategy

import (
	"context"
	"fmt"
	"solana-sniper-backend/internal/database"
	"time"
)

// Strategy admits only fresh, non-duplicated PumpPortal creation events. Capital
// and operational limits remain enforced by the execution engine immediately
// before an entry is created.
type Strategy struct {
	db     *database.DB
	maxAge time.Duration
	screen func(context.Context, string) (string, error)
}

func (s *Strategy) SetScreen(screen func(context.Context, string) (string, error)) { s.screen = screen }

func New(db *database.DB, maxAge time.Duration) *Strategy {
	return &Strategy{db: db, maxAge: maxAge}
}
func (s *Strategy) Evaluate(ctx context.Context, mint string, receivedAt time.Time) (bool, string) {
	if mint == "" {
		return false, "missing mint"
	}
	if s.maxAge > 0 && time.Since(receivedAt) > s.maxAge {
		return false, "token creation exceeded maximum entry age"
	}
	active, e := s.db.ActiveForMint(mint)
	if e != nil {
		return false, "database unavailable"
	}
	if active {
		return false, "existing unreconciled position"
	}
	if s.screen != nil {
		reason, err := s.screen(ctx, mint)
		if err != nil {
			return false, err.Error()
		}
		return true, reason
	}
	return true, "fresh PumpPortal token accepted by entry filters"
}
func (s *Strategy) String() string { return fmt.Sprintf("maximum event age %s", s.maxAge) }
