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
	const maximumEntryAge = 5 * time.Minute
	if maxAge <= 0 || maxAge > maximumEntryAge {
		maxAge = maximumEntryAge
	}
	return &Strategy{db: db, maxAge: maxAge}
}
func (s *Strategy) EvaluateFast(mint string, createdAt time.Time) (bool, string) {
	if mint == "" {
		return false, "missing mint"
	}
	if createdAt.IsZero() {
		return false, "missing token creation timestamp"
	}
	age := time.Since(createdAt)
	if age < -30*time.Second {
		return false, "token creation timestamp is in the future"
	}
	if s.maxAge > 0 && age >= s.maxAge {
		return false, "token creation exceeded maximum entry age"
	}
	active, e := s.db.ActiveForMint(mint)
	if e != nil {
		return false, "database unavailable"
	}
	if active {
		return false, "existing unreconciled position"
	}
	purchased, e := s.db.HasPurchaseForMint(mint)
	if e != nil {
		return false, "database unavailable"
	}
	if purchased {
		return false, "mint already has a recorded buy attempt"
	}
	return true, "fresh PumpPortal token accepted by entry filters"
}

func (s *Strategy) Screen(ctx context.Context, mint string) (string, error) {
	if s.screen == nil {
		return "no screen configured", nil
	}
	return s.screen(ctx, mint)
}

func (s *Strategy) Evaluate(ctx context.Context, mint string, createdAt time.Time) (bool, string) {
	ok, reason := s.EvaluateFast(mint, createdAt)
	if !ok {
		return false, reason
	}
	if s.screen != nil {
		screenReason, err := s.Screen(ctx, mint)
		if err != nil {
			return false, err.Error()
		}
		return true, screenReason
	}
	return true, reason
}
func (s *Strategy) String() string { return fmt.Sprintf("maximum event age %s", s.maxAge) }
