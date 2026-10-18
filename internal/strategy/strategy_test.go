package strategy

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"solana-sniper-backend/internal/database"
)

func TestNewTokenIsObservationOnly(t *testing.T) {
	s := &Strategy{}
	ok, reason := s.Evaluate(nil, "", time.Now())
	if ok || reason != "missing mint" {
		t.Fatal("empty mint must be rejected")
	}
}

func TestEvaluateRejectsMissingStaleAndFutureCreationTimes(t *testing.T) {
	s := New(nil, 5*time.Minute)
	tests := []struct {
		name      string
		createdAt time.Time
		reason    string
	}{
		{name: "missing", reason: "missing token creation timestamp"},
		{name: "older than configured maximum", createdAt: time.Now().Add(-5*time.Minute - time.Second), reason: "token creation exceeded maximum entry age"},
		{name: "implausibly in the future", createdAt: time.Now().Add(time.Minute), reason: "token creation timestamp is in the future"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, reason := s.Evaluate(context.Background(), "mint", test.createdAt)
			if ok || reason != test.reason {
				t.Fatalf("Evaluate() = %v, %q; want false, %q", ok, reason, test.reason)
			}
		})
	}
}

func TestEvaluateRejectsMintWithAnyPersistedBuyAttempt(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "strategy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(database.Trade{
		ID: "failed-first-buy", Mint: "already-traded", Side: "buy",
		Quantity: "1", Requested: 0.01, State: "failed",
	}); err != nil {
		t.Fatal(err)
	}

	ok, reason := New(db, 5*time.Minute).Evaluate(context.Background(), "already-traded", time.Now().UTC())
	if ok || reason != "mint already has a recorded buy attempt" {
		t.Fatalf("Evaluate() = %v, %q; want prior buy attempt rejected", ok, reason)
	}
}

func TestEvaluateRejectsTokenAtFiveMinuteAgeBoundary(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "strategy-age.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	createdAt := time.Now().UTC().Add(-5 * time.Minute)
	s := New(db, 10*time.Minute)
	ok, reason := s.Evaluate(context.Background(), "five-minute-token", createdAt)
	if ok || reason != "token creation exceeded maximum entry age" {
		t.Fatalf("Evaluate() = %v, %q; want token at 5-minute boundary rejected", ok, reason)
	}

	ok, reason = s.Evaluate(context.Background(), "fresh-token", time.Now().UTC().Add(-4*time.Minute-59*time.Second))
	if !ok {
		t.Fatalf("token younger than five minutes was rejected: %q", reason)
	}
}
