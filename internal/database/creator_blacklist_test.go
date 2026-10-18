package database

import (
	"path/filepath"
	"testing"
	"time"
)

func TestClosedStopLossBlacklistsCreatorFor24Hours(t *testing.T) {
	tests := []struct {
		name        string
		reason      string
		finalize    func(*DB, string) error
		wantBlocked bool
	}{
		{
			name:   "paper stop loss",
			reason: "stop loss",
			finalize: func(db *DB, id string) error {
				return db.FinalizeSimulationSell(id, "paper-stop-mint", 0.075)
			},
			wantBlocked: true,
		},
		{
			name:   "live stop-loss",
			reason: "stop-loss",
			finalize: func(db *DB, id string) error {
				return db.FinalizeLiveSell(id, "live-stop-mint", "confirmed", "signature", 0.075, 0.001)
			},
			wantBlocked: true,
		},
		{
			name:   "take profit",
			reason: "take profit",
			finalize: func(db *DB, id string) error {
				return db.FinalizeSimulationSell(id, "take-profit-mint", 0.12)
			},
			wantBlocked: false,
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "creator-blacklist.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Migrate(); err != nil {
				t.Fatal(err)
			}

			mints := []string{"paper-stop-mint", "live-stop-mint", "take-profit-mint"}
			mint := mints[i]
			creator := "creator-" + mint
			if err = db.RecordEvent(mint, creator, "{}", time.Now().UTC(), 1); err != nil {
				t.Fatal(err)
			}
			if err = db.OpenPosition(mint, 0.1); err != nil {
				t.Fatal(err)
			}
			id := "sell-" + mint
			if err = db.InsertTrade(Trade{
				ID: id, Mint: mint, Side: "sell", Quantity: "100", Requested: 0.1,
				State: "prepared", ExitReason: test.reason,
			}); err != nil {
				t.Fatal(err)
			}
			if err = test.finalize(db, id); err != nil {
				t.Fatal(err)
			}

			blocked, err := db.IsCreatorBlacklisted(creator)
			if err != nil || blocked != test.wantBlocked {
				t.Fatalf("blacklisted = %v, err=%v; want %v", blocked, err, test.wantBlocked)
			}
			if !test.wantBlocked {
				return
			}

			var blockedAt string
			if err = db.QueryRow(`SELECT blocked_at FROM creator_blacklist WHERE creator=?`, creator).Scan(&blockedAt); err != nil {
				t.Fatal(err)
			}
			blockedAtTime, err := time.Parse(time.RFC3339Nano, blockedAt)
			if err != nil {
				t.Fatalf("parse stored block timestamp %q: %v", blockedAt, err)
			}
			age := time.Since(blockedAtTime)
			if age < 0 || age > time.Second {
				t.Fatalf("creator block timestamp age = %s; want approximately zero", age)
			}
			if _, err = db.Exec(`UPDATE creator_blacklist SET blocked_at=? WHERE creator=?`,
				time.Now().UTC().Add(-25*time.Hour).Format(time.RFC3339Nano), creator); err != nil {
				t.Fatal(err)
			}
			blocked, err = db.IsCreatorBlacklisted(creator)
			if err != nil || blocked {
				t.Fatalf("expired creator block = %v, err=%v; want false", blocked, err)
			}
		})
	}
}
