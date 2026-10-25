package database

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"math"
	_ "github.com/jackc/pgx/v5/stdlib"
	"io/fs"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed migrations/*.sql migrations/postgres/*.sql
var migrations embed.FS

type SellConfirmer interface {
	ConfirmSellState(ctx context.Context, signature string) (string, error)
}

type DB struct {
	*sql.DB
	postgres      bool
	markMu        sync.Mutex
	sellConfirmer SellConfirmer
}

func (d *DB) SetSellConfirmer(c SellConfirmer) {
	d.markMu.Lock()
	d.sellConfirmer = c
	d.markMu.Unlock()
}

func (d *DB) sellConfirmation(signature string) (string, error) {
	d.markMu.Lock()
	c := d.sellConfirmer
	d.markMu.Unlock()
	if c == nil {
		return "", fmt.Errorf("live sell confirmation is not configured")
	}
	if strings.TrimSpace(signature) == "" {
		return "unknown", nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return c.ConfirmSellState(ctx, signature)
}

func Open(target string) (*DB, error) {
	postgres := strings.HasPrefix(target, "postgres://") || strings.HasPrefix(target, "postgresql://")
	if !postgres {
		if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil && filepath.Dir(target) != "." {
			return nil, err
		}
	}
	driver := "sqlite"
	if postgres {
		driver = "pgx"
	}
	d, e := sql.Open(driver, target)
	if e != nil {
		return nil, e
	}
	if !postgres {
		d.SetMaxOpenConns(1)
	}
	return &DB{DB: d, postgres: postgres}, d.Ping()
}

func (d *DB) Migrate() error {
	pattern := "migrations/*.sql"
	if d.postgres {
		pattern = "migrations/postgres/*.sql"
	}
	files, e := fs.Glob(migrations, pattern)
	if e != nil {
		return e
	}
	for _, f := range files {
		var n int
		_, e = fmt.Sscanf(filepath.Base(f), "%d_", &n)
		if e != nil {
			return e
		}
		var exists int
		_ = d.QueryRow(d.bind("SELECT COUNT(*) FROM schema_migrations WHERE version=?"), n).Scan(&exists)
		if exists > 0 {
			continue
		}
		b, e := migrations.ReadFile(f)
		if e != nil {
			return e
		}
		tx, e := d.Begin()
		if e != nil {
			return e
		}
		if _, e = tx.Exec(string(b)); e == nil {
			appliedAt := any(time.Now().UTC().Format(time.RFC3339Nano))
			if d.postgres {
				appliedAt = time.Now().UTC()
			}
			recordMigration := "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)"
			if d.postgres {
				recordMigration += " ON CONFLICT(version) DO NOTHING"
			}
			_, e = tx.Exec(d.bind(recordMigration), n, appliedAt)
		}
		if e != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s: %w", f, e)
		}
		if e = tx.Commit(); e != nil {
			return e
		}
	}
	return nil
}

type Trade struct {
	ID, Mint, Side, Quantity, Signature, State string
	Requested, Executed, Fees, Slippage        float64
	DetectedAt, SubmittedAt, ConfirmedAt       *time.Time
	EntryReason, ExitReason                    string
	EntryProfile                               string
	EntryScore                                 float64
	Reconciliation                             bool
}

func (d *DB) InsertTrade(t Trade) error {
	now := time.Now().UTC()
	exitReason := t.ExitReason
	var realizedPnl any
	if t.Side == "sell" {
		exitReason = strings.TrimSpace(exitReason)
		if exitReason == "" {
			exitReason = "unspecified exit"
		}
	}
	entryProfile := strings.TrimSpace(t.EntryProfile)
	tx, e := d.Begin()
	if e != nil {
		return e
	}
	if _, e = tx.Exec(d.bind(`INSERT INTO trades(id,mint,side,quantity,requested_value_sol,fees_sol,realized_pnl_sol,slippage_pct,state,entry_reason,exit_reason,entry_profile,entry_score,reconciliation_needed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), t.ID, t.Mint, t.Side, t.Quantity, t.Requested, t.Fees, realizedPnl, t.Slippage, t.State, t.EntryReason, exitReason, entryProfile, t.EntryScore, true, now, now); e == nil {
		_, e = tx.Exec(d.bind(`INSERT INTO transactions(id,trade_id,state,created_at,updated_at) VALUES(?,?,?,?,?)`), "tx_"+t.ID, t.ID, t.State, now, now)
	}
	if e != nil {
		_ = tx.Rollback()
		return e
	}
	return tx.Commit()
}

func (d *DB) UpdateTradeState(id, state, sig string, confirmed bool) error {
	now := time.Now().UTC()
	tx, e := d.Begin()
	if e != nil {
		return fmt.Errorf("begin trade state update for %q: %w", id, e)
	}
	defer tx.Rollback()
	result, e := tx.Exec(d.bind(`UPDATE trades SET state=?, signature=COALESCE(NULLIF(?,''),signature), submitted_at=CASE WHEN ? IN ('submitted','pending') THEN ? ELSE submitted_at END, confirmed_at=CASE WHEN ? THEN ? ELSE confirmed_at END, reconciliation_needed=?, updated_at=? WHERE id=?`), state, sig, state, now, confirmed, now, !confirmed, now, id)
	if e != nil {
		return fmt.Errorf("update trade %q to %q: %w", id, state, e)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check trade %q state update: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("trade %q not found while setting state %q", id, state)
	}
	result, e = tx.Exec(d.bind(`UPDATE transactions SET state=?,signature=COALESCE(NULLIF(?,''),signature),updated_at=? WHERE trade_id=?`), state, sig, now, id)
	if e != nil {
		return fmt.Errorf("update transaction for trade %q to %q: %w", id, state, e)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check transaction state update for trade %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for trade %q not found while setting state %q", id, state)
	}
	if e = tx.Commit(); e != nil {
		return fmt.Errorf("commit trade state %q for %q: %w", state, id, e)
	}
	return nil
}

func (d *DB) MarkTradeFailed(id string) error {
	now := time.Now().UTC()
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("begin failed-trade update for %q: %w", id, err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(d.bind(`UPDATE trades SET state='failed',reconciliation_needed=?,updated_at=? WHERE id=? AND state IN ('prepared','unknown')`), false, now, id)
	if err != nil {
		return fmt.Errorf("mark trade %q failed: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check failed state for trade %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("trade %q is not in a markable state", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='failed',updated_at=? WHERE trade_id=? AND state IN ('prepared','unknown')`), now, id)
	if err != nil {
		return fmt.Errorf("mark transaction for trade %q failed: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check failed transaction state for trade %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for trade %q is not in a markable state", id)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed state for trade %q: %w", id, err)
	}
	return nil
}

func (d *DB) ActiveForMint(mint string) (bool, error) {
	var n int
	e := d.QueryRow(d.bind(`SELECT COUNT(*) FROM trades WHERE mint=? AND state IN ('prepared','signed','submitted','pending','unknown')`), mint).Scan(&n)
	return n > 0, e
}

func (d *DB) UpdateTradeQuote(id string, quotedAt time.Time, quoteAgeMS int64, curveImpactBPS, minimumOutputSOL, requestedValueSOL float64, quantity string) error {
	now := time.Now().UTC()
	result, err := d.Exec(d.bind(`UPDATE trades
		SET quote_observed_at=?,quote_age_ms=?,curve_impact_bps=?,minimum_output_sol=?,
		requested_value_sol=?,quantity=CASE WHEN ?<>'' THEN ? ELSE quantity END,updated_at=?
		WHERE id=? AND state='prepared'`),
		quotedAt.UTC(), quoteAgeMS, curveImpactBPS, minimumOutputSOL,
		requestedValueSOL, quantity, quantity, now, id)
	if err != nil {
		return fmt.Errorf("persist live execution quote for %q: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("prepared live trade %q not found while persisting quote", id)
	}
	return nil
}

func (d *DB) HasPurchaseForMint(mint string) (bool, error) {
	var n int
	e := d.QueryRow(d.bind(`SELECT COUNT(*) FROM trades WHERE mint=? AND side='buy'`), mint).Scan(&n)
	return n > 0, e
}

func (d *DB) PositionFeesSOL(mint string) (float64, error) {
	var fees float64
	err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(fees_sol),0) FROM trades
		WHERE mint=? AND side='buy' AND state IN ('confirmed','finalized')`), mint).Scan(&fees)
	return fees, err
}

func (d *DB) HasFailedSellForMint(mint string) (bool, error) {
	var n int
	e := d.QueryRow(d.bind(`SELECT COUNT(*) FROM trades WHERE mint=? AND side='sell' AND state='failed'`), mint).Scan(&n)
	return n > 0, e
}

func (d *DB) FailedSellRetryState(mint string) (int, time.Time, error) {
	var attempts int
	var lastFailure sql.NullTime
	err := d.QueryRow(d.bind(`SELECT COUNT(*),MAX(t.updated_at)
		FROM trades t
		WHERE t.mint=? AND t.side='sell' AND t.state='failed'
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='open')`), mint).Scan(&attempts, &lastFailure)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("read failed sell retry state for mint %q: %w", mint, err)
	}
	if !lastFailure.Valid {
		return attempts, time.Time{}, nil
	}
	return attempts, lastFailure.Time.UTC(), nil
}

func (d *DB) LastSellState(mint string) (state string, updatedAt time.Time, err error) {
	var s string
	var at sql.NullTime
	err = d.QueryRow(d.bind(`SELECT state,updated_at FROM trades
		WHERE mint=? AND side='sell' ORDER BY created_at DESC,id DESC LIMIT 1`), mint).Scan(&s, &at)
	if err == sql.ErrNoRows {
		return "", time.Time{}, nil
	}
	if err != nil {
		return "", time.Time{}, fmt.Errorf("read last sell state for mint %q: %w", mint, err)
	}
	if at.Valid {
		updatedAt = at.Time.UTC()
	}
	return s, updatedAt, nil
}

func (d *DB) LastSellTradeForMint(mint string) (id, signature string, err error) {
	var sig sql.NullString
	err = d.QueryRow(d.bind(`SELECT id,COALESCE(signature,'') FROM trades
		WHERE mint=? AND side='sell' ORDER BY created_at DESC,id DESC LIMIT 1`), mint).Scan(&id, &sig)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("read last sell trade for mint %q: %w", mint, err)
	}
	return id, sig.String, nil
}

func (d *DB) StuckExitMint(mint string, olderThan time.Time) (bool, error) {
	var count int
	err := d.QueryRow(d.bind(`SELECT COUNT(*)
		FROM positions p
		JOIN trades t ON t.mint=p.mint AND t.side='sell'
		WHERE p.mint=? AND p.status='open'
		AND t.state IN ('failed','unknown','prepared')
		AND t.updated_at < ?`), mint, olderThan).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check stuck exit mint %q: %w", mint, err)
	}
	return count > 0, nil
}

func (d *DB) QuarantineStuckSimulationMint(mint string, olderThan time.Time) (int, error) {
	return d.quarantineStuckMint(mint, olderThan, false)
}

func (d *DB) QuarantineStuckLiveMint(mint string, olderThan time.Time) (int, error) {
	return d.quarantineStuckMint(mint, olderThan, true)
}

func (d *DB) quarantineStuckMint(mint string, olderThan time.Time, live bool) (int, error) {
	if live {
		_, sig, err := d.LastSellTradeForMint(mint)
		if err != nil {
			return 0, fmt.Errorf("quarantine live mint %q: %w", mint, err)
		}
		state, err := d.sellConfirmation(sig)
		if err != nil {
			return 0, fmt.Errorf("quarantine live mint %q: on-chain confirmation unavailable: %w", mint, err)
		}
		if state != "failed" {
			return 0, fmt.Errorf("quarantine live mint %q refused: on-chain state is %q (not failed)", mint, state)
		}
	}

	tx, err := d.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin quarantine for stuck mint %q: %w", mint, err)
	}
	defer tx.Rollback()

	var positionOpen int
	if err = tx.QueryRow(d.bind(`SELECT COUNT(*) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&positionOpen); err != nil {
		return 0, fmt.Errorf("read open position for stuck mint %q: %w", mint, err)
	}
	if positionOpen == 0 {
		return 0, nil
	}

	now := time.Now().UTC()
	rows, err := tx.Query(d.bind(`SELECT id FROM trades
		WHERE mint=? AND side='sell'
		AND state IN ('failed','unknown','prepared')
		AND updated_at < ?`), mint, olderThan)
	if err != nil {
		return 0, fmt.Errorf("find stuck sells for mint %q: %w", mint, err)
	}
	var tradeIDs []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("read stuck sell id for mint %q: %w", mint, err)
		}
		tradeIDs = append(tradeIDs, id)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate stuck sells for mint %q: %w", mint, err)
	}
	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("close stuck sell scan for mint %q: %w", mint, err)
	}

	result, err := tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=? WHERE mint=? AND status='open'`), now, mint)
	if err != nil {
		return 0, fmt.Errorf("close stuck position for mint %q: %w", mint, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return 0, fmt.Errorf("check stuck position close for mint %q: %w", mint, rowsErr)
	} else if affected != 1 {
		return 0, fmt.Errorf("stuck position for mint %q changed during quarantine", mint)
	}

	cleaned := 0
	for _, id := range tradeIDs {
		if _, err = tx.Exec(d.bind(`UPDATE trades SET state='failed',reconciliation_needed=?,updated_at=?
			WHERE id=? AND side='sell' AND state IN ('failed','unknown','prepared')`), false, now, id); err != nil {
			return 0, fmt.Errorf("quarantine trade %q for mint %q: %w", id, mint, err)
		}
		if _, err = tx.Exec(d.bind(`UPDATE transactions SET state='failed',updated_at=?
			WHERE trade_id=? AND state IN ('prepared','unknown','failed')`), now, id); err != nil {
			return 0, fmt.Errorf("quarantine transaction for trade %q mint %q: %w", id, mint, err)
		}
		cleaned++
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit quarantine for stuck mint %q: %w", mint, err)
	}
	return cleaned, nil
}

type SimulationSellSnapshot struct {
	TradeState       string
	TransactionState string
	PositionState    string
}

type LiveSellSnapshot = SimulationSellSnapshot

func (d *DB) SimulationSellSnapshot(id, mint string) (SimulationSellSnapshot, error) {
	return d.sellSnapshot(id, mint)
}

func (d *DB) LiveSellSnapshot(id, mint string) (LiveSellSnapshot, error) {
	return d.sellSnapshot(id, mint)
}

func (d *DB) sellSnapshot(id, mint string) (SimulationSellSnapshot, error) {
	var snapshot SimulationSellSnapshot
	err := d.QueryRow(d.bind(`SELECT t.state,COALESCE(x.state,''),COALESCE(p.status,'')
		FROM trades t
		LEFT JOIN transactions x ON x.trade_id=t.id
		LEFT JOIN positions p ON p.mint=t.mint
		WHERE t.id=? AND t.mint=? AND t.side='sell'`), id, mint).Scan(
		&snapshot.TradeState, &snapshot.TransactionState, &snapshot.PositionState)
	if err != nil {
		return snapshot, fmt.Errorf("read sell snapshot id=%q mint=%q: %w", id, mint, err)
	}
	return snapshot, nil
}

func (d *DB) MarkSimulationSellUnknown(id string) error {
	return d.markSellUnknown(id)
}

func (d *DB) MarkLiveSellUnknown(id string) error {
	return d.markSellUnknown(id)
}

func (d *DB) markSellUnknown(id string) error {
	now := time.Now().UTC()
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("begin unknown sell update for %q: %w", id, err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(d.bind(`UPDATE trades SET state='unknown',reconciliation_needed=?,updated_at=?
		WHERE id=? AND side='sell' AND state IN ('prepared','unknown')`), true, now, id)
	if err != nil {
		return fmt.Errorf("mark sell %q unknown: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check unknown state for sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("sell %q is not in a markable state", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='unknown',updated_at=?
		WHERE trade_id=? AND state IN ('prepared','unknown')`), now, id)
	if err != nil {
		return fmt.Errorf("mark transaction for sell %q unknown: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check unknown transaction state for sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for sell %q is not in a markable state", id)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit unknown state for sell %q: %w", id, err)
	}
	return nil
}

func (d *DB) MarkSimulationSellFailed(id, mint string) error {
	return d.markSellFailed(id, mint, false)
}

func (d *DB) MarkLiveSellFailed(id, mint string) error {
	var sig sql.NullString
	err := d.QueryRow(d.bind(`SELECT COALESCE(signature,'') FROM trades WHERE id=? AND side='sell'`), id).Scan(&sig)
	if err != nil {
		return fmt.Errorf("read signature for live sell %q: %w", id, err)
	}
	state, err := d.sellConfirmation(sig.String)
	if err != nil {
		return d.markSellUnknown(id)
	}
	if state != "failed" {
		return d.markSellUnknown(id)
	}
	return d.markSellFailed(id, mint, true)
}

func (d *DB) markSellFailed(id, mint string, live bool) error {
	_ = live
	now := time.Now().UTC()
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("begin failed sell update for %q: %w", id, err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=?
		WHERE mint=? AND status='open'`), now, mint)
	if err != nil {
		return fmt.Errorf("close position for failed sell %q: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check position close for %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("position for mint %q is no longer open", mint)
	}
	result, err = tx.Exec(d.bind(`UPDATE trades SET state='failed',reconciliation_needed=?,updated_at=?
		WHERE id=? AND side='sell' AND state IN ('prepared','unknown')`), false, now, id)
	if err != nil {
		return fmt.Errorf("mark sell %q failed: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check failed state for sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("sell %q is not in a markable state", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='failed',updated_at=?
		WHERE trade_id=? AND state IN ('prepared','unknown')`), now, id)
	if err != nil {
		return fmt.Errorf("mark transaction for sell %q failed: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check failed transaction state for sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for sell %q is not in a markable state", id)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failed sell %q: %w", id, err)
	}
	return nil
}

func (d *DB) ReconcileStaleSimulationSells(mint string, olderThan time.Time) (int, error) {
	return d.reconcileStaleSells(mint, olderThan, false)
}

func (d *DB) ReconcileStaleLiveSells(mint string, olderThan time.Time) (int, error) {
	return d.reconcileStaleSells(mint, olderThan, true)
}

func (d *DB) reconcileStaleSells(mint string, olderThan time.Time, live bool) (int, error) {
	if live {
		_, sig, err := d.LastSellTradeForMint(mint)
		if err != nil {
			return 0, fmt.Errorf("reconcile stale live sells for mint %q: %w", mint, err)
		}
		state, err := d.sellConfirmation(sig)
		if err != nil {
			return 0, fmt.Errorf("reconcile stale live sells for mint %q: on-chain confirmation unavailable: %w", mint, err)
		}
		if state != "failed" {
			return 0, fmt.Errorf("reconcile stale live sells for mint %q refused: on-chain state is %q (not failed)", mint, state)
		}
	}

	tx, err := d.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin stale sell reconciliation for mint %q: %w", mint, err)
	}
	defer tx.Rollback()

	rows, err := tx.Query(d.bind(`SELECT t.id,t.state,COALESCE(x.state,''),COALESCE(p.status,''),t.updated_at
		FROM trades t
		LEFT JOIN transactions x ON x.trade_id=t.id
		LEFT JOIN positions p ON p.mint=t.mint
		WHERE t.mint=? AND t.side='sell' AND t.state IN ('prepared','unknown')
		ORDER BY t.created_at`), mint)
	if err != nil {
		return 0, fmt.Errorf("find stale sells for mint %q: %w", mint, err)
	}
	type staleSell struct {
		id, tradeState, transactionState, positionState string
		updatedAt                                       time.Time
	}
	var candidates []staleSell
	for rows.Next() {
		var candidate staleSell
		if err = rows.Scan(&candidate.id, &candidate.tradeState, &candidate.transactionState, &candidate.positionState, &candidate.updatedAt); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("read stale sell for mint %q: %w", mint, err)
		}
		candidates = append(candidates, candidate)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate stale sells for mint %q: %w", mint, err)
	}
	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale sell scan for mint %q: %w", mint, err)
	}

	reconciled := 0
	now := time.Now().UTC()
	for _, candidate := range candidates {
		if candidate.updatedAt.After(olderThan) {
			continue
		}
		if candidate.positionState != "open" ||
			(candidate.tradeState != "prepared" && candidate.tradeState != "unknown") ||
			(candidate.transactionState != "" &&
				candidate.transactionState != "prepared" &&
				candidate.transactionState != "unknown" &&
				candidate.transactionState != "failed") {
			return 0, fmt.Errorf("stale sell id=%q mint=%q has inconsistent persisted states trade=%q transaction=%q position=%q",
				candidate.id, mint, candidate.tradeState, candidate.transactionState, candidate.positionState)
		}
		result, updateErr := tx.Exec(d.bind(`UPDATE trades SET state='failed',reconciliation_needed=?,updated_at=?
			WHERE id=? AND side='sell' AND state=?`), false, now, candidate.id, candidate.tradeState)
		if updateErr != nil {
			return 0, fmt.Errorf("recover stale sell id=%q mint=%q: %w", candidate.id, mint, updateErr)
		}
		if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
			return 0, fmt.Errorf("check stale sell id=%q mint=%q: %w", candidate.id, mint, rowsErr)
		} else if affected != 1 {
			return 0, fmt.Errorf("stale sell id=%q mint=%q changed during reconciliation", candidate.id, mint)
		}
		if candidate.transactionState == "prepared" || candidate.transactionState == "unknown" {
			result, updateErr = tx.Exec(d.bind(`UPDATE transactions SET state='failed',updated_at=?
				WHERE trade_id=? AND state=?`), now, candidate.id, candidate.transactionState)
			if updateErr != nil {
				return 0, fmt.Errorf("recover transaction for stale sell id=%q mint=%q: %w", candidate.id, mint, updateErr)
			}
			if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
				return 0, fmt.Errorf("check transaction recovery for sell id=%q mint=%q: %w", candidate.id, mint, rowsErr)
			} else if affected != 1 {
				return 0, fmt.Errorf("transaction for stale sell id=%q mint=%q changed during reconciliation", candidate.id, mint)
			}
		}
		if _, closeErr := tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=?
			WHERE mint=? AND status='open'`), now, mint); closeErr != nil {
			return 0, fmt.Errorf("close stale position for mint %q: %w", mint, closeErr)
		}
		reconciled++
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit stale sell reconciliation for mint %q: %w", mint, err)
	}
	return reconciled, nil
}

func (d *DB) RecordEvent(mint, creator, raw string, at time.Time, latency int64) error {
	query := `INSERT OR IGNORE INTO token_events(mint,creator,received_at,raw_json,latency_ms) VALUES(?,?,?,?,?)`
	if d.postgres {
		query = `INSERT INTO token_events(mint,creator,received_at,raw_json,latency_ms) VALUES(?,?,?,?,?) ON CONFLICT (mint, received_at) DO NOTHING`
	}
	receivedAt := any(at.UTC().Format(time.RFC3339Nano))
	if d.postgres {
		receivedAt = at.UTC()
	}
	_, e := d.Exec(d.bind(query), mint, creator, receivedAt, raw, latency)
	return e
}

func (d *DB) RecordAssessment(mint string, approved bool, reason string) error {
	_, e := d.Exec(d.bind(`INSERT INTO risk_assessments(mint,approved,reason,created_at) VALUES(?,?,?,?)`), mint, approved, reason, time.Now().UTC())
	return e
}

// RecordAssessmentWithProfile persists the assessment together with the
// entry profile that authorized the decision.
func (d *DB) RecordAssessmentWithProfile(mint string, approved bool, reason, profile string) error {
	_, e := d.Exec(d.bind(`INSERT INTO risk_assessments(mint,approved,reason,entry_profile,created_at) VALUES(?,?,?,?,?)`), mint, approved, reason, strings.TrimSpace(profile), time.Now().UTC())
	return e
}

func (d *DB) ListTrades() (rows *sql.Rows, err error) {
	return d.Query(`SELECT id,mint,side,quantity,requested_value_sol,COALESCE(executed_value_sol,0),
		COALESCE(effective_price_sol,0),
		COALESCE(fees_sol,0),slippage_pct,COALESCE(signature,''),state,created_at,realized_pnl_sol,
		COALESCE(exit_reason,''),COALESCE(entry_profile,''),COALESCE(entry_score,0) FROM trades ORDER BY created_at DESC LIMIT 200`)
}

func (d *DB) TokenEventCount() (int, error) {
	var n int
	e := d.QueryRow(`SELECT COUNT(*) FROM token_events`).Scan(&n)
	return n, e
}

func (d *DB) RecordRPC(endpoint string, healthy bool, latency int64, status int, msg string) error {
	_, e := d.Exec(d.bind(`INSERT INTO rpc_health(endpoint,checked_at,latency_ms,healthy,http_status,error,supports_read) VALUES(?,?,?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET checked_at=excluded.checked_at,latency_ms=excluded.latency_ms,healthy=excluded.healthy,http_status=excluded.http_status,error=excluded.error,supports_read=excluded.supports_read`), endpoint, time.Now().UTC(), latency, healthy, status, strings.TrimSpace(msg), healthy)
	return e
}

func (d *DB) ListTokens() (*sql.Rows, error) {
	return d.Query(d.bind(`SELECT e.mint,COALESCE(e.creator,''),e.received_at,e.latency_ms
		,r.approved,COALESCE(r.reason,''),COALESCE(r.entry_profile,''),COALESCE(r.entry_score,0),COALESCE(t.side,''),COALESCE(t.state,''),COALESCE(p.status,''),
		COALESCE((SELECT l.message FROM operational_logs l
			WHERE l.event_type='post_entry_risk_screen'
			AND l.payload_json LIKE '%' || e.mint || '%'
			ORDER BY l.id DESC LIMIT 1),'')
		FROM token_events e
		LEFT JOIN risk_assessments r ON r.id=(SELECT MAX(r2.id) FROM risk_assessments r2
			WHERE r2.mint=e.mint AND r2.reason NOT LIKE 'post-entry risk screen:%')
		LEFT JOIN trades t ON t.id=(SELECT t2.id FROM trades t2 WHERE t2.mint=e.mint ORDER BY t2.created_at DESC LIMIT 1)
		LEFT JOIN positions p ON p.mint=e.mint
		ORDER BY e.id DESC LIMIT 200`))
}

func (d *DB) OpenSimulationPosition(mint string, amount float64) error {
	return d.OpenPosition(mint, amount)
}

func (d *DB) FinalizeSimulationBuy(id, mint string, amount float64, feesOpt ...float64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	fees := 0.0
	if len(feesOpt) > 0 {
		fees = feesOpt[0]
	}
	if _, err = tx.Exec(d.bind(`INSERT INTO positions(mint,quantity,cost_sol,status,opened_at,updated_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,opened_at=excluded.opened_at,updated_at=excluded.updated_at`),
		mint, fmt.Sprint(amount), amount, "open", now, now); err != nil {
		return err
	}
	if err = setSimulationTradeState(tx, d, id, amount, fees, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) FinalizeSimulationSell(id, mint string, value float64, feesOpt ...float64) error {
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("begin finalization for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	defer tx.Rollback()

	var cost float64
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(cost_sol,0) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&cost); err != nil {
		return fmt.Errorf("read open position cost for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	var exitReason string
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(exit_reason,'') FROM trades WHERE id=? AND side='sell'`), id).Scan(&exitReason); err != nil {
		return fmt.Errorf("read exit reason for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	exitReason = strings.TrimSpace(exitReason)
	if exitReason == "" {
		exitReason = "unspecified exit"
	}
	now := time.Now().UTC()
	fees := 0.0
	if len(feesOpt) > 0 {
		fees = feesOpt[0]
	}
	realizedPnl := value - cost
	result, err := tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=? WHERE mint=? AND status='open'`), now, mint)
	if err != nil {
		return fmt.Errorf("mark position closed for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check position close for simulated sell id=%q mint=%q: %w", id, mint, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("simulated position %q is no longer open", mint)
	}
	result, err = tx.Exec(d.bind(`UPDATE trades SET state='simulated',executed_value_sol=COALESCE(?,0.0),fees_sol=COALESCE(?,0.0),realized_pnl_sol=COALESCE(?,0.0),exit_reason=COALESCE(NULLIF(TRIM(exit_reason),''),?),confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=? AND side='sell' AND state='prepared'`),
		value, fees, realizedPnl, exitReason, now, false, now, id)
	if err != nil {
		return fmt.Errorf("finalize trade row for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check trade finalization for simulated sell id=%q mint=%q: %w", id, mint, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("simulated sell trade %q not found", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='simulated',updated_at=? WHERE trade_id=? AND state='prepared'`), now, id)
	if err != nil {
		return fmt.Errorf("finalize transaction row for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check transaction finalization for simulated sell id=%q mint=%q: %w", id, mint, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for simulated sell %q not found", id)
	}
	if err = blacklistCreatorAfterStopLoss(tx, d, mint, exitReason, now); err != nil {
		return fmt.Errorf("apply stop-loss creator blacklist for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit finalization for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	return nil
}

func (d *DB) FinalizeLiveBuy(id, mint, state, signature string, cost, fees float64, quantity string) error {
	effectivePrice, err := executionPrice(cost, quantity)
	if err != nil {
		return fmt.Errorf("invalid confirmed live buy execution: %w", err)
	}
	if cost <= 0 || fees < 0 || math.IsNaN(fees) || math.IsInf(fees, 0) {
		return fmt.Errorf("invalid confirmed live buy cost or fees")
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	if _, err = tx.Exec(d.bind(`INSERT INTO positions(mint,quantity,cost_sol,status,opened_at,updated_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,opened_at=excluded.opened_at,updated_at=excluded.updated_at`),
		mint, quantity, cost, "open", now, now); err != nil {
		return err
	}
	if err = updateExecutedTrade(tx, d, id, state, signature, quantity, cost, fees, effectivePrice, nil, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) FinalizeLiveSell(id, mint, state, signature string, value, fees float64, quantity string) error {
	effectivePrice, err := executionPrice(value, quantity)
	if err != nil {
		return fmt.Errorf("invalid confirmed live sell execution: %w", err)
	}
	if value <= 0 || fees < 0 || math.IsNaN(fees) || math.IsInf(fees, 0) {
		return fmt.Errorf("invalid confirmed live sell value or fees")
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var cost float64
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(cost_sol,0) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&cost); err != nil {
		return err
	}
	if cost <= 0 {
		return fmt.Errorf("invalid confirmed live position cost")
	}
	var entryFees float64
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(SUM(fees_sol),0) FROM trades
		WHERE mint=? AND side='buy' AND state IN ('confirmed','finalized')`), mint).Scan(&entryFees); err != nil {
		return err
	}
	var expectedQuantity, exitReason string
	var minimumOutputSOL float64
	if err = tx.QueryRow(d.bind(`SELECT quantity,COALESCE(minimum_output_sol,0),COALESCE(exit_reason,'')
		FROM trades WHERE id=? AND side='sell'`), id).Scan(&expectedQuantity, &minimumOutputSOL, &exitReason); err != nil {
		return err
	}
	if minimumOutputSOL > 0 && strings.TrimSpace(expectedQuantity) != strings.TrimSpace(quantity) {
		return fmt.Errorf("confirmed sell token amount differs from the quoted wallet balance")
	}
	now := time.Now().UTC()
	result, err := tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=? WHERE mint=? AND status='open'`), now, mint)
	if err != nil {
		return err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("live position %q is no longer open", mint)
	}
	netPnl := value - cost - entryFees - fees
	if err = updateExecutedTrade(tx, d, id, state, signature, quantity, value, fees, effectivePrice, netPnl, now); err != nil {
		return err
	}
	if err = blacklistCreatorAfterStopLoss(tx, d, mint, exitReason, now); err != nil {
		return err
	}
	return tx.Commit()
}

func blacklistCreatorAfterStopLoss(tx *sql.Tx, d *DB, mint, reason string, now time.Time) error {
	normalizedReason := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(reason)), "-", " ")
	if !strings.Contains(normalizedReason, "stop loss") {
		return nil
	}
	var creator sql.NullString
	err := tx.QueryRow(d.bind(`SELECT creator FROM token_events WHERE mint=? AND TRIM(COALESCE(creator,''))<>'' ORDER BY received_at DESC LIMIT 1`), mint).Scan(&creator)
	if err == sql.ErrNoRows || (err == nil && (!creator.Valid || strings.TrimSpace(creator.String) == "")) {
		return nil
	}
	if err != nil {
		return err
	}

	blockedAt := any(now)
	if !d.postgres {
		blockedAt = now.UTC().Format(time.RFC3339Nano)
	}
	_, err = tx.Exec(d.bind(`INSERT INTO creator_blacklist(creator,blocked_at)
		VALUES(?,?)
		ON CONFLICT(creator) DO UPDATE SET blocked_at=excluded.blocked_at`),
		strings.TrimSpace(creator.String), blockedAt)
	return err
}

func (d *DB) IsCreatorBlacklisted(creator string) (bool, error) {
	creator = strings.TrimSpace(creator)
	if creator == "" {
		return false, nil
	}
	cutoff := any(time.Now().UTC().Add(-24 * time.Hour))
	if !d.postgres {
		cutoff = time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339Nano)
	}
	var count int
	err := d.QueryRow(d.bind(`SELECT COUNT(*) FROM creator_blacklist WHERE creator=? AND blocked_at>?`), creator, cutoff).Scan(&count)
	return count > 0, err
}

func updateExecutedTrade(tx *sql.Tx, d *DB, id, state, signature, quantity string, executed, fees, effectivePrice float64, realized any, now time.Time) error {
	result, err := tx.Exec(d.bind(`UPDATE trades SET state=?,signature=COALESCE(NULLIF(?,''),signature),
		quantity=?,executed_value_sol=?,fees_sol=?,effective_price_sol=?,realized_pnl_sol=?,
		confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=?`),
		state, signature, quantity, executed, fees, effectivePrice, realized, now, false, now, id)
	if err != nil {
		return err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("live trade %q not found", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state=?,signature=COALESCE(NULLIF(?,''),signature),updated_at=? WHERE trade_id=?`), state, signature, now, id)
	if err != nil {
		return err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("transaction for live trade %q not found", id)
	}
	return nil
}

func executionPrice(valueSOL float64, quantity string) (float64, error) {
	if valueSOL <= 0 || math.IsNaN(valueSOL) || math.IsInf(valueSOL, 0) {
		return 0, fmt.Errorf("executed SOL value must be finite and positive")
	}
	amount, err := strconv.ParseFloat(strings.TrimSpace(quantity), 64)
	if err != nil || amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return 0, fmt.Errorf("confirmed token quantity must be finite and positive")
	}
	price := valueSOL / amount
	if price <= 0 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, fmt.Errorf("effective execution price is invalid")
	}
	return price, nil
}

func setSimulationTradeState(tx *sql.Tx, d *DB, id string, amount, fees float64, now time.Time) error {
	result, err := tx.Exec(d.bind(`UPDATE trades SET state='simulated',executed_value_sol=?,fees_sol=?,confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=? AND side='buy'`), amount, fees, now, false, now, id)
	if err != nil {
		return err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("simulated buy trade %q not found", id)
	}
	if _, err = tx.Exec(d.bind(`UPDATE transactions SET state='simulated',updated_at=? WHERE trade_id=?`), now, id); err != nil {
		return err
	}
	return nil
}

func (d *DB) OpenPosition(mint string, amount float64) error {
	now := time.Now().UTC()
	query := `INSERT INTO positions(mint,quantity,cost_sol,status,opened_at,updated_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,opened_at=excluded.opened_at,updated_at=excluded.updated_at`
	_, e := d.Exec(d.bind(query), mint, fmt.Sprint(amount), amount, "open", now, now)
	return e
}

func (d *DB) RecordPaperMark(mint string, marketCapSOL float64) error {
	return d.recordMark("paper_marks", mint, marketCapSOL)
}
func (d *DB) RecordLiveMark(mint string, marketCapSOL float64) error {
	return d.recordMark("live_marks", mint, marketCapSOL)
}

const VerifiedPositionPriceSource = "solana_rpc_bonding_curve"

func (d *DB) RecordVerifiedPaperMark(mint string, marketCapSOL, tokenPriceSOL float64) error {
	return d.recordVerifiedMark("paper_marks", mint, marketCapSOL, tokenPriceSOL)
}
func (d *DB) RecordVerifiedLiveMark(mint string, marketCapSOL, tokenPriceSOL float64) error {
	return d.recordVerifiedMark("live_marks", mint, marketCapSOL, tokenPriceSOL)
}
func (d *DB) recordVerifiedMark(table, mint string, marketCapSOL, tokenPriceSOL float64) error {
	if marketCapSOL <= 0 || tokenPriceSOL <= 0 {
		return nil
	}
	d.markMu.Lock()
	defer d.markMu.Unlock()
	now := time.Now().UTC()
	_, err := d.Exec(d.bind(`INSERT INTO `+table+`(
		mint,entry_market_cap_sol,current_market_cap_sol,peak_market_cap_sol,updated_at,
		entry_token_price_sol,current_token_price_sol,peak_token_price_sol,price_source
	) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(mint) DO NOTHING`),
		mint, marketCapSOL, marketCapSOL, marketCapSOL, now,
		tokenPriceSOL, tokenPriceSOL, tokenPriceSOL, VerifiedPositionPriceSource)
	return err
}
func (d *DB) recordMark(table, mint string, marketCapSOL float64) error {
	if marketCapSOL <= 0 {
		return nil
	}
	d.markMu.Lock()
	defer d.markMu.Unlock()
	now := time.Now().UTC()
	_, e := d.Exec(d.bind(`INSERT INTO `+table+`(mint,entry_market_cap_sol,current_market_cap_sol,peak_market_cap_sol,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(mint) DO NOTHING`), mint, marketCapSOL, marketCapSOL, marketCapSOL, now)
	return e
}
func (d *DB) UpdatePaperMark(mint string, marketCapSOL float64) error {
	return d.updateMark("paper_marks", mint, marketCapSOL)
}
func (d *DB) UpdateLiveMark(mint string, marketCapSOL float64) error {
	return d.updateMark("live_marks", mint, marketCapSOL)
}
func (d *DB) PaperMarkStats() (total, flat int, pctFlat float64, err error) {
	err = d.QueryRow(`SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN m.current_market_cap_sol=m.entry_market_cap_sol THEN 1 ELSE 0 END),0)
		FROM paper_marks m
		JOIN positions p ON p.mint=m.mint AND p.status='open'`).Scan(&total, &flat)
	if err != nil {
		return 0, 0, 0, err
	}
	if total > 0 {
		pctFlat = float64(flat) * 100 / float64(total)
	}
	return total, flat, pctFlat, nil
}
func (d *DB) MarkMarketCaps(mint string, simulation bool) (entry, current, peak float64, known bool, err error) {
	table := "live_marks"
	if simulation {
		table = "paper_marks"
	}
	err = d.QueryRow(d.bind(`SELECT entry_market_cap_sol,current_market_cap_sol,peak_market_cap_sol FROM `+table+` WHERE mint=?`), mint).Scan(&entry, &current, &peak)
	if err == sql.ErrNoRows {
		return 0, 0, 0, false, nil
	}
	return entry, current, peak, err == nil, err
}
func (d *DB) updateMark(table, mint string, marketCapSOL float64) error {
	if marketCapSOL <= 0 {
		return nil
	}
	d.markMu.Lock()
	defer d.markMu.Unlock()
	_, e := d.Exec(d.bind(`UPDATE `+table+` SET current_market_cap_sol=?,peak_market_cap_sol=CASE WHEN ?>peak_market_cap_sol THEN ? ELSE peak_market_cap_sol END,updated_at=? WHERE mint=?`), marketCapSOL, marketCapSOL, marketCapSOL, time.Now().UTC(), mint)
	return e
}
func (d *DB) UpdateVerifiedPaperMark(mint string, marketCapSOL, tokenPriceSOL float64) error {
	return d.updateVerifiedMark("paper_marks", mint, marketCapSOL, tokenPriceSOL)
}
func (d *DB) UpdateVerifiedLiveMark(mint string, marketCapSOL, tokenPriceSOL float64) error {
	return d.updateVerifiedMark("live_marks", mint, marketCapSOL, tokenPriceSOL)
}
func (d *DB) updateVerifiedMark(table, mint string, marketCapSOL, tokenPriceSOL float64) error {
	if marketCapSOL <= 0 || tokenPriceSOL <= 0 {
		return nil
	}
	d.markMu.Lock()
	defer d.markMu.Unlock()
	now := time.Now().UTC()
	_, err := d.Exec(d.bind(`UPDATE `+table+` SET
		current_market_cap_sol=?,
		entry_market_cap_sol=CASE WHEN entry_token_price_sol<=0 THEN ? ELSE entry_market_cap_sol END,
		peak_market_cap_sol=CASE WHEN entry_token_price_sol<=0 THEN ? WHEN ?>peak_market_cap_sol THEN ? ELSE peak_market_cap_sol END,
		current_token_price_sol=?,
		peak_token_price_sol=CASE WHEN ?>peak_token_price_sol THEN ? ELSE peak_token_price_sol END,
		entry_token_price_sol=CASE WHEN entry_token_price_sol<=0 THEN ? ELSE entry_token_price_sol END,
		price_source=?,updated_at=? WHERE mint=?`),
		marketCapSOL, marketCapSOL, marketCapSOL, marketCapSOL, marketCapSOL,
		tokenPriceSOL, tokenPriceSOL, tokenPriceSOL, tokenPriceSOL,
		VerifiedPositionPriceSource, now, mint)
	return err
}

func (d *DB) PositionMarkPrices(mint string, simulation bool) (entry, current, peak float64, source, updatedAt string, known bool, err error) {
	table := "live_marks"
	if simulation {
		table = "paper_marks"
	}
	err = d.QueryRow(d.bind(`SELECT entry_token_price_sol,current_token_price_sol,peak_token_price_sol,
		price_source,COALESCE(CAST(updated_at AS TEXT),'') FROM `+table+` WHERE mint=?`), mint).
		Scan(&entry, &current, &peak, &source, &updatedAt)
	if err == sql.ErrNoRows {
		return 0, 0, 0, "", "", false, nil
	}
	return entry, current, peak, source, updatedAt, err == nil, err
}

func (d *DB) PaperSpotReturnPct(mint string) (float64, bool, error) {
	return d.markSpotReturnPct("paper_marks", mint, false)
}
func (d *DB) LiveSpotReturnPct(mint string) (float64, bool, error) {
	return d.markSpotReturnPct("live_marks", mint, false)
}
func (d *DB) PaperPeakSpotReturnPct(mint string) (float64, bool, error) {
	return d.markSpotReturnPct("paper_marks", mint, true)
}
func (d *DB) LivePeakSpotReturnPct(mint string) (float64, bool, error) {
	return d.markSpotReturnPct("live_marks", mint, true)
}
func (d *DB) markSpotReturnPct(table, mint string, peak bool) (float64, bool, error) {
	target := "current_token_price_sol"
	if peak {
		target = "peak_token_price_sol"
	}
	var entry, current float64
	var source string
	err := d.QueryRow(d.bind(`SELECT entry_token_price_sol,`+target+`,price_source FROM `+table+` WHERE mint=?`), mint).
		Scan(&entry, &current, &source)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if source != VerifiedPositionPriceSource || entry <= 0 || current <= 0 {
		return 0, false, nil
	}
	return (current - entry) / entry * 100, true, nil
}
func (d *DB) PaperReturnPct(mint string) (float64, bool, error) {
	return d.markReturnPct("paper_marks", mint)
}
func (d *DB) LiveReturnPct(mint string) (float64, bool, error) {
	return d.markReturnPct("live_marks", mint)
}
func (d *DB) PaperPeakReturnPct(mint string) (float64, bool, error) {
	return d.markPeakReturnPct("paper_marks", mint)
}
func (d *DB) LivePeakReturnPct(mint string) (float64, bool, error) {
	return d.markPeakReturnPct("live_marks", mint)
}
func (d *DB) markReturnPct(table, mint string) (float64, bool, error) {
	var entry, current float64
	e := d.QueryRow(d.bind(`SELECT entry_market_cap_sol,current_market_cap_sol FROM `+table+` WHERE mint=?`), mint).Scan(&entry, &current)
	if e == sql.ErrNoRows {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if entry <= 0 {
		return 0, false, nil
	}
	return (current - entry) / entry * 100, true, nil
}
func (d *DB) markPeakReturnPct(table, mint string) (float64, bool, error) {
	var entry, peak float64
	e := d.QueryRow(d.bind(`SELECT entry_market_cap_sol,peak_market_cap_sol FROM `+table+` WHERE mint=?`), mint).Scan(&entry, &peak)
	if e == sql.ErrNoRows {
		return 0, false, nil
	}
	if e != nil {
		return 0, false, e
	}
	if entry <= 0 || peak <= 0 {
		return 0, false, nil
	}
	return (peak - entry) / entry * 100, true, nil
}
func (d *DB) PositionCost(mint string) (float64, error) {
	var cost float64
	e := d.QueryRow(d.bind(`SELECT COALESCE(cost_sol,0) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&cost)
	return cost, e
}
func (d *DB) CloseSimulationPosition(mint string, value float64) (float64, error) {
	return d.ClosePosition(mint)
}
func (d *DB) ClosePosition(mint string) (float64, error) {
	tx, e := d.Begin()
	if e != nil {
		return 0, e
	}
	var cost float64
	if e = tx.QueryRow(d.bind(`SELECT COALESCE(cost_sol,0) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&cost); e != nil {
		_ = tx.Rollback()
		return 0, e
	}
	if _, e = tx.Exec(d.bind(`UPDATE positions SET status='closed',updated_at=? WHERE mint=?`), time.Now().UTC(), mint); e != nil {
		_ = tx.Rollback()
		return 0, e
	}
	if e = tx.Commit(); e != nil {
		return 0, e
	}
	return cost, nil
}
func (d *DB) SetTradePnl(id string, executed, pnl float64) error {
	_, e := d.Exec(d.bind(`UPDATE trades SET executed_value_sol=?,realized_pnl_sol=?,updated_at=? WHERE id=?`), executed, pnl, time.Now().UTC(), id)
	return e
}

func (d *DB) SetTradeExecution(id string, executed, fees, pnl float64) error {
	_, e := d.Exec(d.bind(`UPDATE trades SET executed_value_sol=?,fees_sol=?,realized_pnl_sol=?,updated_at=? WHERE id=?`), executed, fees, pnl, time.Now().UTC(), id)
	return e
}
func (d *DB) PaperOpenPnlSOL() (float64, error) {
	return d.OpenPnlSOLFresh(true, time.Now().UTC().Add(-2*time.Minute))
}
func (d *DB) OpenPnlSOL(simulation bool) (float64, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	return d.openPnlSOLFromMarks(marks, VerifiedPositionPriceSource, time.Time{})
}

func (d *DB) OpenPnlSOLFresh(simulation bool, staleBefore time.Time) (float64, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	return d.openPnlSOLFromMarks(marks, VerifiedPositionPriceSource, staleBefore)
}

func (d *DB) openPnlSOLFromMarks(marks, source string, staleBefore time.Time) (float64, error) {
	var pnl float64
	query := `SELECT COALESCE(SUM(p.cost_sol * (m.current_token_price_sol - m.entry_token_price_sol) / NULLIF(m.entry_token_price_sol,0)),0)
		FROM positions p JOIN ` + marks + ` m ON m.mint=p.mint
		WHERE p.status='open' AND p.cost_sol>0 AND m.price_source=?`
	args := []any{source}
	if !staleBefore.IsZero() {
		query += ` AND m.updated_at>=?`
		args = append(args, staleBefore)
	}
	e := d.QueryRow(d.bind(query), args...).Scan(&pnl)
	return pnl, e
}
func (d *DB) UnmarkedOpenPositionCount(simulation bool) (int, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	var count int
	query := `SELECT COUNT(*) FROM positions p LEFT JOIN ` + marks + ` m ON m.mint=p.mint
		WHERE p.status='open' AND (COALESCE(p.cost_sol,0)<=0 OR m.mint IS NULL OR
		m.entry_token_price_sol<=0 OR m.current_token_price_sol<=0 OR m.price_source<>?)`
	err := d.QueryRow(d.bind(query), VerifiedPositionPriceSource).Scan(&count)
	return count, err
}
func (d *DB) TradeCostsSOL() (float64, error) {
	var costs float64
	err := d.QueryRow(`SELECT COALESCE(SUM(fees_sol),0) FROM trades WHERE fees_sol IS NOT NULL AND state IN ('simulated','confirmed','finalized')`).Scan(&costs)
	return costs, err
}
func (d *DB) TradeCount() (int, error) {
	var count int
	err := d.QueryRow(`SELECT COUNT(*) FROM trades`).Scan(&count)
	return count, err
}

type PerformanceStats struct {
	SellAttempts, FailedSellAttempts, ClosedTrades, Winners, Losers                                     int
	WinRatePct, AvgWinSOL, AvgLossSOL, AvgHoldSeconds, FeesSOL, GrossRealizedPnlSOL, NetRealizedPnlSOL float64
}

func (d *DB) Performance(simulationMode ...bool) (PerformanceStats, error) {
	simulation := len(simulationMode) > 0 && simulationMode[0]
	var stats PerformanceStats
	err := d.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN t.state='failed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol IS NOT NULL
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol>0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol<0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(AVG(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol>0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol END),0),
		COALESCE(AVG(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol<0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol IS NOT NULL
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol ELSE 0 END),0)
		FROM trades t WHERE t.side='sell'`).Scan(
		&stats.SellAttempts, &stats.FailedSellAttempts, &stats.ClosedTrades,
		&stats.Winners, &stats.Losers, &stats.AvgWinSOL, &stats.AvgLossSOL, &stats.GrossRealizedPnlSOL)
	if err != nil {
		return stats, err
	}
	err = d.QueryRow(`SELECT COALESCE(SUM(t.fees_sol),0)
		FROM trades t
		WHERE t.state IN ('simulated','confirmed','finalized')
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')`).Scan(&stats.FeesSOL)
	if err != nil {
		return stats, err
	}
	holdQuery := `SELECT COALESCE(AVG((julianday(p.updated_at)-julianday(p.opened_at))*86400),0)
		FROM positions p WHERE p.status='closed' AND EXISTS
		(SELECT 1 FROM trades t WHERE t.mint=p.mint AND t.side='sell'
			AND t.state IN ('simulated','confirmed','finalized') AND t.realized_pnl_sol IS NOT NULL)`
	if d.postgres {
		holdQuery = `SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (p.updated_at-p.opened_at))),0)
			FROM positions p WHERE p.status='closed' AND EXISTS
			(SELECT 1 FROM trades t WHERE t.mint=p.mint AND t.side='sell'
				AND t.state IN ('simulated','confirmed','finalized') AND t.realized_pnl_sol IS NOT NULL)`
	}
	if err = d.QueryRow(holdQuery).Scan(&stats.AvgHoldSeconds); err != nil {
		return stats, err
	}
	if stats.ClosedTrades > 0 {
		stats.WinRatePct = float64(stats.Winners) * 100 / float64(stats.ClosedTrades)
	}
	stats.NetRealizedPnlSOL = stats.GrossRealizedPnlSOL
	if simulation {
		stats.NetRealizedPnlSOL -= stats.FeesSOL
	} else {
		stats.GrossRealizedPnlSOL += stats.FeesSOL
	}
	return stats, nil
}

func (d *DB) PerformanceByProfile(simulationMode bool, profile string) (PerformanceStats, error) {
	var stats PerformanceStats
	profile = strings.TrimSpace(profile)
	err := d.QueryRow(d.bind(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN t.state='failed' THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol IS NOT NULL
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol>0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol<0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN 1 ELSE 0 END),0),
		COALESCE(AVG(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol>0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol END),0),
		COALESCE(AVG(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol<0
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol END),0),
		COALESCE(SUM(CASE WHEN t.state IN ('simulated','confirmed','finalized')
			AND t.realized_pnl_sol IS NOT NULL
			AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
			THEN t.realized_pnl_sol ELSE 0 END),0)
		FROM trades t
		WHERE t.side='sell' AND t.entry_profile=?`), profile).Scan(
		&stats.SellAttempts, &stats.FailedSellAttempts, &stats.ClosedTrades,
		&stats.Winners, &stats.Losers, &stats.AvgWinSOL, &stats.AvgLossSOL, &stats.GrossRealizedPnlSOL)
	if err != nil {
		return stats, err
	}
	err = d.QueryRow(d.bind(`SELECT COALESCE(SUM(t.fees_sol),0)
		FROM trades t
		WHERE t.state IN ('simulated','confirmed','finalized') AND t.entry_profile=?
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')`), profile).Scan(&stats.FeesSOL)
	if err != nil {
		return stats, err
	}
	if stats.ClosedTrades > 0 {
		stats.WinRatePct = float64(stats.Winners) * 100 / float64(stats.ClosedTrades)
	}
	stats.NetRealizedPnlSOL = stats.GrossRealizedPnlSOL
	if simulationMode {
		stats.NetRealizedPnlSOL -= stats.FeesSOL
	} else {
		stats.GrossRealizedPnlSOL += stats.FeesSOL
	}
	return stats, nil
}

// ScoreBucket agrega trades por faixa de score de entrada.
type ScoreBucket struct {
	Bucket     string
	Trades     int
	Winners    int
	Losers     int
	WinRatePct float64
	NetPnlSOL  float64
	FeesSOL    float64
}

// PerformanceByScoreBucket devolve métricas por faixa de score de entrada
// (0-20, 20-40, 40-60, 60-80, 80-100) para provar se o score prevê resultados.
func (d *DB) PerformanceByScoreBucket(simulation bool) ([]ScoreBucket, error) {
	buckets := []ScoreBucket{
		{Bucket: "0-20"}, {Bucket: "20-40"}, {Bucket: "40-60"},
		{Bucket: "60-80"}, {Bucket: "80-100"}, {Bucket: "unknown"},
	}
	rows, err := d.Query(d.bind(`SELECT
		CASE
			WHEN t.entry_score IS NULL THEN 'unknown'
			WHEN t.entry_score < 20 THEN '0-20'
			WHEN t.entry_score < 40 THEN '20-40'
			WHEN t.entry_score < 60 THEN '40-60'
			WHEN t.entry_score < 80 THEN '60-80'
			ELSE '80-100'
		END AS bucket,
		COUNT(*),
		COALESCE(SUM(CASE WHEN t.realized_pnl_sol>0 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN t.realized_pnl_sol<0 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(t.realized_pnl_sol),0),
		COALESCE(SUM(t.fees_sol),0)
		FROM trades t
		WHERE t.side='sell' AND t.state IN ('simulated','confirmed','finalized')
		AND t.realized_pnl_sol IS NOT NULL
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
		GROUP BY bucket`), nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	index := make(map[string]int, len(buckets))
	for i, b := range buckets {
		index[b.Bucket] = i
	}
	for rows.Next() {
		var bucket string
		var trades, winners, losers int
		var netPnl, fees float64
		if err := rows.Scan(&bucket, &trades, &winners, &losers, &netPnl, &fees); err != nil {
			return nil, err
		}
		i, ok := index[bucket]
		if !ok {
			continue
		}
		buckets[i].Trades = trades
		buckets[i].Winners = winners
		buckets[i].Losers = losers
		buckets[i].NetPnlSOL = netPnl
		buckets[i].FeesSOL = fees
		if trades > 0 {
			buckets[i].WinRatePct = float64(winners) * 100 / float64(trades)
		}
		if simulation {
			buckets[i].NetPnlSOL -= fees
		} else {
			buckets[i].NetPnlSOL += fees
		}
	}
	return buckets, rows.Err()
}

func (d *DB) RiskAssessmentCount() (int, error) {
	var count int
	err := d.QueryRow(`SELECT COUNT(*) FROM risk_assessments`).Scan(&count)
	return count, err
}
func (d *DB) TradePeriodStats(start, end time.Time) (total, positive, negative int, err error) {
	err = d.QueryRow(d.bind(`SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN realized_pnl_sol>0 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN realized_pnl_sol<0 THEN 1 ELSE 0 END),0)
		FROM trades t WHERE t.side='sell' AND t.state IN ('simulated','confirmed','finalized')
		AND t.realized_pnl_sol IS NOT NULL
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
		AND t.confirmed_at>=? AND t.confirmed_at<?`), start, end).Scan(&total, &positive, &negative)
	return
}

func (d *DB) SellActivityStats(start, end time.Time) (attempts, failed int, err error) {
	err = d.QueryRow(d.bind(`SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN state='failed' THEN 1 ELSE 0 END),0)
		FROM trades WHERE side='sell'
		AND COALESCE(confirmed_at,created_at)>=? AND COALESCE(confirmed_at,created_at)<?`),
		start, end).Scan(&attempts, &failed)
	if err != nil {
		return 0, 0, fmt.Errorf("count sell attempts for report period: %w", err)
	}
	return attempts, failed, nil
}

func (d *DB) RealizedPnlHistory() (*sql.Rows, error) {
	return d.Query(`SELECT CAST(COALESCE(t.confirmed_at,t.created_at) AS TEXT),t.realized_pnl_sol
		FROM trades t WHERE t.side='sell' AND t.state IN ('simulated','confirmed','finalized')
		AND t.realized_pnl_sol IS NOT NULL
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')
		ORDER BY COALESCE(t.confirmed_at,t.created_at) ASC`)
}
func (d *DB) RealizedPnlSOL() (float64, error) {
	var pnl float64
	e := d.QueryRow(`SELECT COALESCE(SUM(t.realized_pnl_sol),0) FROM trades t
		WHERE t.side='sell' AND t.state IN ('simulated','confirmed','finalized')
		AND t.realized_pnl_sol IS NOT NULL
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')`).Scan(&pnl)
	return pnl, e
}

func (d *DB) SimulationAvailableBalanceSOL(initialBalance float64) (float64, error) {
	var balance float64
	err := d.QueryRow(d.bind(`SELECT ? + COALESCE((SELECT SUM(t.realized_pnl_sol) FROM trades t
		WHERE t.state='simulated' AND t.side='sell' AND t.realized_pnl_sol IS NOT NULL
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')),0)
		- COALESCE((SELECT SUM(fees_sol) FROM trades WHERE state='simulated'),0)
		- COALESCE((SELECT SUM(cost_sol) FROM positions WHERE status='open'),0)
		- COALESCE((SELECT SUM(p.cost_sol) FROM positions p WHERE p.status='closed'
			AND (SELECT t.state FROM trades t WHERE t.mint=p.mint AND t.side='sell'
				ORDER BY t.created_at DESC,t.id DESC LIMIT 1)='failed'),0)`), initialBalance).Scan(&balance)
	return balance, err
}

func (d *DB) ListPositions() (*sql.Rows, error) {
	return d.Query(`SELECT p.mint,p.quantity,COALESCE(p.cost_sol,0),p.status,p.opened_at,p.updated_at,
		COALESCE(m.entry_market_cap_sol,0),COALESCE(m.current_market_cap_sol,0)
		FROM positions p LEFT JOIN paper_marks m ON m.mint=p.mint ORDER BY p.updated_at DESC`)
}
func (d *DB) ListPositionsForMode(simulation bool) (*sql.Rows, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	query := `SELECT p.mint,p.quantity,COALESCE(p.cost_sol,0),p.status,p.opened_at,p.updated_at,
		COALESCE(m.entry_market_cap_sol,0),COALESCE(m.current_market_cap_sol,0),COALESCE(CAST(m.updated_at AS TEXT),''),
		COALESCE(m.price_source,'unverified'),COALESCE(m.entry_token_price_sol,0),COALESCE(m.current_token_price_sol,0),
		COALESCE((SELECT SUM(t.fees_sol) FROM trades t WHERE t.mint=p.mint
			AND t.state IN ('simulated','confirmed','finalized') AND t.fees_sol IS NOT NULL),0),
		(SELECT SUM(t.realized_pnl_sol) FROM trades t WHERE t.mint=p.mint AND t.side='sell'
			AND t.state IN ('simulated','confirmed','finalized') AND t.realized_pnl_sol IS NOT NULL
			AND p.status='closed'),
		COALESCE((SELECT t.state FROM trades t WHERE t.mint=p.mint AND t.side='sell'
			ORDER BY t.created_at DESC LIMIT 1),'')
		FROM positions p LEFT JOIN ` + marks + ` m ON m.mint=p.mint ORDER BY p.updated_at DESC`
	return d.Query(query)
}
func (d *DB) OpenPositionMints() ([]string, error) {
	rows, err := d.Query(`SELECT mint FROM positions WHERE status='open'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mints := make([]string, 0)
	for rows.Next() {
		var mint string
		if err := rows.Scan(&mint); err != nil {
			return nil, err
		}
		mints = append(mints, mint)
	}
	return mints, rows.Err()
}
func (d *DB) OpenPositionMintsWithStaleMarks(simulation bool, staleBefore time.Time) ([]string, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	rows, err := d.Query(d.bind(`SELECT p.mint FROM positions p
		LEFT JOIN `+marks+` m ON m.mint=p.mint
		WHERE p.status='open' AND (m.mint IS NULL OR m.price_source<>? OR m.updated_at<=?)
		ORDER BY p.updated_at`), VerifiedPositionPriceSource, staleBefore)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mints := make([]string, 0)
	for rows.Next() {
		var mint string
		if err := rows.Scan(&mint); err != nil {
			return nil, err
		}
		mints = append(mints, mint)
	}
	return mints, rows.Err()
}
func (d *DB) OpenPositionCount() (int, error) {
	var n int
	e := d.QueryRow(`SELECT COUNT(*) FROM positions WHERE status='open'`).Scan(&n)
	return n, e
}
func (d *DB) PendingBuyCount() (int, error) {
	var n int
	e := d.QueryRow(`SELECT COUNT(*) FROM trades WHERE side='buy' AND state IN ('prepared','signed','submitted','pending','unknown')`).Scan(&n)
	return n, e
}
func (d *DB) PendingBuyExposureSOL() (float64, error) {
	var n sql.NullFloat64
	e := d.QueryRow(`SELECT COALESCE(SUM(requested_value_sol),0) FROM trades WHERE side='buy' AND state IN ('prepared','signed','submitted','pending','unknown')`).Scan(&n)
	return n.Float64, e
}
func (d *DB) PositionOpenedAt(mint string) (time.Time, bool, error) {
	var opened time.Time
	e := d.QueryRow(d.bind(`SELECT opened_at FROM positions WHERE mint=? AND status='open'`), mint).Scan(&opened)
	if e == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	return opened, e == nil, e
}

func (d *DB) UnknownTradeCount() (int, error) {
	var n int
	query := `SELECT COUNT(*) FROM trades WHERE side='buy' AND (state='unknown' OR reconciliation_needed=1)`
	if d.postgres {
		query = `SELECT COUNT(*) FROM trades WHERE side='buy' AND (state='unknown' OR reconciliation_needed=TRUE)`
	}
	e := d.QueryRow(query).Scan(&n)
	return n, e
}

func (d *DB) PurchasesBlocked() (string, bool, error) {
	var id string
	query := `SELECT id FROM trades WHERE side='buy' AND (state='unknown' OR reconciliation_needed=1) ORDER BY created_at ASC LIMIT 1`
	if d.postgres {
		query = `SELECT id FROM trades WHERE side='buy' AND (state='unknown' OR reconciliation_needed=TRUE) ORDER BY created_at ASC LIMIT 1`
	}
	err := d.QueryRow(query).Scan(&id)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

func (d *DB) OpenExposureSOL() (float64, error) {
	var n sql.NullFloat64
	e := d.QueryRow(`SELECT COALESCE(SUM(cost_sol),0) FROM positions WHERE status='open'`).Scan(&n)
	return n.Float64, e
}
func (d *DB) DailyRealizedLossSOL(simulationMode ...bool) (float64, error) {
	var n sql.NullFloat64
	now := time.Now().UTC()
	utcDayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if len(simulationMode) > 0 && simulationMode[0] {
		var gross, fees float64
		if err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(t.realized_pnl_sol),0) FROM trades t
			WHERE t.state='simulated' AND t.side='sell' AND t.realized_pnl_sol IS NOT NULL
			AND t.confirmed_at>=? AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')`), utcDayStart).Scan(&gross); err != nil {
			return 0, err
		}
		if err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(fees_sol),0) FROM trades
			WHERE state='simulated' AND confirmed_at>=?`), utcDayStart).Scan(&fees); err != nil {
			return 0, err
		}
		if net := gross - fees; net < 0 {
			return -net, nil
		}
		return 0, nil
	}
	e := d.QueryRow(d.bind(`SELECT COALESCE(SUM(CASE WHEN t.realized_pnl_sol < 0 THEN -t.realized_pnl_sol ELSE 0 END),0)
		FROM trades t WHERE t.side='sell' AND t.state IN ('confirmed','finalized')
		AND t.realized_pnl_sol IS NOT NULL AND t.confirmed_at>=?
		AND EXISTS (SELECT 1 FROM positions p WHERE p.mint=t.mint AND p.status='closed')`), utcDayStart).Scan(&n)
	return n.Float64, e
}

func (d *DB) ReconciliationRequired() (bool, error) {
	n, e := d.UnknownTradeCount()
	return n > 0, e
}

func (d *DB) UnreconciledTrades() (*sql.Rows, error) {
	query := `SELECT id,COALESCE(signature,''),mint,side FROM trades WHERE reconciliation_needed=1 ORDER BY created_at ASC`
	if d.postgres {
		query = `SELECT id,COALESCE(signature,''),mint,side FROM trades WHERE reconciliation_needed=TRUE ORDER BY created_at ASC`
	}
	return d.Query(query)
}
func (d *DB) UnknownTrades() (*sql.Rows, error) {
	return d.Query(`SELECT id,COALESCE(signature,''),mint,side FROM trades WHERE state='unknown' AND COALESCE(signature,'')<>'' ORDER BY created_at ASC`)
}
func (d *DB) LogOperational(level, eventType, message string, payload string) error {
	_, e := d.Exec(d.bind(`INSERT INTO operational_logs(level,event_type,message,payload_json,created_at) VALUES(?,?,?,?,?)`), level, eventType, message, payload, time.Now().UTC())
	return e
}
func (d *DB) SetSetting(k, v string) error {
	_, e := d.Exec(d.bind(`INSERT INTO settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`), k, v, time.Now().UTC())
	return e
}
func (d *DB) Settings() (*sql.Rows, error) {
	return d.Query(`SELECT key,value,updated_at FROM settings ORDER BY key`)
}

func (d *DB) bind(query string) string {
	if !d.postgres {
		return query
	}
	var out strings.Builder
	index := 1
	for _, char := range query {
		if char == '?' {
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(index))
			index++
			continue
		}
		out.WriteRune(char)
	}
	return out.String()
}