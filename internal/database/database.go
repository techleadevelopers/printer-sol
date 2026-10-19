package database

import (
	"database/sql"
	"embed"
	"fmt"
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

type DB struct {
	*sql.DB
	postgres bool
	// markMu serializes WebSocket and public-API updates to mark rows.
	markMu sync.Mutex
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
		realizedPnl = 0.0
	}
	tx, e := d.Begin()
	if e != nil {
		return e
	}
	if _, e = tx.Exec(d.bind(`INSERT INTO trades(id,mint,side,quantity,requested_value_sol,fees_sol,realized_pnl_sol,slippage_pct,state,entry_reason,exit_reason,reconciliation_needed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), t.ID, t.Mint, t.Side, t.Quantity, t.Requested, t.Fees, realizedPnl, t.Slippage, t.State, t.EntryReason, exitReason, true, now, now); e == nil {
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
func (d *DB) HasPurchaseForMint(mint string) (bool, error) {
	var n int
	// A mint receives at most one buy attempt. This remains true even if the
	// attempt failed or its outcome is unknown, preventing accidental averaging
	// and a second entry after an RPC/PumpPortal failure.
	e := d.QueryRow(d.bind(`SELECT COUNT(*) FROM trades WHERE mint=? AND side='buy'`), mint).Scan(&n)
	return n > 0, e
}
func (d *DB) HasFailedSellForMint(mint string) (bool, error) {
	var n int
	e := d.QueryRow(d.bind(`SELECT COUNT(*) FROM trades WHERE mint=? AND side='sell' AND state='failed'`), mint).Scan(&n)
	return n > 0, e
}

// FailedSellRetryState makes the retry count and cooldown survive process
// restarts by deriving them from persisted, definitively failed sell attempts.
func (d *DB) FailedSellRetryState(mint string) (int, time.Time, error) {
	var attempts int
	var lastFailure sql.NullTime
	err := d.QueryRow(d.bind(`SELECT COUNT(*),MAX(updated_at) FROM trades
		WHERE mint=? AND side='sell' AND state='failed'`), mint).Scan(&attempts, &lastFailure)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("read failed sell retry state for mint %q: %w", mint, err)
	}
	if !lastFailure.Valid {
		return attempts, time.Time{}, nil
	}
	return attempts, lastFailure.Time.UTC(), nil
}

type SimulationSellSnapshot struct {
	TradeState       string
	TransactionState string
	PositionState    string
}

// SimulationSellSnapshot reads the records that must change together when a
// paper sell is finalized.
func (d *DB) SimulationSellSnapshot(id, mint string) (SimulationSellSnapshot, error) {
	var snapshot SimulationSellSnapshot
	err := d.QueryRow(d.bind(`SELECT t.state,COALESCE(x.state,''),COALESCE(p.status,'')
		FROM trades t
		LEFT JOIN transactions x ON x.trade_id=t.id
		LEFT JOIN positions p ON p.mint=t.mint
		WHERE t.id=? AND t.mint=? AND t.side='sell'`), id, mint).Scan(
		&snapshot.TradeState, &snapshot.TransactionState, &snapshot.PositionState)
	if err != nil {
		return snapshot, fmt.Errorf("read simulated sell snapshot id=%q mint=%q: %w", id, mint, err)
	}
	return snapshot, nil
}

// MarkSimulationSellUnknown keeps an unresolved local exit active so the
// monitor cannot retry it until persisted state has been reconciled.
func (d *DB) MarkSimulationSellUnknown(id string) error {
	now := time.Now().UTC()
	tx, err := d.Begin()
	if err != nil {
		return fmt.Errorf("begin unknown simulated sell update for %q: %w", id, err)
	}
	defer tx.Rollback()

	result, err := tx.Exec(d.bind(`UPDATE trades SET state='unknown',reconciliation_needed=?,updated_at=?
		WHERE id=? AND side='sell' AND state IN ('prepared','unknown')`), true, now, id)
	if err != nil {
		return fmt.Errorf("mark simulated sell %q unknown: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check unknown state for simulated sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("simulated sell %q is not in a markable state", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='unknown',updated_at=?
		WHERE trade_id=? AND state IN ('prepared','unknown')`), now, id)
	if err != nil {
		return fmt.Errorf("mark transaction for simulated sell %q unknown: %w", id, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check unknown transaction state for simulated sell %q: %w", id, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("transaction for simulated sell %q is not in a markable state", id)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit unknown state for simulated sell %q: %w", id, err)
	}
	return nil
}

// ReconcileStaleSimulationSells releases old active paper sells only when the
// position is still open and the transaction never reached a finalized state.
// Paper exits have no external submission side effect, so this is definitive.
func (d *DB) ReconcileStaleSimulationSells(mint string, olderThan time.Time) (int, error) {
	tx, err := d.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin stale simulated sell reconciliation for mint %q: %w", mint, err)
	}
	defer tx.Rollback()

	rows, err := tx.Query(d.bind(`SELECT t.id,t.state,COALESCE(x.state,''),COALESCE(p.status,''),t.updated_at
		FROM trades t
		LEFT JOIN transactions x ON x.trade_id=t.id
		LEFT JOIN positions p ON p.mint=t.mint
		WHERE t.mint=? AND t.side='sell' AND t.state IN ('prepared','unknown')
		ORDER BY t.created_at`), mint)
	if err != nil {
		return 0, fmt.Errorf("find stale simulated sells for mint %q: %w", mint, err)
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
			return 0, fmt.Errorf("read stale simulated sell for mint %q: %w", mint, err)
		}
		candidates = append(candidates, candidate)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate stale simulated sells for mint %q: %w", mint, err)
	}
	if err = rows.Close(); err != nil {
		return 0, fmt.Errorf("close stale simulated sell scan for mint %q: %w", mint, err)
	}

	reconciled := 0
	now := time.Now().UTC()
	for _, candidate := range candidates {
		if candidate.updatedAt.After(olderThan) {
			continue
		}
		if candidate.positionState != "open" ||
			(candidate.tradeState != "prepared" && candidate.tradeState != "unknown") ||
			candidate.transactionState != candidate.tradeState {
			return 0, fmt.Errorf("stale simulated sell id=%q mint=%q has inconsistent persisted states trade=%q transaction=%q position=%q",
				candidate.id, mint, candidate.tradeState, candidate.transactionState, candidate.positionState)
		}
		result, updateErr := tx.Exec(d.bind(`UPDATE trades SET state='failed',reconciliation_needed=?,updated_at=?
			WHERE id=? AND side='sell' AND state=?`), false, now, candidate.id, candidate.tradeState)
		if updateErr != nil {
			return 0, fmt.Errorf("recover stale simulated sell id=%q mint=%q: %w", candidate.id, mint, updateErr)
		}
		if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
			return 0, fmt.Errorf("check stale simulated sell id=%q mint=%q: %w", candidate.id, mint, rowsErr)
		} else if affected != 1 {
			return 0, fmt.Errorf("stale simulated sell id=%q mint=%q changed during reconciliation", candidate.id, mint)
		}
		result, updateErr = tx.Exec(d.bind(`UPDATE transactions SET state='failed',updated_at=?
			WHERE trade_id=? AND state=?`), now, candidate.id, candidate.transactionState)
		if updateErr != nil {
			return 0, fmt.Errorf("recover transaction for stale simulated sell id=%q mint=%q: %w", candidate.id, mint, updateErr)
		}
		if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
			return 0, fmt.Errorf("check transaction recovery for simulated sell id=%q mint=%q: %w", candidate.id, mint, rowsErr)
		} else if affected != 1 {
			return 0, fmt.Errorf("transaction for stale simulated sell id=%q mint=%q changed during reconciliation", candidate.id, mint)
		}
		reconciled++
	}
	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit stale simulated sell reconciliation for mint %q: %w", mint, err)
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
func (d *DB) ListTrades() (rows *sql.Rows, err error) {
	return d.Query(`SELECT id,mint,side,quantity,requested_value_sol,COALESCE(executed_value_sol,0),
		COALESCE(fees_sol,0),slippage_pct,COALESCE(signature,''),state,created_at,realized_pnl_sol,
		COALESCE(exit_reason,'') FROM trades ORDER BY created_at DESC LIMIT 200`)
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
	// The console shows persisted detections and their latest real assessment
	// and operation state; it does not infer that an approved token was bought.
	return d.Query(d.bind(`SELECT e.mint,COALESCE(e.creator,''),e.received_at,e.latency_ms
		,r.approved,COALESCE(r.reason,''),COALESCE(t.side,''),COALESCE(t.state,''),COALESCE(p.status,'')
		FROM token_events e
		LEFT JOIN risk_assessments r ON r.id=(SELECT MAX(r2.id) FROM risk_assessments r2 WHERE r2.mint=e.mint)
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
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,updated_at=excluded.updated_at`),
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
	result, err = tx.Exec(d.bind(`UPDATE trades SET state='simulated',executed_value_sol=COALESCE(?,0.0),fees_sol=COALESCE(?,0.0),realized_pnl_sol=COALESCE(?,0.0),exit_reason=COALESCE(NULLIF(TRIM(exit_reason),''),?),confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=? AND side='sell'`),
		value, fees, realizedPnl, exitReason, now, false, now, id)
	if err != nil {
		return fmt.Errorf("finalize trade row for simulated sell id=%q mint=%q: %w", id, mint, err)
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return fmt.Errorf("check trade finalization for simulated sell id=%q mint=%q: %w", id, mint, rowsErr)
	} else if affected != 1 {
		return fmt.Errorf("simulated sell trade %q not found", id)
	}
	result, err = tx.Exec(d.bind(`UPDATE transactions SET state='simulated',updated_at=? WHERE trade_id=?`), now, id)
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

func (d *DB) FinalizeLiveBuy(id, mint, state, signature string, cost, fees float64) error {
	if cost <= 0 {
		return fmt.Errorf("invalid confirmed live buy cost")
	}
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	if _, err = tx.Exec(d.bind(`INSERT INTO positions(mint,quantity,cost_sol,status,opened_at,updated_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,updated_at=excluded.updated_at`),
		mint, fmt.Sprint(cost), cost, "open", now, now); err != nil {
		return err
	}
	if err = updateExecutedTrade(tx, d, id, state, signature, cost, fees, nil, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) FinalizeLiveSell(id, mint, state, signature string, value, fees float64) error {
	if value < 0 {
		return fmt.Errorf("invalid confirmed live sell value")
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
	var exitReason string
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(exit_reason,'') FROM trades WHERE id=? AND side='sell'`), id).Scan(&exitReason); err != nil {
		return err
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
	if err = updateExecutedTrade(tx, d, id, state, signature, value, fees, value-cost, now); err != nil {
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

// IsCreatorBlacklisted reports whether this creator has an active 24-hour
// block from a closed stop-loss. Expired rows remain as an audit trail.
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

func updateExecutedTrade(tx *sql.Tx, d *DB, id, state, signature string, executed, fees float64, realized any, now time.Time) error {
	result, err := tx.Exec(d.bind(`UPDATE trades SET state=?,signature=COALESCE(NULLIF(?,''),signature),executed_value_sol=?,fees_sol=?,realized_pnl_sol=?,confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=?`),
		state, signature, executed, fees, realized, now, false, now, id)
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
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,updated_at=excluded.updated_at`
	_, e := d.Exec(d.bind(query), mint, fmt.Sprint(amount), amount, "open", now, now)
	return e
}
func (d *DB) RecordPaperMark(mint string, marketCapSOL float64) error {
	return d.recordMark("paper_marks", mint, marketCapSOL)
}
func (d *DB) RecordLiveMark(mint string, marketCapSOL float64) error {
	return d.recordMark("live_marks", mint, marketCapSOL)
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
	// Current follows real price movement (including declines); peak is
	// monotonic so an older/lower mark can never erase the trailing-stop high.
	_, e := d.Exec(d.bind(`UPDATE `+table+` SET current_market_cap_sol=?,peak_market_cap_sol=CASE WHEN ?>peak_market_cap_sol THEN ? ELSE peak_market_cap_sol END,updated_at=? WHERE mint=?`), marketCapSOL, marketCapSOL, marketCapSOL, time.Now().UTC(), mint)
	return e
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

// SetTradeExecution persists values derived from the confirmed on-chain
// transaction. It deliberately does not use a market-cap estimate for live P&L.
func (d *DB) SetTradeExecution(id string, executed, fees, pnl float64) error {
	_, e := d.Exec(d.bind(`UPDATE trades SET executed_value_sol=?,fees_sol=?,realized_pnl_sol=?,updated_at=? WHERE id=?`), executed, fees, pnl, time.Now().UTC(), id)
	return e
}
func (d *DB) PaperOpenPnlSOL() (float64, error) {
	return d.OpenPnlSOL(true)
}
func (d *DB) OpenPnlSOL(simulation bool) (float64, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	var pnl float64
	query := `SELECT COALESCE(SUM(p.cost_sol * (m.current_market_cap_sol - m.entry_market_cap_sol) / NULLIF(m.entry_market_cap_sol,0)),0) FROM positions p JOIN ` + marks + ` m ON m.mint=p.mint WHERE p.status='open' AND p.cost_sol>0`
	e := d.QueryRow(query).Scan(&pnl)
	return pnl, e
}
func (d *DB) UnmarkedOpenPositionCount(simulation bool) (int, error) {
	marks := "live_marks"
	if simulation {
		marks = "paper_marks"
	}
	var count int
	query := `SELECT COUNT(*) FROM positions p LEFT JOIN ` + marks + ` m ON m.mint=p.mint
		WHERE p.status='open' AND (COALESCE(p.cost_sol,0)<=0 OR m.mint IS NULL OR m.entry_market_cap_sol<=0 OR m.current_market_cap_sol<=0)`
	err := d.QueryRow(query).Scan(&count)
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
	ClosedTrades, Winners, Losers                                                                      int
	WinRatePct, AvgWinSOL, AvgLossSOL, AvgHoldSeconds, FeesSOL, GrossRealizedPnlSOL, NetRealizedPnlSOL float64
}

func (d *DB) Performance(simulationMode ...bool) (PerformanceStats, error) {
	simulation := len(simulationMode) > 0 && simulationMode[0]
	var stats PerformanceStats
	err := d.QueryRow(`SELECT
		COUNT(*),
		COALESCE(SUM(CASE WHEN realized_pnl_sol>0 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN realized_pnl_sol<0 THEN 1 ELSE 0 END),0),
		COALESCE(AVG(CASE WHEN realized_pnl_sol>0 THEN realized_pnl_sol END),0),
		COALESCE(AVG(CASE WHEN realized_pnl_sol<0 THEN realized_pnl_sol END),0),
		COALESCE(SUM(realized_pnl_sol),0)
		FROM trades WHERE side='sell' AND realized_pnl_sol IS NOT NULL`).Scan(
		&stats.ClosedTrades, &stats.Winners, &stats.Losers, &stats.AvgWinSOL, &stats.AvgLossSOL, &stats.GrossRealizedPnlSOL)
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
	holdQuery := `SELECT COALESCE(AVG((julianday(updated_at)-julianday(opened_at))*86400),0) FROM positions WHERE status='closed'`
	if d.postgres {
		holdQuery = `SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (updated_at-opened_at))),0) FROM positions WHERE status='closed'`
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
		// Confirmed live realized P&L already uses wallet deltas net of fees.
		stats.GrossRealizedPnlSOL += stats.FeesSOL
	}
	return stats, nil
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
		FROM trades WHERE side='sell' AND realized_pnl_sol IS NOT NULL AND confirmed_at>=? AND confirmed_at<?`), start, end).Scan(&total, &positive, &negative)
	return
}
func (d *DB) RealizedPnlHistory() (*sql.Rows, error) {
	return d.Query(`SELECT CAST(COALESCE(confirmed_at,created_at) AS TEXT),realized_pnl_sol
		FROM trades WHERE realized_pnl_sol IS NOT NULL
		ORDER BY COALESCE(confirmed_at,created_at) ASC`)
}
func (d *DB) RealizedPnlSOL() (float64, error) {
	var pnl float64
	e := d.QueryRow(`SELECT COALESCE(SUM(realized_pnl_sol),0) FROM trades WHERE realized_pnl_sol IS NOT NULL`).Scan(&pnl)
	return pnl, e
}

// SimulationAvailableBalanceSOL returns the uninvested paper capital after
// realized results and the cost of currently open paper positions.
func (d *DB) SimulationAvailableBalanceSOL(initialBalance float64) (float64, error) {
	var balance float64
	err := d.QueryRow(d.bind(`SELECT ? + COALESCE((SELECT SUM(realized_pnl_sol) FROM trades WHERE state='simulated' AND realized_pnl_sol IS NOT NULL),0)
		- COALESCE((SELECT SUM(fees_sol) FROM trades WHERE state='simulated'),0)
		- COALESCE((SELECT SUM(cost_sol) FROM positions WHERE status='open'),0)`), initialBalance).Scan(&balance)
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
		COALESCE((SELECT SUM(t.fees_sol) FROM trades t WHERE t.mint=p.mint
			AND t.state IN ('simulated','confirmed','finalized') AND t.fees_sol IS NOT NULL),0),
		(SELECT SUM(t.realized_pnl_sol) FROM trades t WHERE t.mint=p.mint AND t.side='sell'
			AND t.state IN ('simulated','confirmed','finalized') AND t.realized_pnl_sol IS NOT NULL),
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
	query := `SELECT COUNT(*) FROM trades WHERE state='unknown' OR reconciliation_needed=1`
	if d.postgres {
		query = `SELECT COUNT(*) FROM trades WHERE state='unknown' OR reconciliation_needed=TRUE`
	}
	e := d.QueryRow(query).Scan(&n)
	return n, e
}
func (d *DB) OpenExposureSOL() (float64, error) {
	var n sql.NullFloat64
	e := d.QueryRow(`SELECT COALESCE(SUM(cost_sol),0) FROM positions WHERE status='open'`).Scan(&n)
	return n.Float64, e
}
func (d *DB) DailyRealizedLossSOL(simulationMode ...bool) (float64, error) {
	var n sql.NullFloat64
	now := time.Now().UTC()
	// Risk limits reset at UTC midnight (calendar day), not on a rolling 24-hour window.
	utcDayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if len(simulationMode) > 0 && simulationMode[0] {
		var gross, fees float64
		if err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(realized_pnl_sol),0) FROM trades
			WHERE state='simulated' AND side='sell' AND confirmed_at>=?`), utcDayStart).Scan(&gross); err != nil {
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
	e := d.QueryRow(d.bind(`SELECT COALESCE(SUM(CASE WHEN realized_pnl_sol < 0 THEN -realized_pnl_sol ELSE 0 END),0) FROM trades WHERE confirmed_at >= ?`), utcDayStart).Scan(&n)
	return n.Float64, e
}
func (d *DB) ReconciliationRequired() (bool, error) { n, e := d.UnknownTradeCount(); return n > 0, e }
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
