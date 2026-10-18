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
	"time"
)

//go:embed migrations/*.sql migrations/postgres/*.sql
var migrations embed.FS

type DB struct {
	*sql.DB
	postgres bool
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
	return &DB{d, postgres}, d.Ping()
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
			_, e = tx.Exec(d.bind("INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)"), n, appliedAt)
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
	tx, e := d.Begin()
	if e != nil {
		return e
	}
	if _, e = tx.Exec(d.bind(`INSERT INTO trades(id,mint,side,quantity,requested_value_sol,slippage_pct,state,entry_reason,exit_reason,reconciliation_needed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`), t.ID, t.Mint, t.Side, t.Quantity, t.Requested, t.Slippage, t.State, t.EntryReason, t.ExitReason, true, now, now); e == nil {
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
		return e
	}
	if _, e = tx.Exec(d.bind(`UPDATE trades SET state=?, signature=COALESCE(NULLIF(?,''),signature), submitted_at=CASE WHEN ? IN ('submitted','pending') THEN ? ELSE submitted_at END, confirmed_at=CASE WHEN ? THEN ? ELSE confirmed_at END, reconciliation_needed=?, updated_at=? WHERE id=?`), state, sig, state, now, confirmed, now, !confirmed, now, id); e == nil {
		_, e = tx.Exec(d.bind(`UPDATE transactions SET state=?,signature=COALESCE(NULLIF(?,''),signature),updated_at=? WHERE trade_id=?`), state, sig, now, id)
	}
	if e != nil {
		_ = tx.Rollback()
		return e
	}
	return tx.Commit()
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
	return d.Query(`SELECT id,mint,side,quantity,requested_value_sol,COALESCE(executed_value_sol,0),COALESCE(fees_sol,0),slippage_pct,COALESCE(signature,''),state,created_at,realized_pnl_sol FROM trades ORDER BY created_at DESC LIMIT 200`)
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
	// The portal is a high-volume discovery feed. The console intentionally
	// exposes only candidates the strategy approved, never a mock or the full
	// Pump.fun firehose.
	return d.Query(d.bind(`SELECT e.mint,COALESCE(e.creator,''),e.received_at,e.latency_ms
		FROM token_events e
		WHERE EXISTS (SELECT 1 FROM risk_assessments r WHERE r.mint=e.mint AND r.id=(SELECT MAX(r2.id) FROM risk_assessments r2 WHERE r2.mint=e.mint) AND r.approved=?)
		ORDER BY e.id DESC LIMIT 200`), true)
}
func (d *DB) OpenSimulationPosition(mint string, amount float64) error {
	return d.OpenPosition(mint, amount)
}

func (d *DB) FinalizeSimulationBuy(id, mint string, amount float64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	if _, err = tx.Exec(d.bind(`INSERT INTO positions(mint,quantity,cost_sol,status,opened_at,updated_at) VALUES(?,?,?,?,?,?)
		ON CONFLICT(mint) DO UPDATE SET quantity=excluded.quantity,cost_sol=excluded.cost_sol,status=excluded.status,updated_at=excluded.updated_at`),
		mint, fmt.Sprint(amount), amount, "open", now, now); err != nil {
		return err
	}
	if err = setSimulationTradeState(tx, d, id, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) FinalizeSimulationSell(id, mint string, value float64) error {
	tx, err := d.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var cost float64
	if err = tx.QueryRow(d.bind(`SELECT COALESCE(cost_sol,0) FROM positions WHERE mint=? AND status='open'`), mint).Scan(&cost); err != nil {
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
		return fmt.Errorf("simulated position %q is no longer open", mint)
	}
	result, err = tx.Exec(d.bind(`UPDATE trades SET state='simulated',executed_value_sol=?,realized_pnl_sol=?,confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=? AND side='sell'`),
		value, value-cost, now, false, now, id)
	if err != nil {
		return err
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil {
		return rowsErr
	} else if affected != 1 {
		return fmt.Errorf("simulated sell trade %q not found", id)
	}
	if _, err = tx.Exec(d.bind(`UPDATE transactions SET state='simulated',updated_at=? WHERE trade_id=?`), now, id); err != nil {
		return err
	}
	return tx.Commit()
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
	return tx.Commit()
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

func setSimulationTradeState(tx *sql.Tx, d *DB, id string, now time.Time) error {
	result, err := tx.Exec(d.bind(`UPDATE trades SET state='simulated',confirmed_at=?,reconciliation_needed=?,updated_at=? WHERE id=? AND side='buy'`), now, false, now, id)
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
	now := time.Now().UTC()
	_, e := d.Exec(d.bind(`INSERT INTO `+table+`(mint,entry_market_cap_sol,current_market_cap_sol,updated_at) VALUES(?,?,?,?) ON CONFLICT(mint) DO NOTHING`), mint, marketCapSOL, marketCapSOL, now)
	return e
}
func (d *DB) UpdatePaperMark(mint string, marketCapSOL float64) error {
	return d.updateMark("paper_marks", mint, marketCapSOL)
}
func (d *DB) UpdateLiveMark(mint string, marketCapSOL float64) error {
	return d.updateMark("live_marks", mint, marketCapSOL)
}
func (d *DB) updateMark(table, mint string, marketCapSOL float64) error {
	if marketCapSOL <= 0 {
		return nil
	}
	_, e := d.Exec(d.bind(`UPDATE `+table+` SET current_market_cap_sol=?,updated_at=? WHERE mint=?`), marketCapSOL, time.Now().UTC(), mint)
	return e
}
func (d *DB) PaperReturnPct(mint string) (float64, bool, error) {
	return d.markReturnPct("paper_marks", mint)
}
func (d *DB) LiveReturnPct(mint string) (float64, bool, error) {
	return d.markReturnPct("live_marks", mint)
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
	var pnl float64
	e := d.QueryRow(`SELECT COALESCE(SUM(p.cost_sol * (m.current_market_cap_sol - m.entry_market_cap_sol) / NULLIF(m.entry_market_cap_sol,0)),0) FROM positions p JOIN paper_marks m ON m.mint=p.mint WHERE p.status='open'`).Scan(&pnl)
	return pnl, e
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
	err := d.QueryRow(d.bind(`SELECT ? + COALESCE((SELECT SUM(realized_pnl_sol) FROM trades WHERE realized_pnl_sol IS NOT NULL),0)
		- COALESCE((SELECT SUM(cost_sol) FROM positions WHERE status='open'),0)`), initialBalance).Scan(&balance)
	return balance, err
}

func (d *DB) ListPositions() (*sql.Rows, error) {
	return d.Query(`SELECT p.mint,p.quantity,COALESCE(p.cost_sol,0),p.status,p.opened_at,p.updated_at,COALESCE(m.entry_market_cap_sol,0),COALESCE(m.current_market_cap_sol,0) FROM positions p LEFT JOIN paper_marks m ON m.mint=p.mint ORDER BY p.updated_at DESC`)
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
func (d *DB) DailyRealizedLossSOL() (float64, error) {
	var n sql.NullFloat64
	e := d.QueryRow(d.bind(`SELECT COALESCE(SUM(CASE WHEN realized_pnl_sol < 0 THEN -realized_pnl_sol ELSE 0 END),0) FROM trades WHERE confirmed_at >= ?`), time.Now().UTC().Truncate(24*time.Hour)).Scan(&n)
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
