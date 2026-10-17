package database

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

//go:embed migrations/*.sql migrations/postgres/*.sql
var migrations embed.FS

type DB struct { *sql.DB; postgres bool }

func Open(target string) (*DB, error) {
	postgres := strings.HasPrefix(target, "postgres://") || strings.HasPrefix(target, "postgresql://")
	if !postgres {
	if err := os.MkdirAll(filepath.Dir(target), 0750); err != nil && filepath.Dir(target) != "." {
		return nil, err
	}
	}
	driver := "sqlite"
	if postgres { driver = "pgx" }
	d, e := sql.Open(driver, target)
	if e != nil {
		return nil, e
	}
	if !postgres { d.SetMaxOpenConns(1) }
	return &DB{d, postgres}, d.Ping()
}
func (d *DB) Migrate() error {
	pattern := "migrations/*.sql"
	if d.postgres { pattern = "migrations/postgres/*.sql" }
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
			if d.postgres { appliedAt = time.Now().UTC() }
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
	if d.postgres { query = `INSERT INTO token_events(mint,creator,received_at,raw_json,latency_ms) VALUES(?,?,?,?,?) ON CONFLICT (mint, received_at) DO NOTHING` }
	receivedAt := any(at.UTC().Format(time.RFC3339Nano))
	if d.postgres { receivedAt = at.UTC() }
	_, e := d.Exec(d.bind(query), mint, creator, receivedAt, raw, latency)
	return e
}
func (d *DB) ListTrades() (rows *sql.Rows, err error) {
	return d.Query(`SELECT id,mint,side,quantity,requested_value_sol,COALESCE(executed_value_sol,0),COALESCE(fees_sol,0),slippage_pct,COALESCE(signature,''),state FROM trades ORDER BY created_at DESC LIMIT 200`)
}
func (d *DB) RecordRPC(endpoint string, healthy bool, latency int64, status int, msg string) error {
	_, e := d.Exec(d.bind(`INSERT INTO rpc_health(endpoint,checked_at,latency_ms,healthy,http_status,error,supports_read) VALUES(?,?,?,?,?,?,?) ON CONFLICT(endpoint) DO UPDATE SET checked_at=excluded.checked_at,latency_ms=excluded.latency_ms,healthy=excluded.healthy,http_status=excluded.http_status,error=excluded.error,supports_read=excluded.supports_read`), endpoint, time.Now().UTC(), latency, healthy, status, strings.TrimSpace(msg), healthy)
	return e
}
func (d *DB) ListTokens() (*sql.Rows, error) {
	return d.Query(`SELECT mint,COALESCE(creator,''),received_at,latency_ms FROM token_events ORDER BY id DESC LIMIT 200`)
}
func (d *DB) ListPositions() (*sql.Rows, error) {
	return d.Query(`SELECT mint,quantity,COALESCE(cost_sol,0),status,opened_at,updated_at FROM positions ORDER BY updated_at DESC`)
}
func (d *DB) OpenPositionCount() (int, error) {
	var n int
	e := d.QueryRow(`SELECT COUNT(*) FROM positions WHERE status='open'`).Scan(&n)
	return n, e
}
func (d *DB) UnknownTradeCount() (int, error) {
	var n int
	query := `SELECT COUNT(*) FROM trades WHERE state='unknown' OR reconciliation_needed=1`
	if d.postgres { query = `SELECT COUNT(*) FROM trades WHERE state='unknown' OR reconciliation_needed=TRUE` }
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
	query := `SELECT id,COALESCE(signature,''),state FROM trades WHERE reconciliation_needed=1 ORDER BY created_at ASC`
	if d.postgres { query = `SELECT id,COALESCE(signature,''),state FROM trades WHERE reconciliation_needed=TRUE ORDER BY created_at ASC` }
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
	if !d.postgres { return query }
	var out strings.Builder
	index := 1
	for _, char := range query {
		if char == '?' { out.WriteByte('$'); out.WriteString(strconv.Itoa(index)); index++; continue }
		out.WriteRune(char)
	}
	return out.String()
}
