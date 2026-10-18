package database

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type SimulationRun struct {
	StartedAt       time.Time
	BalanceStartSOL float64
}

type SimulationTrade struct {
	ID          string    `json:"id"`
	Mint        string    `json:"mint"`
	Side        string    `json:"side"`
	EntrySOL    float64   `json:"entrySol"`
	ExitSOL     float64   `json:"exitSol"`
	GrossPnLSOL float64   `json:"grossPnlSol"`
	FeesSOL     float64   `json:"feesSol"`
	NetPnLSOL   float64   `json:"pnlSol"`
	HoldSeconds float64   `json:"holdSec"`
	ExitReason  string    `json:"exitReason"`
	ConfirmedAt time.Time `json:"confirmedAt"`
}

type SimulationSummary struct {
	StartedAt         time.Time      `json:"startedAt"`
	BalanceStartSOL   float64        `json:"balanceStartSol"`
	BalanceNowSOL     float64        `json:"balanceNowSol"`
	TotalTrades       int            `json:"totalTrades"`
	Wins              int            `json:"wins"`
	Losses            int            `json:"losses"`
	WinRate           float64        `json:"winRate"`
	AvgWinSOL         float64        `json:"avgWinSol"`
	AvgLossSOL        float64        `json:"avgLossSol"`
	GrossPnLSOL       float64        `json:"grossPnlSol"`
	FeesPaidSOL       float64        `json:"feesPaidSol"`
	NetPnLSOL         float64        `json:"netPnlSol"`
	AvgHoldSeconds    float64        `json:"avgHoldSeconds"`
	MaxDrawdownSOL    float64        `json:"maxDrawdownSol"`
	EVPerTradeSOL     float64        `json:"evPerTradeSol"`
	SharpeRatio       *float64       `json:"sharpeRatio"`
	ProfitFactor      *float64       `json:"profitFactor"`
	ExitReasons       map[string]int `json:"exitReasons"`
	LongestWinStreak  int            `json:"longestWinStreak"`
	LongestLossStreak int            `json:"longestLossStreak"`
}

func (d *DB) EnsureSimulationRun(balanceStartSOL float64) (SimulationRun, error) {
	if balanceStartSOL <= 0 || math.IsNaN(balanceStartSOL) || math.IsInf(balanceStartSOL, 0) {
		return SimulationRun{}, fmt.Errorf("simulation starting balance must be finite and positive")
	}
	now := time.Now().UTC()
	if _, err := d.Exec(d.bind(`INSERT INTO simulation_sessions(id,started_at,balance_start_sol) VALUES(1,?,?) ON CONFLICT(id) DO NOTHING`), now, balanceStartSOL); err != nil {
		return SimulationRun{}, err
	}
	return d.SimulationRun()
}

func (d *DB) SimulationRun() (SimulationRun, error) {
	var run SimulationRun
	err := d.QueryRow(`SELECT started_at,balance_start_sol FROM simulation_sessions WHERE id=1`).Scan(&run.StartedAt, &run.BalanceStartSOL)
	if err != nil {
		return run, err
	}
	run.StartedAt = run.StartedAt.UTC()
	return run, nil
}

func (d *DB) SimulationTrades(from, to time.Time) ([]SimulationTrade, error) {
	rows, err := d.Query(d.bind(`SELECT s.id,s.mint,
		COALESCE(b.executed_value_sol,b.requested_value_sol,0),
		COALESCE(s.executed_value_sol,0),
		COALESCE(s.realized_pnl_sol,0),
		COALESCE(b.fees_sol,0)+COALESCE(s.fees_sol,0),
		p.opened_at,p.updated_at,COALESCE(s.exit_reason,''),
		COALESCE(s.confirmed_at,s.created_at)
		FROM trades s
		JOIN positions p ON p.mint=s.mint AND p.status='closed'
		LEFT JOIN trades b ON b.id=(
			SELECT b2.id FROM trades b2
			WHERE b2.mint=s.mint AND b2.side='buy' AND b2.state='simulated'
			ORDER BY b2.confirmed_at DESC,b2.created_at DESC LIMIT 1
		)
		WHERE s.side='sell' AND s.state='simulated' AND s.realized_pnl_sol IS NOT NULL
		AND COALESCE(s.confirmed_at,s.created_at)>=? AND COALESCE(s.confirmed_at,s.created_at)<?
		ORDER BY COALESCE(s.confirmed_at,s.created_at),s.id`), from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	trades := make([]SimulationTrade, 0)
	for rows.Next() {
		var trade SimulationTrade
		var openedAt, closedAt time.Time
		if err = rows.Scan(&trade.ID, &trade.Mint, &trade.EntrySOL, &trade.ExitSOL, &trade.GrossPnLSOL, &trade.FeesSOL, &openedAt, &closedAt, &trade.ExitReason, &trade.ConfirmedAt); err != nil {
			return nil, err
		}
		trade.Side = "round_trip"
		trade.HoldSeconds = closedAt.Sub(openedAt).Seconds()
		if trade.HoldSeconds < 0 {
			trade.HoldSeconds = 0
		}
		trade.ConfirmedAt = trade.ConfirmedAt.UTC()
		trade.NetPnLSOL = trade.GrossPnLSOL - trade.FeesSOL
		trades = append(trades, trade)
	}
	return trades, rows.Err()
}

func (d *DB) SimulationFees(from, to time.Time) (float64, error) {
	var fees float64
	err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(fees_sol),0)
		FROM trades WHERE state='simulated' AND fees_sol IS NOT NULL
		AND COALESCE(confirmed_at,created_at)>=? AND COALESCE(confirmed_at,created_at)<?`), from, to).Scan(&fees)
	return fees, err
}

func BuildSimulationSummary(run SimulationRun, trades []SimulationTrade, feesPaid, balanceNow float64) SimulationSummary {
	summary := SimulationSummary{
		StartedAt: run.StartedAt.UTC(), BalanceStartSOL: run.BalanceStartSOL,
		BalanceNowSOL: balanceNow, TotalTrades: len(trades), FeesPaidSOL: feesPaid,
		ExitReasons: map[string]int{"take_profit": 0, "trailing_stop": 0, "stop_loss": 0, "max_hold": 0, "other": 0},
	}
	var winTotal, lossTotal, holdTotal, positiveTotal, negativeTotal float64
	var netResults []float64
	var cumulative, peak float64
	currentWinStreak, currentLossStreak := 0, 0
	for _, trade := range trades {
		summary.GrossPnLSOL += trade.GrossPnLSOL
		holdTotal += trade.HoldSeconds
		net := trade.GrossPnLSOL - trade.FeesSOL
		netResults = append(netResults, net)
		cumulative += net
		if cumulative > peak {
			peak = cumulative
		}
		if drawdown := peak - cumulative; drawdown > summary.MaxDrawdownSOL {
			summary.MaxDrawdownSOL = drawdown
		}
		switch {
		case net > 0:
			summary.Wins++
			winTotal += net
			positiveTotal += net
			currentWinStreak++
			currentLossStreak = 0
			if currentWinStreak > summary.LongestWinStreak {
				summary.LongestWinStreak = currentWinStreak
			}
		case net < 0:
			summary.Losses++
			lossTotal += net
			negativeTotal += net
			currentLossStreak++
			currentWinStreak = 0
			if currentLossStreak > summary.LongestLossStreak {
				summary.LongestLossStreak = currentLossStreak
			}
		default:
			currentWinStreak, currentLossStreak = 0, 0
		}
		summary.ExitReasons[exitReasonBucket(trade.ExitReason)]++
	}
	summary.NetPnLSOL = summary.GrossPnLSOL - feesPaid
	if summary.TotalTrades > 0 {
		summary.WinRate = float64(summary.Wins) * 100 / float64(summary.TotalTrades)
		summary.AvgHoldSeconds = holdTotal / float64(summary.TotalTrades)
		summary.EVPerTradeSOL = summary.NetPnLSOL / float64(summary.TotalTrades)
	}
	if summary.Wins > 0 {
		summary.AvgWinSOL = winTotal / float64(summary.Wins)
	}
	if summary.Losses > 0 {
		summary.AvgLossSOL = lossTotal / float64(summary.Losses)
		summary.ProfitFactor = floatPointer(positiveTotal / math.Abs(negativeTotal))
	}
	if len(netResults) >= 2 {
		mean := 0.0
		for _, result := range netResults {
			mean += result
		}
		mean /= float64(len(netResults))
		var variance float64
		for _, result := range netResults {
			delta := result - mean
			variance += delta * delta
		}
		variance /= float64(len(netResults) - 1)
		if standardDeviation := math.Sqrt(variance); standardDeviation > 0 {
			summary.SharpeRatio = floatPointer(mean / standardDeviation)
		}
	}
	return summary
}

func exitReasonBucket(reason string) string {
	reason = strings.ToLower(reason)
	switch {
	case strings.Contains(reason, "trailing"):
		return "trailing_stop"
	case strings.Contains(reason, "take profit"):
		return "take_profit"
	case strings.Contains(reason, "stop loss"):
		return "stop_loss"
	case strings.Contains(reason, "max hold"):
		return "max_hold"
	default:
		return "other"
	}
}

func floatPointer(value float64) *float64 { return &value }

func (d *DB) LiveUnlockSummary() (SimulationSummary, error) {
	run, err := d.SimulationRun()
	if err != nil {
		return SimulationSummary{}, fmt.Errorf("no persisted paper simulation run: %w", err)
	}
	now := time.Now().UTC()
	trades, err := d.SimulationTrades(run.StartedAt, now.Add(time.Nanosecond))
	if err != nil {
		return SimulationSummary{}, err
	}
	fees, err := d.SimulationFees(run.StartedAt, now.Add(time.Nanosecond))
	if err != nil {
		return SimulationSummary{}, err
	}
	openPnl, err := d.OpenPnlSOL(true)
	if err != nil {
		return SimulationSummary{}, err
	}
	return BuildSimulationSummary(run, trades, fees, run.BalanceStartSOL+tradesGross(trades)-fees+openPnl), nil
}

func (d *DB) RequireLiveUnlock() (SimulationSummary, error) {
	report, err := d.LiveUnlockReport()
	if err != nil {
		return report.Summary, err
	}
	if report.Summary.TotalTrades < 30 {
		return report.Summary, fmt.Errorf("paper live-unlock requires at least 30 closed trades; got %d", report.Summary.TotalTrades)
	}
	if !report.Ready {
		return report.Summary, fmt.Errorf("paper live-unlock blocked: %s", strings.Join(report.Blockers, "; "))
	}
	return report.Summary, nil
}

func tradesGross(trades []SimulationTrade) float64 {
	var gross float64
	for _, trade := range trades {
		gross += trade.GrossPnLSOL
	}
	return gross
}

