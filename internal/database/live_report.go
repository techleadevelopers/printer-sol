package database

import (
	"fmt"
	"math"
	"time"
)

type LiveUnlockCheck struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Passed   bool   `json:"passed"`
	Actual   string `json:"actual"`
	Required string `json:"required"`
}

type LiveUnlockReadiness struct {
	Available bool              `json:"available"`
	Ready     bool              `json:"ready"`
	Summary   SimulationSummary `json:"summary"`
	Checks    []LiveUnlockCheck `json:"checks"`
	Blockers  []string          `json:"blockers"`
}

// LiveTrades reports closed live round trips from confirmed on-chain wallet
// deltas. The realized value stored on the sell already includes both
// transaction fees; GrossPnL restores those fees so net remains auditable.
func (d *DB) LiveTrades(from, to time.Time) ([]SimulationTrade, error) {
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
			WHERE b2.mint=s.mint AND b2.side='buy' AND b2.state IN ('confirmed','finalized')
			ORDER BY b2.confirmed_at DESC,b2.created_at DESC LIMIT 1
		)
		WHERE s.side='sell' AND s.state IN ('confirmed','finalized')
		AND s.realized_pnl_sol IS NOT NULL
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
		var realizedNet float64
		if err = rows.Scan(
			&trade.ID, &trade.Mint, &trade.EntrySOL, &trade.ExitSOL,
			&realizedNet, &trade.FeesSOL, &openedAt, &closedAt,
			&trade.ExitReason, &trade.ConfirmedAt,
		); err != nil {
			return nil, err
		}
		trade.Side = "round_trip"
		trade.GrossPnLSOL = realizedNet + trade.FeesSOL
		trade.NetPnLSOL = realizedNet
		trade.HoldSeconds = math.Max(0, closedAt.Sub(openedAt).Seconds())
		trade.ConfirmedAt = trade.ConfirmedAt.UTC()
		trades = append(trades, trade)
	}
	return trades, rows.Err()
}

// LiveFees returns confirmed network fees paid during a time interval. The
// report separately attributes entry and exit fees to each closed round trip.
func (d *DB) LiveFees(from, to time.Time) (float64, error) {
	var fees float64
	err := d.QueryRow(d.bind(`SELECT COALESCE(SUM(fees_sol),0)
		FROM trades WHERE state IN ('confirmed','finalized') AND fees_sol IS NOT NULL
		AND COALESCE(confirmed_at,created_at)>=? AND COALESCE(confirmed_at,created_at)<?`), from, to).Scan(&fees)
	return fees, err
}

// LiveUnlockReport exposes every paper-evidence check used by the startup
// safety gate. It never changes the active configuration or starts trading.
func (d *DB) LiveUnlockReport() (LiveUnlockReadiness, error) {
	summary, err := d.LiveUnlockSummary()
	if err != nil {
		return LiveUnlockReadiness{}, err
	}
	run, err := d.SimulationRun()
	if err != nil {
		return LiveUnlockReadiness{}, err
	}
	unknown, err := d.UnknownTradeCount()
	if err != nil {
		return LiveUnlockReadiness{}, err
	}
	openPositions, err := d.OpenPositionCount()
	if err != nil {
		return LiveUnlockReadiness{}, err
	}

	report := LiveUnlockReadiness{
		Available: true,
		Ready:     true,
		Summary:   summary,
		Checks:    make([]LiveUnlockCheck, 0, 14),
		Blockers:  make([]string, 0),
	}
	add := func(key, label string, passed bool, actual, required string) {
		report.Checks = append(report.Checks, LiveUnlockCheck{
			Key: key, Label: label, Passed: passed, Actual: actual, Required: required,
		})
		if !passed {
			report.Ready = false
			report.Blockers = append(report.Blockers, label+": "+actual+" (required "+required+")")
		}
	}

	tradeCount := summary.TotalTrades
	elapsed := time.Since(run.StartedAt).Hours() / 24
	add("minimum_30_trades", "Mínimo de 30 operações paper", tradeCount >= 30,
		fmt.Sprintf("%d operações", tradeCount), ">= 30")
	add("minimum_7_days", "Simulação por pelo menos 7 dias", elapsed >= 7,
		fmt.Sprintf("%.1f dias", elapsed), ">= 7 dias")
	add("minimum_100_trades", "Mínimo de 100 operações fechadas", tradeCount >= 100,
		fmt.Sprintf("%d operações", tradeCount), ">= 100")
	add("positive_net_pnl", "P&L líquido positivo", summary.NetPnLSOL > 0,
		fmt.Sprintf("%.9f SOL", summary.NetPnLSOL), "> 0 SOL")
	add("minimum_win_rate", "Taxa de acerto mínima", summary.WinRate >= 45,
		fmt.Sprintf("%.2f%%", summary.WinRate), ">= 45%")

	profitFactorOK := summary.ProfitFactor != nil && *summary.ProfitFactor >= 1.2
	profitFactorActual := "indisponível"
	if summary.ProfitFactor != nil {
		profitFactorActual = fmt.Sprintf("%.3f", *summary.ProfitFactor)
	} else if summary.Wins > 0 && summary.Losses == 0 {
		profitFactorOK = true
		profitFactorActual = "sem perdas (fator não finito)"
	}
	add("minimum_profit_factor", "Profit factor mínimo", profitFactorOK,
		profitFactorActual, ">= 1.2")
	maxDrawdownLimit := 0.5 * summary.BalanceStartSOL
	add("maximum_drawdown", "Drawdown máximo", summary.MaxDrawdownSOL <= maxDrawdownLimit,
		fmt.Sprintf("%.9f SOL", summary.MaxDrawdownSOL), fmt.Sprintf("<= %.9f SOL", maxDrawdownLimit))
	add("positive_expectancy", "Expectativa por operação positiva", summary.TotalTrades > 0 && summary.EVPerTradeSOL > 0,
		fmt.Sprintf("%.9f SOL/operação", summary.EVPerTradeSOL), "> 0 SOL/operação")

	requiredExitReasons := []string{"take_profit", "trailing_stop", "stop_loss", "max_hold"}
	exitCountsOK := true
	exitCounts := make([]string, 0, len(requiredExitReasons))
	for _, reason := range requiredExitReasons {
		count := summary.ExitReasons[reason]
		exitCounts = append(exitCounts, fmt.Sprintf("%s=%d", reason, count))
		if count < 10 {
			exitCountsOK = false
		}
	}
	add("exit_reason_coverage", "Pelo menos 10 saídas por motivo", exitCountsOK,
		fmt.Sprint(exitCounts), ">= 10 por motivo")
	add("reconciliation_clean", "Reconciliação sem pendências", unknown == 0,
		fmt.Sprintf("%d pendências", unknown), "0")
	add("maximum_hold", "Tempo médio de retenção", summary.AvgHoldSeconds <= 180,
		fmt.Sprintf("%.1f segundos", summary.AvgHoldSeconds), "<= 180 segundos")

	feesRatioOK := summary.GrossPnLSOL > 0 && summary.FeesPaidSOL <= 0.15*summary.GrossPnLSOL
	feesRatio := math.Inf(1)
	if summary.GrossPnLSOL > 0 {
		feesRatio = summary.FeesPaidSOL / summary.GrossPnLSOL
	}
	add("fees_below_15_percent", "Taxas abaixo de 15% do lucro bruto", feesRatioOK,
		fmt.Sprintf("%.1f%%", feesRatio*100), "<= 15%")

	trailingCount := summary.ExitReasons["trailing_stop"]
	trailingShare := 0.0
	if tradeCount > 0 {
		trailingShare = float64(trailingCount) / float64(tradeCount)
	}
	add("trailing_stop_active", "Saídas trailing acima de 30%", trailingShare > 0.30,
		fmt.Sprintf("%.1f%%", trailingShare*100), "> 30%")
	add("paper_positions_closed", "Todas as posições paper encerradas", openPositions == 0,
		fmt.Sprintf("%d posições abertas", openPositions), "0")

	return report, nil
}
