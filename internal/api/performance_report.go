package api

import (
	"net/http"
	"sort"
	"time"

	"solana-sniper-backend/internal/database"
)

func (s *Server) performanceReport(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, http.MethodGet) {
		return
	}
	now := time.Now().UTC()
	from := now.Add(-30 * 24 * time.Hour)
	var run database.SimulationRun
	if s.cfg.Simulation {
		var err error
		run, err = s.db.SimulationRun()
		if err != nil {
			fail(w, http.StatusServiceUnavailable, "simulation_not_started", "simulation session is not initialized")
			return
		}
		from = run.StartedAt
	}
	to := now
	var err error
	if raw := r.URL.Query().Get("from"); raw != "" {
		from, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_from", "from must be an RFC3339 timestamp")
			return
		}
		from = from.UTC()
	}
	if raw := r.URL.Query().Get("to"); raw != "" {
		to, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_to", "to must be an RFC3339 timestamp")
			return
		}
		to = to.UTC()
	}
	if to.After(now) {
		to = now
	}
	if s.cfg.Simulation && from.Before(run.StartedAt) {
		from = run.StartedAt
	}
	if !from.Before(to) {
		fail(w, http.StatusBadRequest, "invalid_period", "from must be earlier than to")
		return
	}

	var (
		summary       database.SimulationSummary
		trades        []database.SimulationTrade
		allFeesPaid   float64
		openPnl       float64
		reportStarted *time.Time
		balanceStart  *float64
		balanceNow    *float64
		feeModel      string
	)
	if s.cfg.Simulation {
		summary, trades, allFeesPaid, err = s.simulationReportData(run, from, to)
		if err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not build simulation report")
			return
		}
		openPnl, err = s.db.OpenPnlSOL(true)
		if err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load estimated open P&L")
			return
		}
		startedAt := run.StartedAt
		startBalance, currentBalance := summary.BalanceStartSOL, summary.BalanceNowSOL
		reportStarted, balanceStart, balanceNow = &startedAt, &startBalance, &currentBalance
		feeModel = "paper transaction fees use the configured priority-fee cap plus SIMULATION_BASE_FEE_SOL; venue/rent/slippage costs are not fully simulated"
	} else {
		trades, err = s.db.LiveTrades(from, to)
		if err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load confirmed live trades")
			return
		}
		allFeesPaid, err = s.db.LiveFees(from, to)
		if err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load confirmed live fees")
			return
		}
		openPnl, err = s.db.OpenPnlSOL(false)
		if err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load estimated open P&L")
			return
		}
		summary = database.BuildSimulationSummary(
			database.SimulationRun{StartedAt: from},
			trades,
			tradeReportFees(trades),
			0,
		)
		feeModel = "confirmed on-chain wallet deltas and reported network fees; open-position P&L is a market-cap-ratio estimate"
	}

	curve, byHour := buildReportSeries(from, trades)
	openPositions, err := s.db.OpenPositionCount()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not count open positions")
		return
	}
	unmarkedPositions, err := s.db.UnmarkedOpenPositionCount(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not check open position marks")
		return
	}
	var openPnlReported any = openPnl
	if openPositions > 0 && unmarkedPositions >= openPositions {
		openPnlReported = nil
	}
	write(w, http.StatusOK, map[string]any{
		"mode":            s.mode(),
		"period":          map[string]time.Time{"from": from, "to": to},
		"summary":         reportSummaryPayload(summary, reportStarted, balanceStart, balanceNow, allFeesPaid),
		"trades":          trades,
		"cumulativeCurve": curve,
		"byHour":          byHour,
		"allFeesPaidSol":  allFeesPaid,
		"openPnlSol":      openPnlReported,
		"openPnlComplete": unmarkedPositions == 0,
		"unmarkedOpenPositions": unmarkedPositions,
		"openPnlBasis":    "market_cap_ratio_estimate",
		"feeModel":        feeModel,
		"paperReadiness":  s.paperReadiness(),
	})
}

func (s *Server) paperReadiness() any {
	paperDB := s.db
	if !s.cfg.Simulation {
		paperDB = s.paperDB
	}
	if paperDB == nil {
		return map[string]any{"available": false, "ready": false, "error": "paper evidence is not connected"}
	}
	readiness, err := paperDB.LiveUnlockReport()
	if err != nil {
		return map[string]any{"available": false, "ready": false, "error": "paper evidence is unavailable"}
	}
	return readiness
}

func reportSummaryPayload(
	summary database.SimulationSummary,
	startedAt *time.Time,
	balanceStart, balanceNow *float64,
	allFeesPaid float64,
) map[string]any {
	return map[string]any{
		"startedAt": startedAt, "balanceStartSol": balanceStart, "balanceNowSol": balanceNow,
		"totalTrades": summary.TotalTrades, "wins": summary.Wins, "losses": summary.Losses,
		"winRate": summary.WinRate, "avgWinSol": summary.AvgWinSOL, "avgLossSol": summary.AvgLossSOL,
		"grossPnlSol": summary.GrossPnLSOL, "feesPaidSol": summary.FeesPaidSOL,
		"allFeesPaidSol": allFeesPaid, "netPnlSol": summary.NetPnLSOL,
		"avgHoldSeconds": summary.AvgHoldSeconds, "maxDrawdownSol": summary.MaxDrawdownSOL,
		"evPerTradeSol": summary.EVPerTradeSOL, "sharpeRatio": summary.SharpeRatio,
		"profitFactor": summary.ProfitFactor, "exitReasons": summary.ExitReasons,
		"longestWinStreak": summary.LongestWinStreak, "longestLossStreak": summary.LongestLossStreak,
	}
}

func tradeReportFees(trades []database.SimulationTrade) float64 {
	var fees float64
	for _, trade := range trades {
		fees += trade.FeesSOL
	}
	return fees
}

func buildReportSeries(from time.Time, trades []database.SimulationTrade) ([]map[string]any, []map[string]any) {
	curve := make([]map[string]any, 0, len(trades)+1)
	curve = append(curve, map[string]any{"timestamp": from, "cumulativePnlSol": 0.0})
	cumulative := 0.0
	type hourResult struct {
		trades    int
		netPnLSOL float64
	}
	hours := make(map[time.Time]*hourResult)
	for _, trade := range trades {
		cumulative += trade.NetPnLSOL
		curve = append(curve, map[string]any{"timestamp": trade.ConfirmedAt, "cumulativePnlSol": cumulative})
		hour := trade.ConfirmedAt.UTC().Truncate(time.Hour)
		if hours[hour] == nil {
			hours[hour] = &hourResult{}
		}
		hours[hour].trades++
		hours[hour].netPnLSOL += trade.NetPnLSOL
	}
	sortedHours := make([]time.Time, 0, len(hours))
	for hour := range hours {
		sortedHours = append(sortedHours, hour)
	}
	sort.Slice(sortedHours, func(i, j int) bool { return sortedHours[i].Before(sortedHours[j]) })
	byHour := make([]map[string]any, 0, len(sortedHours))
	for _, hour := range sortedHours {
		entry := hours[hour]
		byHour = append(byHour, map[string]any{
			"hour":      hour.Format("2006-01-02T15:04"),
			"trades":    entry.trades,
			"netPnlSol": entry.netPnLSOL,
		})
	}
	return curve, byHour
}
