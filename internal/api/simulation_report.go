package api

import (
	"net/http"
	"sort"
	"time"

	"solana-sniper-backend/internal/database"
)

func (s *Server) simulationReport(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, http.MethodGet) {
		return
	}
	if !s.cfg.Simulation {
		fail(w, http.StatusForbidden, "simulation_only", "simulation reports are unavailable in live mode")
		return
	}
	run, err := s.db.SimulationRun()
	if err != nil {
		fail(w, http.StatusServiceUnavailable, "simulation_not_started", "simulation session is not initialized")
		return
	}
	now := time.Now().UTC()
	from := run.StartedAt
	if raw := r.URL.Query().Get("from"); raw != "" {
		from, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_from", "from must be an RFC3339 timestamp")
			return
		}
		from = from.UTC()
	}
	to := now
	if raw := r.URL.Query().Get("to"); raw != "" {
		to, err = time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			fail(w, http.StatusBadRequest, "invalid_to", "to must be an RFC3339 timestamp")
			return
		}
		to = to.UTC()
	}
	if from.Before(run.StartedAt) {
		from = run.StartedAt
	}
	if to.After(now) {
		to = now
	}
	if !from.Before(to) {
		fail(w, http.StatusBadRequest, "invalid_period", "from must be earlier than to")
		return
	}
	summary, trades, allFeesPaid, err := s.simulationReportData(run, from, to)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not build simulation report")
		return
	}
	sellAttempts, failedSellAttempts, err := s.db.SellActivityStats(from, to)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not count simulated sell attempts")
		return
	}
	curve := make([]map[string]any, 0, len(trades)+1)
	curve = append(curve, map[string]any{"timestamp": from, "cumulativePnlSol": 0.0})
	cumulative := 0.0
	type hourResult struct {
		Hour      time.Time
		Trades    int
		NetPnLSOL float64
	}
	hours := make(map[time.Time]*hourResult)
	for _, trade := range trades {
		cumulative += trade.NetPnLSOL
		curve = append(curve, map[string]any{"timestamp": trade.ConfirmedAt, "cumulativePnlSol": cumulative})
		hour := trade.ConfirmedAt.UTC().Truncate(time.Hour)
		if hours[hour] == nil {
			hours[hour] = &hourResult{Hour: hour}
		}
		hours[hour].Trades++
		hours[hour].NetPnLSOL += trade.NetPnLSOL
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
			"hour": hour.Format("2006-01-02T15:04"), "trades": entry.Trades, "netPnlSol": entry.NetPnLSOL,
		})
	}
	write(w, http.StatusOK, map[string]any{
		"period":          map[string]time.Time{"from": from, "to": to},
		"mode":            "simulation",
		"summary":         summary,
		"allFeesPaidSol":  allFeesPaid,
		"trades":          trades,
		"sellAttempts":    sellAttempts,
		"failedSellAttempts": failedSellAttempts,
		"completedSells":  len(trades),
		"cumulativeCurve": curve,
		"byHour":          byHour,
		"paperReadiness":  s.paperReadiness(),
		"feeModel":        "paper transaction fees = configured priority-fee cap plus SIMULATION_BASE_FEE_SOL; venue/rent/slippage costs are not fully simulated",
"openPnlBasis":    "bonding_curve_spot_price_ratio_estimate",
	})
}

func (s *Server) simulationReportData(run database.SimulationRun, from, to time.Time) (database.SimulationSummary, []database.SimulationTrade, float64, error) {
	trades, err := s.db.SimulationTrades(from, to)
	if err != nil {
		return database.SimulationSummary{}, nil, 0, err
	}
	allFeesPaid, err := s.db.SimulationFees(from, to)
	if err != nil {
		return database.SimulationSummary{}, nil, 0, err
	}
	now := time.Now().UTC()
	allTrades, err := s.db.SimulationTrades(run.StartedAt, now.Add(time.Nanosecond))
	if err != nil {
		return database.SimulationSummary{}, nil, 0, err
	}
	sessionFees, err := s.db.SimulationFees(run.StartedAt, now.Add(time.Nanosecond))
	if err != nil {
		return database.SimulationSummary{}, nil, 0, err
	}
	openPnl, err := s.db.OpenPnlSOLFresh(true, s.positionMarkCutoff(time.Now().UTC()))
	if err != nil {
		return database.SimulationSummary{}, nil, 0, err
	}
	balanceNow := run.BalanceStartSOL + simulationGross(allTrades) - sessionFees + openPnl
	closedTradeFees := simulationTradeFees(trades)
	return database.BuildSimulationSummary(run, trades, closedTradeFees, balanceNow), trades, allFeesPaid, nil
}

func simulationGross(trades []database.SimulationTrade) float64 {
	var gross float64
	for _, trade := range trades {
		gross += trade.GrossPnLSOL
	}
	return gross
}

func simulationTradeFees(trades []database.SimulationTrade) float64 {
	var fees float64
	for _, trade := range trades {
		fees += trade.FeesSOL
	}
	return fees
}
