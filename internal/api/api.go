package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"os"
	"solana-sniper-backend/internal/bot"
	"solana-sniper-backend/internal/config"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/execution"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/tokenmonitor"
	"solana-sniper-backend/internal/wallet"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	db         *database.DB
	wallet     *wallet.Wallet
	rpcs       *rpcmanager.Manager
	engine     *execution.Engine
	controller *bot.Controller
	monitor    *tokenmonitor.Monitor
	bus        *events.Bus
	paperDB    *database.DB
	cfg        config.Config
	log        *zap.Logger
	resetMu    sync.Mutex
	lastReset  time.Time
}

func New(db *database.DB, w *wallet.Wallet, r *rpcmanager.Manager, e *execution.Engine, c *bot.Controller, m *tokenmonitor.Monitor, b *events.Bus, cfg config.Config, l *zap.Logger, paperDatabases ...*database.DB) http.Handler {
	s := &Server{db: db, wallet: w, rpcs: r, engine: e, controller: c, monitor: m, bus: b, cfg: cfg, log: l}
	if len(paperDatabases) > 0 {
		s.paperDB = paperDatabases[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.health)
	mux.HandleFunc("/api/auth/login", s.login)
	mux.HandleFunc("/api/dashboard", s.admin(s.dashboard))
	mux.HandleFunc("/api/simulation/report", s.admin(s.simulationReport))
	mux.HandleFunc("/api/simulation/reset", s.admin(s.simulationReset))
	mux.HandleFunc("/api/performance/report", s.admin(s.performanceReport))
	mux.HandleFunc("/api/tokens", s.admin(s.tokens))
	mux.HandleFunc("/api/positions", s.admin(s.positions))
	mux.HandleFunc("/api/trades", s.admin(s.trades))
	mux.HandleFunc("/api/rpcs", s.admin(s.rpcsList))
	mux.HandleFunc("/api/config", s.admin(s.config))
	mux.HandleFunc("/api/wallet", s.admin(s.walletInfo))
	mux.HandleFunc("/api/bot/stop", s.admin(s.stop))
	mux.HandleFunc("/api/bot/pause", s.admin(s.pause))
	mux.HandleFunc("/api/bot/resume", s.admin(s.resume))
	mux.HandleFunc("/api/bot/emergency-stop", s.admin(s.emergency))
	mux.HandleFunc("/api/rpcs/test", s.admin(s.rpcTest))
	mux.HandleFunc("/api/rpcs/select", s.admin(s.rpcSelect))
	mux.HandleFunc("/ws/events", s.ws)
	return corsPolicy(mux)
}
func method(w http.ResponseWriter, r *http.Request, m string) bool {
	if r.Method != m {
		fail(w, 405, "method_not_allowed", "method not allowed")
		return false
	}
	return true
}
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	monitorState := "disconnected"
	if s.monitor != nil && s.monitor.Connected() {
		monitorState = "connected"
	}
	write(w, 200, map[string]any{"status": "ok", "eventMonitorStatus": monitorState, "checkedAt": time.Now().UTC()})
}
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	positions, err := s.db.OpenPositionCount()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load open position count")
		return
	}
	detected, err := s.db.TokenEventCount()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load detected token count")
		return
	}
	analyzed, err := s.db.RiskAssessmentCount()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load token analysis count")
		return
	}
	openPnl, err := s.db.OpenPnlSOL(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load estimated open P&L")
		return
	}
	unmarkedPositions, err := s.db.UnmarkedOpenPositionCount(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load position price coverage")
		return
	}
	costs, err := s.db.TradeCostsSOL()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load transaction costs")
		return
	}
	performance, err := s.db.Performance(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load realized trade performance")
		return
	}
	trades, err := s.tradeItems(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load recent trades")
		return
	}
	tradeCount, err := s.db.TradeCount()
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load trade count")
		return
	}
	now := time.Now().UTC()
	periodStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	periodEnd := periodStart.AddDate(0, 0, 1)
	periodTotal, positive, negative, err := s.db.TradePeriodStats(periodStart, periodEnd)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load daily trade results")
		return
	}
	if s.cfg.Simulation {
		periodTrades, periodErr := s.db.SimulationTrades(periodStart, periodEnd)
		if periodErr != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load daily paper trade results")
			return
		}
		periodTotal, positive, negative = 0, 0, 0
		for _, trade := range periodTrades {
			periodTotal++
			if trade.NetPnLSOL > 0 {
				positive++
			} else if trade.NetPnLSOL < 0 {
				negative++
			}
		}
	}
	cumulativeResult, err := s.cumulativeResult(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load P&L history")
		return
	}
	var staleMarks map[string]any
	if s.cfg.Simulation {
		total, flat, pctFlat, statsErr := s.db.PaperMarkStats()
		if statsErr != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load paper mark statistics")
			return
		}
		staleMarks = map[string]any{"total": total, "flat": flat, "pctFlat": pctFlat}
	}
	balance, balanceAvailable := s.balanceSOL(r.Context())
	var walletBalance any
	if balanceAvailable {
		walletBalance = balance
	}
	virtualEquity := balance
	var simSummary *database.SimulationSummary
	var simulationAllFeesPaid float64
	balanceHistory := make([]map[string]any, 0)
	netRealizedPnl := performance.NetRealizedPnlSOL
	grossRealizedPnl := performance.GrossRealizedPnlSOL
	feesPaid := costs
	totalEstimatedPnl := netRealizedPnl + openPnl
	if s.cfg.Simulation {
		run, runErr := s.db.SimulationRun()
		if runErr != nil {
			fail(w, http.StatusServiceUnavailable, "simulation_not_started", "simulation session is not initialized")
			return
		}
		summary, sessionTrades, allFeesPaid, summaryErr := s.simulationReportData(run, run.StartedAt, now.Add(time.Nanosecond))
		if summaryErr != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not load paper performance")
			return
		}
		simSummary = &summary
		simulationAllFeesPaid = allFeesPaid
		virtualEquity = summary.BalanceNowSOL
		netRealizedPnl = summary.NetPnLSOL
		grossRealizedPnl = summary.GrossPnLSOL
		feesPaid = allFeesPaid
		totalEstimatedPnl = summary.BalanceNowSOL - summary.BalanceStartSOL
		balanceHistory = simulationBalanceHistory(run, sessionTrades, summary.BalanceNowSOL, now)
	}
	var winRate any
	if performance.ClosedTrades > 0 {
		winRate = performance.WinRatePct
	}
	performancePayload := map[string]any{
		"closedTrades": performance.ClosedTrades, "winners": performance.Winners,
		"losers": performance.Losers, "winRatePct": winRate,
		"avgWinSol": performance.AvgWinSOL, "avgLossSol": performance.AvgLossSOL,
		"avgHoldSeconds": performance.AvgHoldSeconds, "feesPaidSol": performance.FeesSOL,
		"netRealizedPnlSol":   performance.NetRealizedPnlSOL,
		"grossRealizedPnlSol": performance.GrossRealizedPnlSOL,
		"grossPnlBasis":       "net realized PnL plus recorded fees for closed positions",
	}
	if simSummary != nil {
		winRate = simSummary.WinRate
		performancePayload = map[string]any{
			"closedTrades": simSummary.TotalTrades, "winners": simSummary.Wins,
			"losers": simSummary.Losses, "winRatePct": winRate,
			"avgWinSol": simSummary.AvgWinSOL, "avgLossSol": simSummary.AvgLossSOL,
			"avgHoldSeconds": simSummary.AvgHoldSeconds, "feesPaidSol": simSummary.FeesPaidSOL,
			"netRealizedPnlSol": simSummary.NetPnLSOL, "grossRealizedPnlSol": simSummary.GrossPnLSOL,
			"allFeesPaidSol": simulationAllFeesPaid,
			"grossPnlBasis":  "gross closed PnL; net includes modeled paper transaction fees",
		}
	}
	var estimatedOpenPnl any = openPnl
	if positions > 0 && unmarkedPositions >= positions {
		estimatedOpenPnl = nil
	}
	dashboard := map[string]any{
		"botStatus": string(s.controller.State()), "mode": s.mode(), "walletBalanceSol": walletBalance,
		"walletBalanceAvailable": balanceAvailable,
		"walletBalanceLow": balanceAvailable && balance < s.engine.Limits().FeeReserveSOL,
		"feesReserveSol": s.engine.Limits().FeeReserveSOL,
		"detectedTokens": detected, "analyzedTokens": analyzed, "tradesCount": tradeCount, "openPositions": positions,
		"realizedPnlSol": netRealizedPnl, "grossRealizedPnlSol": grossRealizedPnl,
		"estimatedOpenPnlSol": estimatedOpenPnl, "totalEstimatedPnlSol": totalEstimatedPnl,
		"totalEstimatedPnlComplete": unmarkedPositions == 0,
		"netPnlSol": totalEstimatedPnl, "estimatedOpenPnlComplete": unmarkedPositions == 0,
		"unmarkedOpenPositions": unmarkedPositions, "costsSol": feesPaid, "feesPaidSol": feesPaid,
		"openPnlBasis":     "pumpportal_market_cap_ratio_estimate",
		"performance":      performancePayload,
		"cumulativeResult": cumulativeResult,
		"tradesByPeriod":   []map[string]any{{"period": now.Format("2006-01-02"), "total": periodTotal, "positive": positive, "negative": negative}},
		"balanceHistory":   balanceHistory, "recentTrades": trades,
		"closedTrades": performance.ClosedTrades,
	}
	if s.cfg.Simulation {
		dashboard["virtualEquitySol"] = virtualEquity
dashboard["simulationBalanceSol"] = simSummary.BalanceStartSOL
		dashboard["simulationResetBalanceSol"] = s.cfg.SimulationBalanceSOL
		dashboard["simulation"] = simSummary
		dashboard["closedTrades"] = simSummary.TotalTrades
		dashboard["staleMarks"] = staleMarks
	}
	dashboard["transactionsCount"] = tradeCount
	write(w, 200, dashboard)
}
func (s *Server) tokens(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	rows, e := s.db.ListTokens()
	if e != nil {
		fail(w, 500, "database_unavailable", "database unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var mint, creator, at, reason, tradeSide, tradeState, positionState string
		var latency int64
		var approved sql.NullBool
		if err := rows.Scan(&mint, &creator, &at, &latency, &approved, &reason, &tradeSide, &tradeState, &positionState); err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not read token state")
			return
		}
		analysisStatus, riskResult := "detected", "not_assessed"
		if approved.Valid && approved.Bool {
			analysisStatus, riskResult = "approved", "accepted"
		} else if approved.Valid {
			analysisStatus, riskResult = "rejected", "rejected"
		}
		operationStatus := analysisStatus
		if positionState == "open" {
			operationStatus = "position_open"
		} else {
			switch tradeState {
			case "prepared", "signed", "submitted", "pending":
				operationStatus = tradeSide + "_submitted"
			case "simulated", "confirmed", "finalized":
				operationStatus = tradeSide + "_confirmed"
			case "failed":
				operationStatus = tradeSide + "_failed"
			case "unknown":
				operationStatus = "unknown"
			}
		}
		var decisionReason any
		if reason != "" {
			decisionReason = reason
		}
		out = append(out, map[string]any{
			"id": mint, "detectedAt": at, "name": nil, "symbol": nil, "mint": mint,
			"creatorWallet": creator, "analysisStatus": analysisStatus,
			"liquiditySol": nil, "tradeCount": nil, "riskResult": riskResult,
			"decisionReason": decisionReason, "operationStatus": operationStatus,
		})
	}
	write(w, 200, map[string]any{"items": out, "nextCursor": nil})
}
func (s *Server) positions(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	rows, e := s.db.ListPositionsForMode(s.cfg.Simulation)
	if e != nil {
		fail(w, 500, "database_unavailable", "database unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var mint, q, status, opened, updated, markUpdated, latestSellState string
		var cost, entryMarketCap, currentMarketCap, positionFees float64
		var realized sql.NullFloat64
		if err := rows.Scan(&mint, &q, &cost, &status, &opened, &updated, &entryMarketCap, &currentMarketCap, &markUpdated, &positionFees, &realized, &latestSellState); err != nil {
			fail(w, http.StatusInternalServerError, "database_unavailable", "could not read position data")
			return
		}
		quantity, _ := strconv.ParseFloat(q, 64)
		markAvailable := entryMarketCap > 0 && currentMarketCap > 0
		var returnPct, estimatedValue, estimatedPnl any
		if markAvailable && status == "open" && cost > 0 {
			pct := (currentMarketCap - entryMarketCap) / entryMarketCap * 100
			value := cost * currentMarketCap / entryMarketCap
			returnPct, estimatedValue, estimatedPnl = pct, value, value-cost
		}
		var realizedPnl any
		if realized.Valid {
			netPositionPnl := realized.Float64
			if s.cfg.Simulation {
				netPositionPnl -= positionFees
			}
			realizedPnl = netPositionPnl
		}
		pnlEstimateAvailable := markAvailable && cost > 0 && status == "open"
		sellStatus := "not_requested"
		switch latestSellState {
		case "prepared", "signed", "submitted", "pending":
			sellStatus = "submitted"
		case "simulated", "confirmed", "finalized":
			sellStatus = "confirmed"
		case "failed":
			sellStatus = "failed"
		case "unknown":
			sellStatus = "unknown"
		}
		position := map[string]any{
			"id": mint, "tokenName": nil, "symbol": nil, "mint": mint,
			"quantity": quantity, "quantityUnit": "SOL allocation", "investedSol": cost,
			"averageEntryPriceSol": nil, "buyValueSol": cost, "costsSol": positionFees,
			"entryMarketCapSol": nil, "currentMarketCapSol": nil,
			"entryTokenPriceSol": nil, "currentTokenPriceSol": nil, "tokenPriceAvailable": false,
			"returnPct": returnPct, "estimatedValueSol": estimatedValue, "estimatedPnlSol": estimatedPnl,
			"markAvailable": markAvailable, "pnlEstimateAvailable": pnlEstimateAvailable,
			"markUpdatedAt": nil, "markAgeSeconds": nil,
			"markSource": "market_cap_feed", "pnlBasis": "market_cap_ratio_estimate",
			"realizedPnlSol": realizedPnl, "openedAt": opened, "updatedAt": updated, "status": status,
			"sellStatus": sellStatus, "simulation": s.cfg.Simulation,
		}
		if markAvailable {
			position["entryMarketCapSol"] = entryMarketCap
			position["currentMarketCapSol"] = currentMarketCap
			position["markUpdatedAt"] = markUpdated
			if age, ok := positionMarkAgeSeconds(markUpdated, time.Now().UTC()); ok {
				position["markAgeSeconds"] = age
			}
		}
		out = append(out, position)
	}
	if err := rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not read positions")
		return
	}
	write(w, 200, map[string]any{"items": out, "mode": s.mode()})
}
func (s *Server) trades(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	items, err := s.tradeItems(s.cfg.Simulation)
	if err != nil {
		fail(w, http.StatusInternalServerError, "database_unavailable", "could not load trades")
		return
	}
	write(w, 200, map[string]any{"items": items, "nextCursor": nil})
}
func (s *Server) tradeItems(simulation bool) ([]map[string]any, error) {
	rows, err := s.db.ListTrades()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]any{}
	simNetByID := make(map[string]database.SimulationTrade)
	liveNetByID := make(map[string]database.SimulationTrade)
	if simulation {
		run, runErr := s.db.SimulationRun()
		if runErr != nil {
			return nil, runErr
		}
		simTrades, simErr := s.db.SimulationTrades(run.StartedAt, time.Now().UTC().Add(time.Nanosecond))
		if simErr != nil {
			return nil, simErr
		}
		for _, trade := range simTrades {
			simNetByID[trade.ID] = trade
		}
	} else {
		liveTrades, liveErr := s.db.LiveTrades(time.Time{}, time.Now().UTC().Add(time.Nanosecond))
		if liveErr != nil {
			return nil, liveErr
		}
		for _, trade := range liveTrades {
			liveNetByID[trade.ID] = trade
		}
	}
	for rows.Next() {
		var id, mint, side, quantityText, signature, state, createdAt, exitReason string
		var requested, executed, fees, slippage float64
		var realized sql.NullFloat64
		if err := rows.Scan(&id, &mint, &side, &quantityText, &requested, &executed, &fees, &slippage, &signature, &state, &createdAt, &realized, &exitReason); err != nil {
			return nil, err
		}
		quantity, _ := strconv.ParseFloat(quantityText, 64)
		status := state
		if state == "simulated" || state == "finalized" {
			status = "confirmed"
		}
		if status != "pending" && status != "submitted" && status != "confirmed" && status != "failed" {
			status = "unknown"
		}
		item := map[string]any{
			"id": id, "timestamp": createdAt, "tokenName": nil, "symbol": nil, "mint": mint,
			"side": side, "quantity": quantity, "requestedValueSol": requested,
			"buyValueSol": nil, "sellValueSol": nil, "executedValueSol": nil, "feesSol": fees,
			"slippage": slippage, "grossPnlSol": nil, "netPnlSol": nil, "realizedPnlSol": nil,
			"exitReason": exitReason, "transactionId": nil, "signature": nil, "status": status,
		}
		if executed > 0 {
			item["executedValueSol"] = executed
		}
		value := requested
		if executed > 0 {
			value = executed
		}
		if side == "buy" {
			item["buyValueSol"] = value
		} else {
			item["sellValueSol"] = value
		}
		if realized.Valid {
			item["grossPnlSol"] = realized.Float64 + fees
			item["netPnlSol"] = realized.Float64
			item["realizedPnlSol"] = realized.Float64
			if simTrade, exists := simNetByID[id]; exists {
				item["grossPnlSol"] = simTrade.GrossPnLSOL
				item["netPnlSol"] = simTrade.NetPnLSOL
				item["realizedPnlSol"] = simTrade.NetPnLSOL
			} else if liveTrade, exists := liveNetByID[id]; exists {
				item["grossPnlSol"] = liveTrade.GrossPnLSOL
				item["netPnlSol"] = liveTrade.NetPnLSOL
				item["realizedPnlSol"] = liveTrade.NetPnLSOL
			}
		}
		if signature != "" {
			item["transactionId"] = signature
			item["signature"] = signature
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}
func (s *Server) cumulativeResult(simulation bool) ([]map[string]any, error) {
	if simulation {
		run, err := s.db.SimulationRun()
		if err != nil {
			return nil, err
		}
		trades, err := s.db.SimulationTrades(run.StartedAt, time.Now().UTC().Add(time.Nanosecond))
		if err != nil {
			return nil, err
		}
		result := make([]map[string]any, 0, len(trades))
		var cumulative float64
		for _, trade := range trades {
			cumulative += trade.NetPnLSOL
			result = append(result, map[string]any{"timestamp": trade.ConfirmedAt, "value": cumulative})
		}
		return result, nil
	}
	rows, err := s.db.RealizedPnlHistory()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []map[string]any{}
	var cumulative float64
	for rows.Next() {
		var timestamp string
		var pnl float64
		if err := rows.Scan(&timestamp, &pnl); err != nil {
			return nil, err
		}
		cumulative += pnl
		result = append(result, map[string]any{"timestamp": timestamp, "value": cumulative})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
func simulationBalanceHistory(run database.SimulationRun, trades []database.SimulationTrade, currentEquity float64, now time.Time) []map[string]any {
	history := make([]map[string]any, 0, len(trades)+2)
	history = append(history, map[string]any{"timestamp": run.StartedAt, "value": run.BalanceStartSOL})
	equity := run.BalanceStartSOL
	for _, trade := range trades {
		equity += trade.NetPnLSOL
		history = append(history, map[string]any{"timestamp": trade.ConfirmedAt, "value": equity})
	}
	history = append(history, map[string]any{"timestamp": now, "value": currentEquity})
	return history
}

func positionMarkAgeSeconds(markUpdated string, now time.Time) (float64, bool) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999-07",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		markedAt, err := time.Parse(layout, markUpdated)
		if err == nil {
			age := now.Sub(markedAt).Seconds()
			if age < 0 {
				age = 0
			}
			return age, true
		}
	}
	return 0, false
}
func (s *Server) mode() string {
	if s.cfg.Simulation {
		return "simulation"
	}
	return "real"
}
func (s *Server) balanceSOL(ctx context.Context) (float64, bool) {
	if s.cfg.Simulation {
		balance, err := s.db.SimulationAvailableBalanceSOL(s.engine.SimulationBalanceSOL())
		if err != nil || balance < 0 {
			return 0, false
		}
		return balance, true
	}
	if s.wallet.Address() == "" || !s.rpcs.Available() {
		return 0, false
	}
	balanceCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if lamports, err := s.rpcs.Balance(balanceCtx, s.wallet.Address()); err == nil {
		return float64(lamports) / 1e9, true
	}
	return 0, false
}
func (s *Server) rpcsList(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	write(w, 200, map[string]any{"items": s.rpcs.Endpoints(), "available": s.rpcs.Available()})
}
func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		write(w, 200, map[string]any{
			"simulation":             s.cfg.Simulation,
			"liveTradingEnabled":     s.cfg.EnableLiveTrading,
			"maxHoldSeconds":         s.cfg.MaxHoldSeconds,
			"effectiveMaxHoldSeconds": s.monitor.MaxHoldSeconds(),
			"limits":                 s.engine.Limits(),
		})
		return
	}
	if r.Method != "POST" {
		fail(w, 405, "method_not_allowed", "method not allowed")
		return
	}
	var l execution.Limits
	if !decode(w, r, &l) {
		return
	}
	if l.MaxTradeSOL < 0 || l.MaxExposureSOL < 0 || l.MaxDailyLossSOL < 0 || l.MaxSlippagePct < 0 || l.MaxPriorityFeeSOL < 0 || l.FeeReserveSOL < 0 || l.MaxOpenPositions < 1 || l.MaxWaitSeconds < 1 || l.MaxAttempts < 1 || l.MaxAttempts > 6 {
		fail(w, 400, "invalid_config", "limits must be non-negative; counts positive and submission attempts between 1 and 6")
		return
	}
	s.engine.SetLimits(l)
	b, _ := json.Marshal(l)
	_ = s.db.SetSetting("risk_limits", string(b))
	write(w, 200, map[string]any{"limits": l})
}
func (s *Server) walletInfo(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	result := map[string]any{"publicAddress": s.wallet.Address(), "balanceSol": nil, "feesReserveSol": s.engine.Limits().FeeReserveSOL, "tokenHoldings": []any{}, "syncStatus": "unavailable", "updatedAt": time.Now().UTC()}
	if s.cfg.Simulation {
		result["publicAddress"] = nil
		if balance, available := s.balanceSOL(r.Context()); available {
			result["balanceSol"] = balance
			result["syncStatus"] = "synced"
		}
	} else if balance, available := s.balanceSOL(r.Context()); available {
		result["balanceSol"] = balance
		result["syncStatus"] = "synced"
	}
	write(w, 200, result)
}
func (s *Server) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			fail(w, 401, "unauthorized", "administrator authentication required")
			return
		}
		h(w, r)
	}
}
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	var body struct {
		Password string `json:"password"`
	}
	if !decode(w, r, &body) {
		return
	}
	if subtle.ConstantTimeCompare([]byte(body.Password), []byte(s.cfg.AdminPassword)) != 1 {
		fail(w, http.StatusUnauthorized, "invalid_credentials", "invalid master password")
		return
	}
	write(w, http.StatusOK, map[string]string{"token": s.sessionToken()})
}
func (s *Server) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	return s.validSessionToken(token, time.Now().UTC())
}
func (s *Server) sessionToken() string {
	return s.sessionTokenAt(time.Now().UTC())
}
func (s *Server) sessionTokenAt(now time.Time) string {
	payload := strconv.FormatInt(now.UTC().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(s.cfg.AdminPassword))
	_, _ = mac.Write([]byte("solana-sniper-admin-session-v2:" + payload))
	token := payload + "." + hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(token))
}
func (s *Server) validSessionToken(token string, now time.Time) bool {
	if token == "" || len(token) > 256 {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return false
	}
	parts := strings.Split(string(decoded), ".")
	if len(parts) != 2 {
		return false
	}
	issuedAt, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return false
	}
	age := now.UTC().Unix() - issuedAt
	if age < -30 || age > int64(time.Hour/time.Second) {
		return false
	}
	mac := hmac.New(sha256.New, []byte(s.cfg.AdminPassword))
	_, _ = mac.Write([]byte("solana-sniper-admin-session-v2:" + parts[0]))
	expected := hex.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(parts[1]), []byte(expected)) == 1
}
func (s *Server) stop(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	s.controller.Stop()
	write(w, 200, map[string]any{"state": s.controller.State(), "positionsRetained": true})
}
func (s *Server) pause(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	s.controller.Pause()
	write(w, 200, map[string]any{"state": s.controller.State()})
}
func (s *Server) resume(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	s.controller.Resume()
	write(w, 200, map[string]any{"state": s.controller.State()})
}
func (s *Server) emergency(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	s.controller.EmergencyStop()
	write(w, 200, map[string]any{"state": s.controller.State(), "positionsRetained": true})
}

// cors handles browser preflight before routes perform method or auth checks.
// Origins are explicit: credentials are never combined with a wildcard origin.
func corsPolicy(next http.Handler) http.Handler {
	allowed := configuredOrigins()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if _, ok := allowed[origin]; ok {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			if _, ok := allowed[origin]; !ok {
				fail(w, http.StatusForbidden, "origin_not_allowed", "origin is not allowed")
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func configuredOrigins() map[string]struct{} {
	allowed := map[string]struct{}{
		"https://solana-sniper-bot-dashboard.vercel.app": {},
		"http://localhost:5173":                          {},
	}
	for _, origin := range strings.Split(os.Getenv("CORS_ALLOWED_ORIGINS"), ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			allowed[origin] = struct{}{}
		}
	}
	return allowed
}
func (s *Server) originAllowed(origin string) bool {
	_, ok := configuredOrigins()[origin]
	return ok
}
func (s *Server) rpcTest(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	var v struct {
		URL string `json:"url"`
	}
	if !decode(w, r, &v) {
		return
	}
	if v.URL == "" {
		fail(w, 400, "invalid_request", "url is required")
		return
	}
	ctx, c := context.WithTimeout(r.Context(), 10*time.Second)
	defer c()
	write(w, 200, s.rpcs.Test(ctx, v.URL))
}
func (s *Server) rpcSelect(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "POST") {
		return
	}
	var v struct {
		URL string `json:"url"`
	}
	if !decode(w, r, &v) {
		return
	}
	if e := s.rpcs.Select(v.URL); e != nil {
		fail(w, 409, "rpc_unavailable", e.Error())
		return
	}
	write(w, 200, map[string]any{"selected": v.URL})
}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if origin != "" && !s.originAllowed(origin) {
		fail(w, http.StatusForbidden, "origin_not_allowed", "origin is not allowed")
		return
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return origin == "" || s.originAllowed(origin)
	}}
	c, e := upgrader.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
	c.SetReadLimit(8192)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	messageType, authMessage, e := c.ReadMessage()
	var auth struct {
		Type  string `json:"type"`
		Token string `json:"token"`
	}
	if e != nil || messageType != websocket.TextMessage || json.Unmarshal(authMessage, &auth) != nil || auth.Type != "auth" || !s.validSessionToken(auth.Token, time.Now().UTC()) {
		_ = c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "authentication required"), time.Now().Add(time.Second))
		return
	}
	sub, unsubscribe := s.bus.Subscribe()
	defer unsubscribe()
	_ = c.SetReadDeadline(time.Now().Add(70 * time.Second))
	c.SetPongHandler(func(string) error { _ = c.SetReadDeadline(time.Now().Add(70 * time.Second)); return nil })
	go func() {
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
	if e = c.WriteJSON(events.Event{Type: "connection_ready", Data: map[string]any{"botState": s.controller.State(), "rpcAvailable": s.rpcs.Available(), "simulation": s.cfg.Simulation}, At: time.Now().UTC()}); e != nil {
		return
	}
	ping := time.NewTicker(25 * time.Second)
	defer ping.Stop()
	for {
		select {
		case e, ok := <-sub:
			if !ok {
				return
			}
			if err := c.WriteJSON(e); err != nil {
				return
			}
		case <-ping.C:
			if err := c.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		fail(w, 400, "invalid_request", "invalid JSON request")
		return false
	}
	return true
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}
