package api

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/http"
	"solana-sniper-backend/internal/bot"
	"solana-sniper-backend/internal/config"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/execution"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/tokenmonitor"
	"solana-sniper-backend/internal/wallet"
	"strings"
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
	cfg        config.Config
	log        *zap.Logger
}

func New(db *database.DB, w *wallet.Wallet, r *rpcmanager.Manager, e *execution.Engine, c *bot.Controller, m *tokenmonitor.Monitor, b *events.Bus, cfg config.Config, l *zap.Logger) http.Handler {
	s := &Server{db: db, wallet: w, rpcs: r, engine: e, controller: c, monitor: m, bus: b, cfg: cfg, log: l}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", s.health)
	mux.HandleFunc("/api/auth/login", s.login)
	mux.HandleFunc("/api/dashboard", s.dashboard)
	mux.HandleFunc("/api/tokens", s.tokens)
	mux.HandleFunc("/api/positions", s.positions)
	mux.HandleFunc("/api/trades", s.trades)
	mux.HandleFunc("/api/rpcs", s.rpcsList)
	mux.HandleFunc("/api/config", s.config)
	mux.HandleFunc("/api/wallet", s.walletInfo)
	mux.HandleFunc("/api/bot/stop", s.admin(s.stop))
	mux.HandleFunc("/api/bot/pause", s.admin(s.pause))
	mux.HandleFunc("/api/bot/resume", s.admin(s.resume))
	mux.HandleFunc("/api/bot/emergency-stop", s.admin(s.emergency))
	mux.HandleFunc("/api/rpcs/test", s.admin(s.rpcTest))
	mux.HandleFunc("/api/rpcs/select", s.admin(s.rpcSelect))
	mux.HandleFunc("/ws/events", s.ws)
	return cors(mux)
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
	trading := s.rpcs.Available() && (s.cfg.Simulation || s.cfg.EnableLiveTrading)
	write(w, 200, map[string]any{
		"service": map[string]any{"active": true},
		"monitor": map[string]any{"connected": s.monitor != nil && s.monitor.Connected()},
		"trading": map[string]any{"available": trading, "simulation": s.cfg.Simulation, "botState": s.controller.State()},
		"at":      time.Now().UTC(),
	})
}
func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	positions, _ := s.db.OpenPositionCount()
	unknown, _ := s.db.UnknownTradeCount()
	exposure, _ := s.db.OpenExposureSOL()
	loss, _ := s.db.DailyRealizedLossSOL()
	write(w, 200, map[string]any{"botState": s.controller.State(), "simulation": s.cfg.Simulation, "openPositions": positions, "openExposureSol": exposure, "dailyLossSol": loss, "reconciliationRequired": unknown > 0, "rpcAvailable": s.rpcs.Available()})
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
		var mint, creator, at string
		var latency int64
		if rows.Scan(&mint, &creator, &at, &latency) == nil {
			out = append(out, map[string]any{"mint": mint, "creator": creator, "receivedAt": at, "latencyMs": latency})
		}
	}
	write(w, 200, map[string]any{"items": out})
}
func (s *Server) positions(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	rows, e := s.db.ListPositions()
	if e != nil {
		fail(w, 500, "database_unavailable", "database unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var mint, q, status, opened, updated string
		var cost float64
		if rows.Scan(&mint, &q, &cost, &status, &opened, &updated) == nil {
			out = append(out, map[string]any{"mint": mint, "quantity": q, "costSol": cost, "status": status, "openedAt": opened, "updatedAt": updated})
		}
	}
	write(w, 200, map[string]any{"items": out})
}
func (s *Server) trades(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	rows, e := s.db.ListTrades()
	if e != nil {
		fail(w, 500, "database_unavailable", "database unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, mint, side, q, sig, state string
		var req, done, fees, slip float64
		if rows.Scan(&id, &mint, &side, &q, &req, &done, &fees, &slip, &sig, &state) == nil {
			out = append(out, map[string]any{"id": id, "mint": mint, "side": side, "quantity": q, "requestedValueSol": req, "executedValueSol": done, "feesSol": fees, "slippagePct": slip, "signature": sig, "state": state})
		}
	}
	write(w, 200, map[string]any{"items": out})
}
func (s *Server) rpcsList(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, "GET") {
		return
	}
	write(w, 200, map[string]any{"items": s.rpcs.Endpoints(), "available": s.rpcs.Available()})
}
func (s *Server) config(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		write(w, 200, map[string]any{"simulation": s.cfg.Simulation, "liveTradingEnabled": s.cfg.EnableLiveTrading, "limits": s.engine.Limits()})
		return
	}
	if r.Method != "POST" {
		fail(w, 405, "method_not_allowed", "method not allowed")
		return
	}
	if !s.authorized(r) {
		fail(w, 401, "unauthorized", "administrator authentication required")
		return
	}
	var l execution.Limits
	if !decode(w, r, &l) {
		return
	}
	if l.MaxTradeSOL < 0 || l.MaxExposureSOL < 0 || l.MaxDailyLossSOL < 0 || l.MaxSlippagePct < 0 || l.MaxPriorityFeeSOL < 0 || l.FeeReserveSOL < 0 || l.MaxOpenPositions < 1 || l.MaxWaitSeconds < 1 || l.MaxAttempts < 1 {
		fail(w, 400, "invalid_config", "limits must be non-negative and counts positive")
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
	result := map[string]any{"address": s.wallet.Address(), "simulation": s.cfg.Simulation, "feeReserveSol": s.engine.Limits().FeeReserveSOL}
	if s.wallet.Address() != "" && s.rpcs.Available() {
		ctx, c := context.WithTimeout(r.Context(), 5*time.Second)
		defer c()
		if bal, e := s.rpcs.Balance(ctx, s.wallet.Address()); e == nil {
			result["balanceLamports"] = bal
			result["balanceSol"] = float64(bal) / 1e9
		} else {
			result["balanceError"] = e.Error()
		}
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
	if !method(w, r, "POST") { return }
	var body struct { Password string `json:"password"` }
	if !decode(w, r, &body) { return }
	if subtle.ConstantTimeCompare([]byte(body.Password), []byte(s.cfg.AdminPassword)) != 1 {
		fail(w, http.StatusUnauthorized, "invalid_credentials", "invalid master password")
		return
	}
	write(w, http.StatusOK, map[string]string{"token": s.sessionToken()})
}
func (s *Server) authorized(r *http.Request) bool {
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return token != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.sessionToken())) == 1
}
func (s *Server) sessionToken() string {
	mac := hmac.New(sha256.New, []byte(s.cfg.AdminPassword))
	_, _ = mac.Write([]byte("solana-sniper-admin-session-v1"))
	return hex.EncodeToString(mac.Sum(nil))
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

var up = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func (s *Server) ws(w http.ResponseWriter, r *http.Request) {
	c, e := up.Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
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
	_ = json.NewEncoder(w).Encode(map[string]any{"data": v})
}
func fail(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}
		next.ServeHTTP(w, r)
	})
}
