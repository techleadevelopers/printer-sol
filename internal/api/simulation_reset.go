package api

import (
	"errors"
	"net/http"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/wallet"
)

func (s *Server) simulationReset(w http.ResponseWriter, r *http.Request) {
	if !method(w, r, http.MethodPost) {
		return
	}
	if !s.cfg.Simulation {
		fail(w, http.StatusBadRequest, "simulation_only", "reset is simulation-only")
		return
	}
	if s.db == nil || s.engine == nil || s.controller == nil || s.monitor == nil {
		fail(w, http.StatusServiceUnavailable, "simulation_unavailable", "simulation reset is unavailable")
		return
	}

	var request struct {
		BalanceSOL *float64 `json:"balance_sol"`
	}
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 && !decode(w, r, &request) {
		return
	}

	s.resetMu.Lock()
	defer s.resetMu.Unlock()
	now := time.Now().UTC()
	if !s.lastReset.IsZero() && now.Sub(s.lastReset) < 30*time.Second {
		fail(w, http.StatusTooManyRequests, "reset_rate_limited", "aguarde 30 segundos antes de resetar novamente")
		return
	}

	balanceSOL := s.cfg.SimulationBalanceSOL
	if request.BalanceSOL != nil {
		balanceSOL = *request.BalanceSOL
	}
	if _, err := wallet.SOLToLamports(balanceSOL); err != nil {
		fail(w, http.StatusBadRequest, "invalid_balance", "balance_sol must be a finite positive SOL amount")
		return
	}

	previousState := s.controller.State()

	deletedTrades, deletedPositions := 0, 0
	err := s.monitor.WithReset(func() error {
		return s.engine.WithSimulationReset(balanceSOL, func() error {
			var resetErr error
			deletedTrades, deletedPositions, resetErr = s.db.ResetSimulation(r.Context(), balanceSOL)
			return resetErr
		})
	})
	if err != nil {
		if errors.Is(err, database.ErrSimulationResetPending) {
			fail(w, http.StatusConflict, "reconciliation_pending", database.ErrSimulationResetPending.Error())
			return
		}
		s.log.Warn("simulation reset failed", zap.Error(err))
		fail(w, http.StatusInternalServerError, "simulation_reset_failed", "could not reset simulation")
		return
	}
	s.lastReset = time.Now().UTC()
	write(w, http.StatusOK, map[string]any{
		"accepted":        true,
		"balanceSol":      balanceSOL,
		"deletedTrades":   deletedTrades,
		"deletedPositions": deletedPositions,
		"botStatus":       previousState,
	})
}
