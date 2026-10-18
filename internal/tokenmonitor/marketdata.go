package tokenmonitor

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	candidateCheckWorkers      = 8
	pricePollWorkers           = 5
)

type candidateState struct {
	event             Event
	tradeEvents       int
	tradeMarketCapSOL float64
}

func (m *Monitor) addCandidate(event Event) bool {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	if m.candidates == nil {
		m.candidates = make(map[string]*candidate)
	}
	if _, exists := m.candidates[event.Mint]; exists {
		return false
	}
	if len(m.candidates) >= cap(m.q) {
		return false
	}
	detectedAt := event.Received
	if detectedAt.IsZero() {
		detectedAt = time.Now().UTC()
	}
	m.candidates[event.Mint] = &candidate{event: event, detectedAt: detectedAt}
	return true
}

// recordCandidateTrade consumes the candidate's own PumpPortal subscription.
// It counts only actual buy/sell trade frames, never the creation announcement.
func (m *Monitor) recordCandidateTrade(mint, txType string, marketCapSOL float64) bool {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	item, exists := m.candidates[mint]
	if !exists {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(txType)) {
	case "buy", "sell", "trade":
		item.tradeEvents++
		if marketCapSOL > 0 {
			item.tradeMarketCapSOL = marketCapSOL
		}
	}
	return true
}

func (m *Monitor) runCandidateChecker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	workers := make(chan struct{}, candidateCheckWorkers)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, mint := range m.dueCandidates(time.Now().UTC()) {
				select {
				case workers <- struct{}{}:
					go func(candidateMint string) {
						defer func() { <-workers }()
						m.checkCandidate(ctx, candidateMint)
					}(mint)
				default:
					m.uncheckCandidate(mint)
				}
			}
		}
	}
}

func (m *Monitor) dueCandidates(now time.Time) []string {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	mints := make([]string, 0)
	for mint, item := range m.candidates {
		if !item.checking && now.Sub(item.detectedAt) >= m.entryDelay {
			item.checking = true
			mints = append(mints, mint)
		}
	}
	return mints
}

func (m *Monitor) uncheckCandidate(mint string) {
	m.candidateMu.Lock()
	if item := m.candidates[mint]; item != nil {
		item.checking = false
	}
	m.candidateMu.Unlock()
}

func (m *Monitor) checkCandidate(parent context.Context, mint string) {
	generation := m.resetGeneration.Load()
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	state, exists := m.candidateSnapshot(mint)
	if !exists {
		return
	}
	marketCapSOL := state.tradeMarketCapSOL
	if state.tradeEvents >= m.minTransactions && marketCapSOL <= 0 {
		var err error
		marketCapSOL, err = m.readBondingCurveMarketCap(ctx, mint)
		if err != nil {
			m.log.Debug("candidate bonding-curve market-cap lookup failed",
				zap.String("mint", mint), zap.Error(err))
		}
	}
	if parent.Err() != nil {
		return
	}
	m.runOperationAt(generation, func() {
		m.finishCandidate(mint, marketCapSOL)
	})
}

func (m *Monitor) candidateSnapshot(mint string) (candidateState, bool) {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	item := m.candidates[mint]
	if item == nil {
		return candidateState{}, false
	}
	return candidateState{
		event: item.event, tradeEvents: item.tradeEvents,
		tradeMarketCapSOL: item.tradeMarketCapSOL,
	}, true
}

func (m *Monitor) finishCandidate(mint string, rpcMarketCapSOL float64) {
	m.candidateMu.Lock()
	item := m.candidates[mint]
	if item == nil {
		m.candidateMu.Unlock()
		return
	}
	state := candidateState{
		event: item.event,
		tradeEvents: item.tradeEvents, tradeMarketCapSOL: item.tradeMarketCapSOL,
	}
	delete(m.candidates, mint)
	m.candidateMu.Unlock()

	accepted, reason := m.websocketCandidateFallback(state)
	marketCapSOL := state.tradeMarketCapSOL
	if marketCapSOL <= 0 {
		marketCapSOL = rpcMarketCapSOL
	}
	if accepted && marketCapSOL <= 0 {
		accepted = false
		reason = "candidate has no positive WebSocket or bonding-curve market cap"
	}
	if !accepted {
		m.rejectCandidate(state.event, reason)
		return
	}
	if marketCapSOL <= 0 {
		m.rejectCandidate(state.event, "candidate has no positive market cap")
		return
	}
	state.event.MarketCapSOL = marketCapSOL
	select {
	case m.q <- state.event:
	default:
		m.rejectCandidate(state.event, "token queue full after activity validation")
		m.log.Warn("validated token queue full", zap.String("mint", mint))
	}
}

func (m *Monitor) websocketCandidateFallback(state candidateState) (bool, string) {
	if state.tradeEvents < m.minTransactions {
		return false, fmt.Sprintf("fewer than %d WebSocket trades in the entry window", m.minTransactions)
	}
	return true, "WebSocket activity fallback accepted"
}

func (m *Monitor) bondingCurveMarketCapSOL(ctx context.Context, mint string) (float64, error) {
	if m.bondingCurve == nil {
		return 0, fmt.Errorf("bonding-curve market-cap source is not configured")
	}
	marketCapSOL, err := m.bondingCurve.BondingCurveMarketCapSOL(ctx, mint)
	if err != nil {
		return 0, err
	}
	if marketCapSOL <= 0 || math.IsNaN(marketCapSOL) || math.IsInf(marketCapSOL, 0) {
		return 0, fmt.Errorf("bonding-curve market cap is not positive and finite")
	}
	return marketCapSOL, nil
}

func (m *Monitor) readBondingCurveMarketCap(ctx context.Context, mint string) (float64, error) {
	marketCapSOL, err := m.bondingCurveMarketCapSOL(ctx, mint)
	m.recordPriceSourceResult(err)
	if err != nil {
		return 0, err
	}
	m.log.Info("polling source=solana_rpc_bonding_curve",
		zap.String("mint", mint), zap.Float64("mcap", marketCapSOL))
	return marketCapSOL, nil
}

func (m *Monitor) recordPriceSourceResult(err error) {
	m.priceSourceStatusMu.Lock()
	if err == nil {
		m.priceSourceFailures = 0
		m.priceSourceAlerted = false
		m.priceSourceStatusMu.Unlock()
		return
	}
	m.priceSourceFailures++
	failures := m.priceSourceFailures
	shouldAlert := failures >= 3 && !m.priceSourceAlerted
	if shouldAlert {
		m.priceSourceAlerted = true
	}
	m.priceSourceStatusMu.Unlock()
	if !shouldAlert {
		return
	}

	message := "polling failing"
	detail := "Solana RPC bonding-curve market-cap source unavailable; valid WebSocket marks remain usable"
	m.log.Warn("polling failing: Solana RPC bonding-curve price feed degraded",
		zap.Int("consecutiveFailures", failures), zap.Error(err))
	if m.db != nil {
		payload := fmt.Sprintf(`{"component":"solana_bonding_curve_rpc","consecutiveFailures":%d,"detail":"%s"}`, failures, detail)
		if logErr := m.db.LogOperational("warn", "risk_alert", message, payload); logErr != nil {
			m.log.Warn("persist public price feed risk alert", zap.Error(logErr))
		}
	}
	if m.bus != nil {
		m.bus.Publish("risk.alert", map[string]any{
			"reason": message, "component": "solana_bonding_curve_rpc",
			"consecutiveFailures": failures, "detail": detail + ": " + err.Error(),
		})
	}
}

func (m *Monitor) rejectCandidate(event Event, reason string) {
	if event.Received.IsZero() {
		event.Received = time.Now().UTC()
	}
	latency := time.Since(event.Received).Milliseconds()
	if err := m.db.RecordEvent(event.Mint, event.Creator, event.Raw, event.Received, latency); err != nil {
		m.log.Warn("record rejected candidate event", zap.String("mint", event.Mint), zap.Error(err))
	}
	if err := m.db.RecordAssessment(event.Mint, false, reason); err != nil {
		m.log.Warn("record rejected candidate assessment", zap.String("mint", event.Mint), zap.Error(err))
	}
	m.log.Info("token rejected by entry activity gate", zap.String("mint", event.Mint), zap.String("reason", reason))
	m.bus.Publish("token_rejected", map[string]any{
		"mint": event.Mint, "reason": reason, "latencyMs": latency,
	})
}

func (m *Monitor) runPricePoller(ctx context.Context) {
	ticker := time.NewTicker(m.pricePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.pollOpenPositionPrices(ctx)
			ticker.Reset(m.pricePollInterval)
		}
	}
}

func (m *Monitor) pollOpenPositionPrices(ctx context.Context) {
	generation := m.resetGeneration.Load()
	var mints []string
	var scanErr error
	if !m.runOperationAt(generation, func() {
		mints, scanErr = m.db.OpenPositionMints()
	}) {
		return
	}
	if scanErr != nil {
		m.log.Warn("scan open positions for RPC mark refresh", zap.Error(scanErr))
		return
	}
	workers := make(chan struct{}, pricePollWorkers)
	var wait sync.WaitGroup
	for _, mint := range mints {
		select {
		case <-ctx.Done():
			wait.Wait()
			return
		case workers <- struct{}{}:
		}
		wait.Add(1)
		go func(positionMint string) {
			defer wait.Done()
			defer func() { <-workers }()
			m.refreshOpenPositionPrice(ctx, positionMint, generation)
		}(mint)
	}
	wait.Wait()
}

func (m *Monitor) refreshOpenPositionPrice(ctx context.Context, mint string, generation uint64) {
	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	m.refreshBondingCurveMarketCap(requestCtx, mint, generation)
}

func (m *Monitor) refreshBondingCurveMarketCap(ctx context.Context, mint string, generation uint64) bool {
	marketCapSOL, err := m.readBondingCurveMarketCap(ctx, mint)
	if err != nil {
		if m.bondingCurve != nil {
			m.log.Debug("bonding-curve market-cap refresh failed",
				zap.String("mint", mint), zap.Error(err))
		}
		return false
	}
	m.log.Info("polling source=solana_rpc_bonding_curve",
		zap.String("mint", mint), zap.Float64("mcap", marketCapSOL))
	m.markTradeWithSourceAt(generation, ctx, mint, marketCapSOL, "solana_bonding_curve_rpc")
	return true
}
