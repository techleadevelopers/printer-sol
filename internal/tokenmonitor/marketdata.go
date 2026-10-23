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
	pricePollWorkers = 5
)

type candidateState struct {
	event Event
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

func (m *Monitor) runCandidateChecker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, mint := range m.dueCandidates(time.Now().UTC()) {
				select {
				case m.validationSlots <- struct{}{}:
					go func(candidateMint string) {
						defer func() { <-m.validationSlots }()
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

func (m *Monitor) enqueueSnipe(event Event) (bool, string) {
	now := time.Now().UTC()
	ttl := m.dedupeTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	m.snipeMu.Lock()
	m.pruneSnipeDedupeLocked(now, ttl)
	if _, duplicate := m.snipePending[event.Mint]; duplicate {
		m.snipeMu.Unlock()
		return false, "duplicate snipe suppressed within dedupe TTL"
	}
	event.queuedAt = now
	m.snipePending[event.Mint] = now
	select {
	case m.q <- event:
		m.snipeMu.Unlock()
		return true, ""
	default:
		delete(m.snipePending, event.Mint)
		m.snipeMu.Unlock()
		return false, "token queue full after activity validation"
	}
}

func (m *Monitor) pruneSnipeDedupe(now time.Time) {
	ttl := m.dedupeTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	m.snipeMu.Lock()
	m.pruneSnipeDedupeLocked(now, ttl)
	m.snipeMu.Unlock()
}

func (m *Monitor) pruneSnipeDedupeLocked(now time.Time, ttl time.Duration) {
	for mint, queuedAt := range m.snipePending {
		if now.Sub(queuedAt) >= ttl {
			delete(m.snipePending, mint)
		}
	}
}

func (m *Monitor) creatorBlacklistReason(creator string) string {
	if strings.TrimSpace(creator) == "" {
		return ""
	}
	blocked, err := m.db.IsCreatorBlacklisted(creator)
	if err != nil {
		m.log.Warn("check creator stop-loss blacklist", zap.String("creator", creator), zap.Error(err))
		return "creator blacklist unavailable; entry blocked"
	}
	if blocked {
		m.log.Info("creator in blacklist", zap.String("creator", creator))
		return "creator in blacklist: blocked for 24 hours after a stop-loss"
	}
	return ""
}

func (m *Monitor) checkCandidate(parent context.Context, mint string) {
	generation := m.resetGeneration.Load()
	timeout := m.snipeTimeout
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	event, exists := m.candidateSnapshot(mint)
	if !exists {
		return
	}

	rpcCtx, rpcCancel := context.WithTimeout(ctx, 2*time.Second)
	defer rpcCancel()

	marketCapSOL, realSOLReservesSOL, err := m.readBondingCurveEntryMetrics(rpcCtx, mint)
	if parent.Err() != nil {
		return
	}
	m.runOperationAt(generation, func() {
		m.finishCandidate(mint, event, marketCapSOL, realSOLReservesSOL, err)
	})
}

func (m *Monitor) candidateSnapshot(mint string) (Event, bool) {
	m.candidateMu.Lock()
	defer m.candidateMu.Unlock()
	item := m.candidates[mint]
	if item == nil {
		return Event{}, false
	}
	return item.event, true
}

func (m *Monitor) finishCandidate(mint string, event Event, rpcMarketCapSOL, realSOLReservesSOL float64, rpcErr error) {
	m.candidateMu.Lock()
	item := m.candidates[mint]
	if item == nil {
		m.candidateMu.Unlock()
		return
	}
	delete(m.candidates, mint)
	m.candidateMu.Unlock()

	if rpcErr != nil {
		m.rejectCandidate(event, fmt.Sprintf("bonding-curve RPC could not verify real SOL reserves: %v", rpcErr))
		return
	}

	marketCapToCheck := rpcMarketCapSOL
	marketCapSource := "RPC"
	if m.useWSMarketCap {
		marketCapToCheck = event.MarketCapSOL
		marketCapSource = "WebSocket"
	}
	if marketCapToCheck <= 0 || math.IsNaN(marketCapToCheck) || math.IsInf(marketCapToCheck, 0) {
		m.rejectCandidate(event, fmt.Sprintf("%s market cap is missing or invalid", marketCapSource))
		return
	}
	event.MarketCapSOL = marketCapToCheck

	if m.maxMarketCapSOL > 0 && marketCapToCheck > m.maxMarketCapSOL {
		m.rejectCandidate(event, fmt.Sprintf("%s market cap %.6f SOL exceeds configured maximum %.6f SOL", marketCapSource, marketCapToCheck, m.maxMarketCapSOL))
		return
	}

	if marketCapToCheck < m.minMarketCapSOL {
		m.rejectCandidate(event, fmt.Sprintf("%s market cap %.6f SOL is below configured minimum %.6f SOL", marketCapSource, marketCapToCheck, m.minMarketCapSOL))
		return
	}

	if realSOLReservesSOL < m.minVolumeBeforeBuySOL {
		m.rejectCandidate(event, fmt.Sprintf("bonding-curve real SOL reserves %.6f SOL are below minimum %.6f SOL", realSOLReservesSOL, m.minVolumeBeforeBuySOL))
		return
	}
	if realSOLReservesSOL > m.maxVolumeBeforeBuySOL {
		m.rejectCandidate(event, fmt.Sprintf("bonding-curve real SOL reserves %.6f SOL exceed maximum %.6f SOL", realSOLReservesSOL, m.maxVolumeBeforeBuySOL))
		return
	}

	if reason := m.creatorBlacklistReason(event.Creator); reason != "" {
		m.rejectCandidate(event, reason)
		return
	}

	fields := []zap.Field{
		zap.String("mint", mint),
		zap.Float64("market_cap_sol", marketCapToCheck),
		zap.String("market_cap_source", marketCapSource),
	zap.Bool("reserve_gate_enabled", true),
	}
	fields = append(fields, zap.Float64("real_sol_reserves", realSOLReservesSOL))
	m.log.Info("entry approved", fields...)
	queued, enqueueReason := m.enqueueSnipe(event)
	if !queued {
		m.rejectCandidate(event, enqueueReason)
	}
}

func (m *Monitor) readBondingCurveEntryMetrics(ctx context.Context, mint string) (float64, float64, error) {
	if m.bondingCurve == nil {
		err := fmt.Errorf("bonding-curve RPC source is not configured")
		m.recordPriceSourceResult(err)
		return 0, 0, err
	}
	marketCapSOL, realSOLReservesSOL, err := m.bondingCurve.BondingCurveEntryMetrics(ctx, mint)
	if err == nil && !m.useWSMarketCap && (marketCapSOL <= 0 || math.IsNaN(marketCapSOL) || math.IsInf(marketCapSOL, 0)) {
		err = fmt.Errorf("bonding-curve RPC market cap is not positive and finite")
	}
	if err == nil && (realSOLReservesSOL < 0 || math.IsNaN(realSOLReservesSOL) || math.IsInf(realSOLReservesSOL, 0)) {
		err = fmt.Errorf("bonding-curve real SOL reserves are invalid")
	}
	m.recordPriceSourceResult(err)
	if err != nil {
		return 0, 0, err
	}
	return marketCapSOL, realSOLReservesSOL, nil
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

		message := "price refresh failing"
		detail := "Solana RPC fallback for stale position marks is unavailable; WebSocket marks remain usable when new trades arrive"
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
if m.useWSMarketCap {
staleBefore := time.Now().UTC().Add(-m.pricePollInterval)
mints, scanErr = m.db.OpenPositionMintsWithStaleMarks(m.simulation, staleBefore)
} else {
mints, scanErr = m.db.OpenPositionMints()
}
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