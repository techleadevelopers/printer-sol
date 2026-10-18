package tokenmonitor

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/pumpportal"
)

const (
	fallbackMinimumTradeEvents = 2
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
	var snapshot pumpportal.CoinSnapshot
	var apiErr error
	if m.coins != nil {
		snapshot, apiErr = m.coins.Coin(ctx, mint)
	} else {
		apiErr = context.DeadlineExceeded
	}
	if parent.Err() != nil {
		return
	}
	m.recordCoinAPIResult(apiErr)
	m.runOperationAt(generation, func() {
		m.finishCandidate(ctx, mint, snapshot, apiErr)
	})
}

func (m *Monitor) finishCandidate(ctx context.Context, mint string, coin pumpportal.CoinSnapshot, apiErr error) {
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

	accepted := false
	reason := ""
	marketCapSOL := 0.0
	if apiErr == nil {
		activityAvailable := coin.HasTransactions && coin.HasVolume
		if coin.HasTransactions && coin.Transactions < m.minTransactions {
			reason = "Pump.fun activity below MIN_TXNS_BEFORE_BUY"
		}
		if reason == "" && coin.HasVolume && coin.Volume < m.minVolume {
			reason = "Pump.fun volume below MIN_VOLUME_BEFORE_BUY"
		}
		if reason == "" {
			if coin.HasMarketCapSOL || coin.HasMarketCapUSD {
				var valid bool
				marketCapSOL, valid = m.coinMarketCapSOL(ctx, coin)
				if !valid {
					reason = "Pump.fun reported no positive market cap; candidate rejected"
				}
			} else {
				marketCapSOL = state.tradeMarketCapSOL
			}
		}

		if reason == "" && activityAvailable {
			if marketCapSOL <= 0 {
				reason = "candidate has no positive market cap"
			} else {
				accepted = true
			}
		} else if reason == "" {
			accepted, reason = m.websocketCandidateFallback(state)
		}
	} else {
		accepted, reason = m.websocketCandidateFallback(state)
		marketCapSOL = state.tradeMarketCapSOL
	}

	if !accepted {
		if reason == "" {
			reason = "candidate did not meet the minimum activity and positive-price requirements"
		}
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
	if state.tradeEvents < fallbackMinimumTradeEvents {
		return false, "Pump.fun API unavailable or incomplete; fewer than two WebSocket trades in the entry window"
	}
	if state.tradeMarketCapSOL <= 0 {
		return false, "WebSocket fallback has no positive trade market cap"
	}
	return true, "WebSocket activity fallback accepted"
}

func (m *Monitor) recordCoinAPIResult(err error) {
	m.coinAPIStatusMu.Lock()
	if err == nil {
		m.coinAPIFailures = 0
		m.coinAPIAlerted = false
		m.coinAPIStatusMu.Unlock()
		return
	}
	m.coinAPIFailures++
	failures := m.coinAPIFailures
	shouldAlert := failures >= 3 && !m.coinAPIAlerted
	if shouldAlert {
		m.coinAPIAlerted = true
	}
	m.coinAPIStatusMu.Unlock()
	if !shouldAlert {
		return
	}

	message := "polling failing"
	detail := "Fallback de preço falhando - bot operando com dados do WS apenas"
	m.log.Warn("polling failing: Pump.fun public price feed degraded",
		zap.Int("consecutiveFailures", failures), zap.Error(err))
	if m.db != nil {
		payload := fmt.Sprintf(`{"component":"pump_fun_coin_api","consecutiveFailures":%d,"detail":"%s"}`, failures, detail)
		if logErr := m.db.LogOperational("warn", "risk_alert", message, payload); logErr != nil {
			m.log.Warn("persist public price feed risk alert", zap.Error(logErr))
		}
	}
	if m.bus != nil {
		m.bus.Publish("risk.alert", map[string]any{
			"reason": message, "component": "pump_fun_coin_api",
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

func (m *Monitor) coinMarketCapSOL(ctx context.Context, coin pumpportal.CoinSnapshot) (float64, bool) {
	if coin.HasMarketCapSOL {
		return coin.MarketCapSOL, coin.MarketCapSOL > 0 && !math.IsNaN(coin.MarketCapSOL) && !math.IsInf(coin.MarketCapSOL, 0)
	}
	if !coin.HasMarketCapUSD || coin.MarketCapUSD <= 0 || m.solUSD == nil {
		return 0, false
	}
	solUSD, err := m.solUSD.SOLUSD(ctx)
	if err != nil || solUSD <= 0 {
		return 0, false
	}
	marketCapSOL := coin.MarketCapUSD / solUSD
	return marketCapSOL, marketCapSOL > 0 && !math.IsNaN(marketCapSOL) && !math.IsInf(marketCapSOL, 0)
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
			nextInterval := m.pricePollInterval
			if limiter, ok := m.coins.(interface{ RateLimitDelay() time.Duration }); ok {
				if retryAfter := limiter.RateLimitDelay(); retryAfter > nextInterval {
					nextInterval = retryAfter
				}
			}
			ticker.Reset(nextInterval)
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
		m.log.Warn("scan open positions for public price refresh", zap.Error(scanErr))
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
	if m.coins == nil {
		m.recordCoinAPIResult(fmt.Errorf("Pump.fun coin API client is not configured"))
		return
	}
	requestCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	coin, err := m.coins.Coin(requestCtx, mint)
	if err != nil {
		m.recordCoinAPIResult(err)
		m.log.Debug("public market-cap refresh failed", zap.String("mint", mint), zap.Error(err))
		return
	}
	marketCapSOL, valid := m.coinMarketCapSOL(requestCtx, coin)
	if !valid || marketCapSOL <= 0 {
		m.recordCoinAPIResult(fmt.Errorf("Pump.fun response has no usable positive market cap"))
		m.log.Debug("public market-cap refresh returned no positive mark", zap.String("mint", mint))
		return
	}
	m.recordCoinAPIResult(nil)
	m.markTradeWithSourceAt(generation, requestCtx, mint, marketCapSOL, "pump_fun_public_coin_api")
}
