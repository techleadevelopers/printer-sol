package tokenmonitor

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"net/url"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/positions"
	"solana-sniper-backend/internal/strategy"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Event struct {
	Mint         string `json:"mint"`
	Creator      string `json:"creator"`
	User         string `json:"user"`
	Received     time.Time
	Created      time.Time
	MarketCapSOL float64
	Raw          string
}

// Entrier is implemented by the execution engine. Keeping the dependency as
// an interface lets discovery remain independent from transaction signing.
type Entrier interface {
	BuyNewTokenUSD(context.Context, string, float64, float64, float64, string, string) (string, error)
	ClosePaperPosition(context.Context, string, float64, string) (string, error)
	CloseLivePosition(context.Context, string, float64, float64, string) (string, error)
}
type unknownTradeReconciler interface {
	ReconcileUnknown(context.Context) error
}
type Monitor struct {
	ws, key                         string
	q                               chan Event
	db                              *database.DB
	strategy                        *strategy.Strategy
	bus                             *events.Bus
	log                             *zap.Logger
	entry                           Entrier
	tradeUSD, slippage, priorityFee float64
	takeProfit, stopLoss            float64
	trailingStop                    float64
	maxHold                         time.Duration
	simulation                      bool
	asyncRiskScreen                 bool
	subscriptions                   chan string
	tracked                         sync.Map
	seenMu                          sync.Mutex
	seen                            map[string]time.Time
	connected                       atomic.Bool
	exitMu                          sync.Mutex
	exitRetryAfter                  sync.Map
	exitIntents                     sync.Map
}

const exitRetryDelay = 5 * time.Second

func New(ws, key string, size int, db *database.DB, s *strategy.Strategy, entry Entrier, tradeUSD, slippage, priorityFee, takeProfit, stopLoss float64, maxHold time.Duration, simulation bool, b *events.Bus, l *zap.Logger) *Monitor {
	const maximumPositionHold = 5 * time.Minute
	if maxHold <= 0 || maxHold > maximumPositionHold {
		maxHold = maximumPositionHold
	}
	return &Monitor{ws: ws, key: key, q: make(chan Event, size), db: db, strategy: s, entry: entry, tradeUSD: tradeUSD, slippage: slippage, priorityFee: priorityFee, takeProfit: takeProfit, stopLoss: stopLoss, trailingStop: 15, maxHold: maxHold, simulation: simulation, subscriptions: make(chan string, size), seen: make(map[string]time.Time), bus: b, log: l}
}
func (m *Monitor) SetTrailingStopPct(pct float64) {
	if pct > 0 {
		m.trailingStop = pct
	}
}
func (m *Monitor) SetAsyncRugCheckAfterEntry(enabled bool) { m.asyncRiskScreen = enabled }
func (m *Monitor) Run(ctx context.Context) {
	// The selected database is mode-isolated, so restoring its open positions
	// keeps live and paper marks separate while resuming exits after a restart.
	m.restoreOpenPositions()
	go m.consume(ctx)
	go m.runMaxHoldChecker(ctx)
	go m.runSeenJanitor(ctx)
	m.runConnections(ctx)
}

func (m *Monitor) runSeenJanitor(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.pruneSeen(now)
		}
	}
}

func (m *Monitor) pruneSeen(now time.Time) {
	cutoff := now.Add(-30 * time.Minute)
	m.seenMu.Lock()
	for mint, seenAt := range m.seen {
		if seenAt.Before(cutoff) {
			delete(m.seen, mint)
		}
	}
	m.seenMu.Unlock()
}

func (m *Monitor) rememberMint(mint string, now time.Time) bool {
	m.seenMu.Lock()
	defer m.seenMu.Unlock()
	if seenAt, exists := m.seen[mint]; exists && now.Sub(seenAt) < 30*time.Minute {
		return false
	}
	m.seen[mint] = now
	return true
}

func (m *Monitor) forgetMint(mint string) {
	m.seenMu.Lock()
	delete(m.seen, mint)
	m.seenMu.Unlock()
}

func (m *Monitor) runMaxHoldChecker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	reconcileTicker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer reconcileTicker.Stop()
	if ctx.Err() != nil {
		return
	}
	m.reconcileUnknownTrades(ctx)
	m.expireOpenPositions(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.expireOpenPositions(ctx)
		case <-reconcileTicker.C:
			m.reconcileUnknownTrades(ctx)
			m.expireOpenPositions(ctx)
		}
	}
}

func (m *Monitor) reconcileUnknownTrades(ctx context.Context) {
	reconciler, ok := m.entry.(unknownTradeReconciler)
	if !ok {
		return
	}
	if err := reconciler.ReconcileUnknown(ctx); err != nil {
		m.log.Warn("reconcile unknown live trades", zap.Error(err))
	}
}

func (m *Monitor) expireOpenPositions(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	mints, err := m.db.OpenPositionMints()
	if err != nil {
		m.log.Warn("scan positions for max-hold exits", zap.Error(err))
		return
	}
	now := time.Now().UTC()
	openMints := make(map[string]struct{}, len(mints))
	for _, mint := range mints {
		openMints[mint] = struct{}{}
		if _, tracked := m.tracked.Load(mint); !tracked {
			select {
			case m.subscriptions <- mint:
				m.tracked.Store(mint, struct{}{})
			default:
				m.log.Warn("open-position subscription queue full", zap.String("mint", mint))
			}
		}
	}
	m.tracked.Range(func(key, _ any) bool {
		mint, ok := key.(string)
		if !ok {
			return true
		}
		if _, open := openMints[mint]; open {
			return true
		}
		active, activeErr := m.db.ActiveForMint(mint)
		if activeErr == nil && !active {
			m.tracked.Delete(mint)
		}
		return true
	})
	m.exitRetryAfter.Range(func(key, _ any) bool {
		mint, ok := key.(string)
		if ok {
			if _, stillOpen := openMints[mint]; !stillOpen {
				m.exitRetryAfter.Delete(mint)
				m.exitIntents.Delete(mint)
			}
		}
		return true
	})
	for _, mint := range mints {
		if ctx.Err() != nil {
			return
		}
		openedAt, open, err := m.db.PositionOpenedAt(mint)
		if err != nil {
			m.log.Warn("read position age", zap.String("mint", mint), zap.Error(err))
			continue
		}
		if !open {
			continue
		}

		holdExpired := now.Sub(openedAt) >= m.maxHold
		var returnPct float64
		var exitMarketCap float64
		var known bool
		if m.simulation {
			returnPct, known, err = m.db.PaperReturnPct(mint)
		} else {
			returnPct, known, err = m.db.LiveReturnPct(mint)
		}
		_, exitMarketCap, _, _, _ = m.db.MarkMarketCaps(mint, m.simulation)
		if err != nil {
			m.log.Warn("read last position mark; using safe timed-exit fallback", zap.String("mint", mint), zap.Error(err))
			returnPct = 0
		} else if !known {
			// A timed-out paper position with no recorded price is closed at
			// its entry value; live exits sell the full confirmed token balance.
			returnPct = 0
		}
		failedExit, failedExitErr := m.db.HasFailedSellForMint(mint)
		if failedExitErr != nil {
			m.log.Warn("check previous exit failures", zap.String("mint", mint), zap.Error(failedExitErr))
		}
		reason := ""
		exitIntent, hasExitIntent := m.exitIntents.Load(mint)
		switch {
		case hasExitIntent:
			reason = exitIntent.(string)
		case failedExitErr == nil && failedExit:
			reason = "retry failed exit"
		case known:
			reason = m.positionExitReason(mint, returnPct, openedAt, now)
		case holdExpired:
			switch {
			case !m.simulation:
				reason = "max hold time"
			case now.Sub(openedAt)-m.maxHold >= m.maxHold:
				reason = "max hold time (extended)"
			}
		}
		if reason != "" {
			m.submitExit(ctx, mint, exitMarketCap, returnPct, reason)
		}
	}
}

func (m *Monitor) restoreOpenPositions() {
	mints, err := m.db.OpenPositionMints()
	if err != nil {
		m.log.Warn("restore simulated positions", zap.Error(err))
		return
	}
	for _, mint := range mints {
		m.tracked.Store(mint, struct{}{})
	}
	if len(mints) > 0 {
		m.log.Info("restored open positions", zap.Int("count", len(mints)), zap.Bool("simulation", m.simulation))
	}
}
func (m *Monitor) runConnections(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := m.connect(ctx); err != nil {
			m.log.Warn("token websocket disconnected", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		} else {
			backoff = time.Second
		}
	}
}
func (m *Monitor) Connected() bool { return m.connected.Load() }
func (m *Monitor) connect(ctx context.Context) error {
	u, _ := url.Parse(m.ws)
	q := u.Query()
	if m.key != "" {
		q.Set("api-key", m.key)
	}
	u.RawQuery = q.Encode()
	c, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return err
	}
	defer c.Close()
	if err = c.WriteJSON(map[string]string{"method": "subscribeNewToken"}); err != nil {
		return err
	}
	m.connected.Store(true)
	defer m.connected.Store(false)
	writerDone := make(chan struct{})
	defer close(writerDone)
	go func() {
		for {
			select {
			case <-writerDone:
				return
			case mint := <-m.subscriptions:
				if err := c.WriteJSON(map[string]any{"method": "subscribeTokenTrade", "keys": []string{mint}}); err != nil {
					m.log.Warn("token trade subscription failed; reconnecting", zap.String("mint", mint), zap.Error(err))
					m.bus.Publish("operational_error", map[string]any{
						"component": "token_monitor", "mint": mint, "message": "price subscription failed; reconnecting",
					})
					// Reconnect in both modes so tracked positions are subscribed again.
					_ = c.Close()
					return
				}
			}
		}
	}()
	m.tracked.Range(func(key, _ any) bool {
		select {
		case m.subscriptions <- key.(string):
		default:
		}
		return true
	})
	for {
		_, b, err := c.ReadMessage()
		if err != nil {
			return err
		}
		var d struct {
			Mint, Creator, User                    string
			CreatedTimestamp, Timestamp, BlockTime json.RawMessage
			MarketCapSol                           float64 `json:"marketCapSol"`
		}
		if err = json.Unmarshal(b, &d); err != nil || d.Mint == "" {
			m.log.Warn("invalid token event", zap.Error(err))
			continue
		}
		if _, tracked := m.tracked.Load(d.Mint); tracked && d.MarketCapSol > 0 {
			m.markTrade(ctx, d.Mint, d.MarketCapSol)
			continue
		}
		creator := d.Creator
		if creator == "" {
			creator = d.User
		}
		received := time.Now().UTC()
		created := timestampFromEvent(d.CreatedTimestamp, d.Timestamp, d.BlockTime)
		if created.IsZero() && m.simulation {
			created = received
		}
		e := Event{Mint: d.Mint, Creator: creator, Received: received, Created: created, MarketCapSOL: d.MarketCapSol, Raw: string(b)}
		if !m.rememberMint(e.Mint, received) {
			continue
		}
		m.bus.Publish("token_detected", map[string]any{"mint": e.Mint, "creator": e.Creator, "receivedAt": e.Received.UTC()})
		select {
		case m.q <- e:
		default:
			m.log.Warn("token queue full", zap.String("mint", e.Mint))
			m.bus.Publish("operational_error", map[string]string{"component": "token_monitor", "message": "token queue full"})
			m.forgetMint(e.Mint)
		}
	}
}
func (m *Monitor) consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-m.q:
			latency := time.Since(e.Received).Milliseconds()
			if err := m.db.RecordEvent(e.Mint, e.Creator, e.Raw, e.Received, latency); err != nil {
				m.log.Warn("record event", zap.Error(err))
				m.bus.Publish("operational_error", map[string]string{"component": "database", "message": "unable to record token"})
			}
			if e.MarketCapSOL <= 0 {
				reason := "missing creation market cap for take-profit baseline"
				_ = m.db.RecordAssessment(e.Mint, false, reason)
				m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
				continue
			}
			var ok bool
			var reason string
			if m.asyncRiskScreen && !m.simulation {
				ok, reason = m.strategy.EvaluateFast(e.Mint, e.Created)
			} else {
				ok, reason = m.strategy.Evaluate(ctx, e.Mint, e.Created)
			}
			m.log.Info("token evaluated", zap.String("mint", e.Mint), zap.Bool("eligible", ok), zap.String("reason", reason), zap.Int64("latency_ms", latency))
			if !ok || m.entry == nil {
				_ = m.db.RecordAssessment(e.Mint, false, reason)
				m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
				continue
			}
			id, entryErr := m.entry.BuyNewTokenUSD(ctx, e.Mint, m.tradeUSD, m.slippage, m.priorityFee, "pump", reason)
			if entryErr != nil {
				_ = m.db.RecordAssessment(e.Mint, false, entryErr.Error())
				m.bus.Publish("entry_rejected", map[string]any{"mint": e.Mint, "reason": entryErr.Error()})
				m.log.Info("approved token not entered", zap.String("mint", e.Mint), zap.Error(entryErr))
				continue
			}
			if m.asyncRiskScreen && !m.simulation {
				m.screenAfterEntry(ctx, e.Mint)
			}
			if assessmentErr := m.db.RecordAssessment(e.Mint, true, reason); assessmentErr != nil {
				m.log.Warn("record entered-token assessment", zap.Error(assessmentErr))
			}
			if m.simulation {
				if err := m.db.RecordPaperMark(e.Mint, e.MarketCapSOL); err != nil {
					m.log.Warn("record paper mark", zap.Error(err))
				}
			} else if err := m.db.RecordLiveMark(e.Mint, e.MarketCapSOL); err != nil {
				m.log.Warn("record live mark", zap.Error(err))
			}
			m.tracked.Store(e.Mint, struct{}{})
			select {
			case m.subscriptions <- e.Mint:
			default:
				m.log.Warn("token trade subscription queue full", zap.String("mint", e.Mint))
			}
			m.bus.Publish("token_approved", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
			m.bus.Publish("entry_submitted", map[string]any{"mint": e.Mint, "tradeId": id, "simulation": m.simulation})
		}
	}
}

func (m *Monitor) screenAfterEntry(parent context.Context, mint string) {
	go func() {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		reason, err := m.strategy.Screen(ctx, mint)
		m.log.Info("post-entry risk screen completed", zap.String("mint", mint), zap.String("reason", reason), zap.Error(err))
		if err == nil && strings.HasPrefix(reason, "RugCheck passed") {
			return
		}
		detail := reason
		if err != nil {
			detail = err.Error()
		}
		if detail == "" {
			detail = "risk screen did not return an explicit pass"
		}
		assessment := "post-entry risk screen: " + detail
		if recordErr := m.db.RecordAssessment(mint, false, assessment); recordErr != nil {
			m.log.Warn("record post-entry risk assessment", zap.String("mint", mint), zap.Error(recordErr))
		}
		payload, _ := json.Marshal(map[string]string{"mint": mint, "reason": detail})
		if logErr := m.db.LogOperational("warn", "post_entry_risk_screen", assessment, string(payload)); logErr != nil {
			m.log.Warn("record post-entry risk alert", zap.String("mint", mint), zap.Error(logErr))
		}
		m.log.Warn("post-entry risk flagged", zap.String("mint", mint), zap.String("reason", detail))
		m.bus.Publish("risk_alert", map[string]any{
			"mint": mint, "reason": "post-entry risk screen", "detail": detail,
			"positionRemainsOpen": true,
		})
	}()
}

func (m *Monitor) markTrade(ctx context.Context, mint string, marketCapSOL float64) {
	var err error
	if m.simulation {
		err = m.db.RecordPaperMark(mint, marketCapSOL)
	} else {
		err = m.db.RecordLiveMark(mint, marketCapSOL)
	}
	if err != nil {
		m.log.Warn("record take-profit mark", zap.Error(err))
		return
	}
	if m.simulation {
		err = m.db.UpdatePaperMark(mint, marketCapSOL)
	} else {
		err = m.db.UpdateLiveMark(mint, marketCapSOL)
	}
	if err != nil {
		m.log.Warn("update take-profit mark", zap.Error(err))
		return
	}
	var returnPct float64
	var known bool
	if m.simulation {
		returnPct, known, err = m.db.PaperReturnPct(mint)
	} else {
		returnPct, known, err = m.db.LiveReturnPct(mint)
	}
	if err != nil || !known {
		return
	}
	if !m.simulation {
		active, activeErr := m.db.ActiveForMint(mint)
		if activeErr != nil || active {
			return
		}
	}
	openedAt, ok, err := m.db.PositionOpenedAt(mint)
	if err != nil {
		return
	}
	if !ok {
		m.tracked.Delete(mint)
		return
	}
	reason := m.positionExitReason(mint, returnPct, openedAt, time.Now().UTC())
	var estimatedPnl, estimatedValue any
	if cost, costErr := m.db.PositionCost(mint); costErr == nil && cost > 0 {
		estimatedPnl = cost * returnPct / 100
		estimatedValue = cost + cost*returnPct/100
	}
	var entryMarketCap any
	if marketCapRatio := 1 + returnPct/100; marketCapRatio > 0 {
		entryMarketCap = marketCapSOL / marketCapRatio
	}
	m.bus.Publish("position_mark_updated", map[string]any{
		"mint": mint, "marketCapSol": marketCapSOL, "entryMarketCapSol": entryMarketCap,
		"currentMarketCapSol": marketCapSOL, "returnPct": returnPct,
		"estimatedPnlSol": estimatedPnl, "estimatedValueSol": estimatedValue,
		"markAvailable": true, "markUpdatedAt": time.Now().UTC(),
		"markSource": "pumpportal_market_cap", "tokenPriceAvailable": false,
		"pnlBasis": "market_cap_ratio_estimate", "takeProfitPct": m.takeProfit,
		"stopLossPct": m.stopLoss, "trailingStopPct": m.trailingStop, "simulation": m.simulation,
	})
	if reason == "" {
		return
	}
	m.submitExit(ctx, mint, marketCapSOL, returnPct, reason)
}

func (m *Monitor) positionExitReason(mint string, returnPct float64, openedAt, now time.Time) string {
	cost, err := m.db.PositionCost(mint)
	if err != nil || cost <= 0 {
		return ""
	}
	peakReturnPct, peakKnown, peakErr := m.db.PaperPeakReturnPct(mint)
	if !m.simulation {
		peakReturnPct, peakKnown, peakErr = m.db.LivePeakReturnPct(mint)
	}
	peakValue := cost * (1 + returnPct/100)
	if peakErr == nil && peakKnown && peakReturnPct > returnPct {
		peakValue = cost * (1 + peakReturnPct/100)
	}
	currentValue := cost * (1 + returnPct/100)
	return positions.ExitReason(
		positions.Position{Mint: mint, CostSOL: cost, PeakSOL: peakValue, OpenedAt: openedAt},
		currentValue,
		true,
		positions.Limits{TakeProfitPct: m.takeProfit, StopLossPct: m.stopLoss, TrailingStopPct: m.trailingStop, MaxHold: m.maxHold, Simulation: m.simulation},
		now,
	)
}

func (m *Monitor) submitExit(ctx context.Context, mint string, exitMarketCap, returnPct float64, reason string) {
	m.exitMu.Lock()
	defer m.exitMu.Unlock()

	_, open, err := m.db.PositionOpenedAt(mint)
	if err != nil {
		m.log.Warn("check position before exit", zap.String("mint", mint), zap.Error(err))
		return
	}
	if !open {
		m.tracked.Delete(mint)
		m.exitRetryAfter.Delete(mint)
		m.exitIntents.Delete(mint)
		return
	}
	if retryAt, ok := m.exitRetryAfter.Load(mint); ok && time.Now().Before(retryAt.(time.Time)) {
		return
	}
	active, activeErr := m.db.ActiveForMint(mint)
	if activeErr != nil {
		m.log.Warn("check pending exit", zap.String("mint", mint), zap.Error(activeErr))
		return
	}
	if active {
		return
	}
	if m.entry == nil {
		m.log.Warn("cannot close position: entry engine unavailable", zap.String("mint", mint))
		return
	}
	entryMarketCap, markedMarketCap, peakMarketCap, markKnown, markErr := m.db.MarkMarketCaps(mint, m.simulation)
	if markErr != nil {
		m.log.Warn("load exit audit market caps", zap.String("mint", mint), zap.Error(markErr))
	}
	if exitMarketCap <= 0 {
		exitMarketCap = markedMarketCap
	}
	writeExitAudit := func(eventType, tradeID string, exitErr error) {
		exitAudit := map[string]any{
			"mint": mint, "tradeId": tradeID, "reason": reason, "returnPct": returnPct,
			"entryMcap": nil, "peakMcap": nil, "exitMcap": nil,
			"simulation": m.simulation,
		}
		if markKnown {
			exitAudit["entryMcap"] = entryMarketCap
			exitAudit["peakMcap"] = peakMarketCap
		}
		if exitMarketCap > 0 {
			exitAudit["exitMcap"] = exitMarketCap
		}
		if exitErr != nil {
			exitAudit["error"] = exitErr.Error()
		}
		auditJSON, marshalErr := json.Marshal(exitAudit)
		if marshalErr != nil {
			m.log.Warn("encode position exit audit", zap.String("mint", mint), zap.Error(marshalErr))
			return
		}
		level := "info"
		message := reason
		if exitErr != nil {
			level = "warn"
			message = reason + ": " + exitErr.Error()
		}
		if logErr := m.db.LogOperational(level, eventType, message, string(auditJSON)); logErr != nil {
			m.log.Warn("write position exit audit", zap.String("mint", mint), zap.Error(logErr))
		}
	}
	m.exitRetryAfter.Store(mint, time.Now().Add(exitRetryDelay))
	m.exitIntents.Store(mint, reason)
	var id string
	if m.simulation {
		id, err = m.entry.ClosePaperPosition(ctx, mint, returnPct, reason+" reached in simulation")
	} else {
		id, err = m.entry.CloseLivePosition(ctx, mint, m.slippage, m.priorityFee, reason+" reached in live operation")
	}
	if err != nil {
		writeExitAudit("position_exit_failed", id, err)
		m.log.Warn("position exit", zap.String("mint", mint), zap.Error(err))
		m.bus.Publish("exit_retry_scheduled", map[string]any{
			"mint": mint, "reason": reason, "retryInSeconds": int(exitRetryDelay.Seconds()),
			"simulation": m.simulation,
		})
		return
	}
	writeExitAudit("position_exit_submitted", id, nil)
	if m.simulation {
		m.tracked.Delete(mint)
		m.exitRetryAfter.Delete(mint)
		m.exitIntents.Delete(mint)
	}
	m.bus.Publish("exit_triggered", map[string]any{"mint": mint, "tradeId": id, "reason": reason, "returnPct": returnPct, "simulation": m.simulation})
}

// timestampFromEvent accepts timestamp variants emitted by PumpPortal. Live
// entries reject missing timestamps; simulation may use receipt time because
// its feed is observation-only.
func timestampFromEvent(values ...json.RawMessage) time.Time {
	for _, raw := range values {
		if len(raw) == 0 || string(raw) == "null" {
			continue
		}
		var numeric float64
		if json.Unmarshal(raw, &numeric) == nil && numeric > 0 {
			if numeric > 1e12 {
				numeric /= 1000
			}
			return time.Unix(int64(numeric), 0).UTC()
		}
		var text string
		if json.Unmarshal(raw, &text) != nil {
			continue
		}
		if numeric, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err == nil && numeric > 0 {
			if numeric > 1e12 {
				numeric /= 1000
			}
			return time.Unix(int64(numeric), 0).UTC()
		}
		if parsed, err := time.Parse(time.RFC3339, text); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}
