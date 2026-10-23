package tokenmonitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/positions"
	"solana-sniper-backend/internal/strategy"
)

type Event struct {
	Mint         string `json:"mint"`
	Creator      string `json:"creator"`
	User         string `json:"user"`
	Received     time.Time
	Created      time.Time
	MarketCapSOL float64
	Raw          string
	queuedAt     time.Time
}

type Entrier interface {
	BuyNewTokenUSD(context.Context, string, float64, float64, float64, string, string) (string, error)
	ClosePaperPosition(context.Context, string, float64, string) (string, error)
	CloseLivePosition(context.Context, string, float64, float64, string) (string, error)
}
type unknownTradeReconciler interface {
	ReconcileUnknown(context.Context) error
}

type BondingCurveMarketCapSource interface {
	BondingCurveMarketCapSOL(context.Context, string) (float64, error)
	BondingCurveEntryMetrics(context.Context, string) (float64, float64, error)
}

type Settings struct {
	EntryDelay            time.Duration
	MinVolumeBeforeBuySOL float64
	MinMarketCapSOL       float64
	MaxMarketCapSOL       float64
	MaxTokenAge           time.Duration
	PricePollInterval     time.Duration
	SnipeConcurrency      int
	SnipeTimeout          time.Duration
	DedupeTTL             time.Duration
	BondingCurve          BondingCurveMarketCapSource
}

type candidate struct {
	event      Event
	detectedAt time.Time
	checking   bool
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
	maxTokenAge                     time.Duration
	simulation                      bool
	subscriptions                   chan string
	tracked                         sync.Map
	recentlyProcessed               sync.Map
	recentlyProcessedMu             sync.Mutex
	connected                       atomic.Bool
	operationMu                     sync.RWMutex
	resetting                       atomic.Bool
	resetGeneration                 atomic.Uint64
	connectionMu                    sync.Mutex
	connection                      *websocket.Conn
	missingMarks                    sync.Map
	exitMu                          sync.Mutex
	exitRetryAfter                  sync.Map
	exitIntents                     sync.Map
	exitRetryExhausted              sync.Map
	candidateMu                     sync.Mutex
	candidates                      map[string]*candidate
	snipeMu                         sync.Mutex
	snipePending                    map[string]time.Time
	validationSlots                 chan struct{}
	entryDelay                      time.Duration
	minVolumeBeforeBuySOL           float64
	minMarketCapSOL                 float64
	maxMarketCapSOL                 float64
	pricePollInterval               time.Duration
	snipeConcurrency                int
	snipeTimeout                    time.Duration
	dedupeTTL                       time.Duration
	bondingCurve                    BondingCurveMarketCapSource
	priceSourceStatusMu             sync.Mutex
	priceSourceFailures             int
	priceSourceAlerted              bool
}

const exitRetryDelay = 5 * time.Second
const maxAutomaticSimulationExitAttempts = 3
const maxAutomaticLiveExitAttempts = 3
const staleSimulationSellRecoveryAge = time.Minute
const staleLiveSellRecoveryAge = time.Minute
const stuckExitQuarantineAge = 2 * time.Minute

func (m *Monitor) runOperation(fn func()) bool {
	return m.runOperationAt(m.resetGeneration.Load(), fn)
}

func (m *Monitor) runOperationAt(generation uint64, fn func()) bool {
	if m.resetting.Load() {
		return false
	}
	m.operationMu.RLock()
	defer m.operationMu.RUnlock()
	if m.resetting.Load() || m.resetGeneration.Load() != generation {
		return false
	}
	fn()
	return true
}

func (m *Monitor) WithReset(reset func() error) error {
	if !m.resetting.CompareAndSwap(false, true) {
		return errors.New("monitor reset already in progress")
	}
	m.resetGeneration.Add(1)
	m.operationMu.Lock()
	defer func() {
		m.resetting.Store(false)
		m.operationMu.Unlock()
	}()

	m.closeConnection()
	if reset != nil {
		if err := reset(); err != nil {
			return err
		}
	}
	m.ResetInMemoryState()
	return nil
}

func (m *Monitor) closeConnection() {
	m.connectionMu.Lock()
	connection := m.connection
	m.connectionMu.Unlock()
	m.connected.Store(false)
	if connection != nil {
		_ = connection.Close()
	}
}

func (m *Monitor) ResetInMemoryState() {
	m.tracked.Range(func(key, _ any) bool {
		m.tracked.Delete(key)
		return true
	})
	m.recentlyProcessedMu.Lock()
	m.recentlyProcessed.Range(func(key, _ any) bool {
		m.recentlyProcessed.Delete(key)
		return true
	})
	m.recentlyProcessedMu.Unlock()
	m.exitRetryAfter.Range(func(key, _ any) bool {
		m.exitRetryAfter.Delete(key)
		return true
	})
	m.exitIntents.Range(func(key, _ any) bool {
		m.exitIntents.Delete(key)
		return true
	})
	m.exitRetryExhausted.Range(func(key, _ any) bool {
		m.exitRetryExhausted.Delete(key)
		return true
	})
	m.missingMarks.Range(func(key, _ any) bool {
		m.missingMarks.Delete(key)
		return true
	})
	m.candidateMu.Lock()
	m.candidates = make(map[string]*candidate)
	m.candidateMu.Unlock()
	m.snipeMu.Lock()
	m.snipePending = make(map[string]time.Time)
	m.snipeMu.Unlock()
	for {
		select {
		case <-m.q:
		default:
			goto subscriptions
		}
	}
subscriptions:
	for {
		select {
		case <-m.subscriptions:
		default:
			return
		}
	}
}

func New(ws, key string, size int, db *database.DB, s *strategy.Strategy, entry Entrier, tradeUSD, slippage, priorityFee, takeProfit, stopLoss float64, maxHold time.Duration, simulation bool, b *events.Bus, l *zap.Logger, options ...Settings) *Monitor {
	if maxHold <= 0 {
		maxHold = 5 * time.Minute
	}
	settings := Settings{
		EntryDelay:            5 * time.Second,
		MinVolumeBeforeBuySOL: .02,
		MinMarketCapSOL:       20,
		MaxMarketCapSOL:       0,
		MaxTokenAge:           0,
		PricePollInterval:     20 * time.Second,
		SnipeConcurrency:      4,
		SnipeTimeout:          15 * time.Second,
		DedupeTTL:             60 * time.Second,
	}
	if len(options) > 0 {
		option := options[0]
		if option.EntryDelay > 0 {
			settings.EntryDelay = option.EntryDelay
		}
		if option.MinVolumeBeforeBuySOL > 0 {
			settings.MinVolumeBeforeBuySOL = option.MinVolumeBeforeBuySOL
		}
		if option.MinMarketCapSOL > 0 {
			settings.MinMarketCapSOL = option.MinMarketCapSOL
		}
		if option.MaxMarketCapSOL > 0 {
			settings.MaxMarketCapSOL = option.MaxMarketCapSOL
		}
		if option.MaxTokenAge > 0 {
			settings.MaxTokenAge = option.MaxTokenAge
		}
		if option.PricePollInterval > 0 {
			settings.PricePollInterval = option.PricePollInterval
		}
		if option.SnipeConcurrency > 0 {
			settings.SnipeConcurrency = option.SnipeConcurrency
		}
		if option.SnipeTimeout > 0 {
			settings.SnipeTimeout = option.SnipeTimeout
		}
		if option.DedupeTTL > 0 {
			settings.DedupeTTL = option.DedupeTTL
		}
		settings.BondingCurve = option.BondingCurve
	}
	if settings.SnipeConcurrency > 10 {
		settings.SnipeConcurrency = 10
	}
	return &Monitor{
		ws: ws, key: key, q: make(chan Event, size), db: db, strategy: s, entry: entry,
		tradeUSD: tradeUSD, slippage: slippage, priorityFee: priorityFee,
		takeProfit: takeProfit, stopLoss: stopLoss, trailingStop: 15,
		maxHold: maxHold, maxTokenAge: settings.MaxTokenAge,
		simulation: simulation, subscriptions: make(chan string, size),
		candidates:      make(map[string]*candidate),
		snipePending:    make(map[string]time.Time),
		validationSlots: make(chan struct{}, settings.SnipeConcurrency),
		entryDelay:      settings.EntryDelay,
		minVolumeBeforeBuySOL: settings.MinVolumeBeforeBuySOL,
		minMarketCapSOL:       settings.MinMarketCapSOL,
		maxMarketCapSOL:       settings.MaxMarketCapSOL,
		pricePollInterval:     settings.PricePollInterval,
		snipeConcurrency:      settings.SnipeConcurrency,
		snipeTimeout:          settings.SnipeTimeout, dedupeTTL: settings.DedupeTTL,
		bondingCurve: settings.BondingCurve,
		bus:          b, log: l,
	}
}
func (m *Monitor) SetTrailingStopPct(pct float64) {
	if pct > 0 {
		m.trailingStop = pct
	}
}
func (m *Monitor) MaxHoldSeconds() int {
	if m == nil {
		return 0
	}
	return int(m.maxHold / time.Second)
}
func (m *Monitor) Run(ctx context.Context) {
	m.restoreOpenPositions()
	go m.consume(ctx)
	go m.runMaxHoldChecker(ctx)
	go m.runSeenJanitor(ctx)
	go m.runCandidateChecker(ctx)
	go m.runPricePoller(ctx)
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
			m.pruneSnipeDedupe(now)
		}
	}
}

func (m *Monitor) pruneSeen(now time.Time) {
	ttl := m.dedupeTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	cutoff := now.Add(-ttl)
	m.recentlyProcessed.Range(func(mint, value any) bool {
		seenAt, ok := value.(time.Time)
		if !ok || seenAt.Before(cutoff) {
			m.recentlyProcessed.Delete(mint)
		}
		return true
	})
}

func (m *Monitor) rememberMint(mint string, now time.Time) bool {
	ttl := m.dedupeTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	m.recentlyProcessedMu.Lock()
	defer m.recentlyProcessedMu.Unlock()
	if value, exists := m.recentlyProcessed.Load(mint); exists {
		if seenAt, ok := value.(time.Time); ok && now.Sub(seenAt) < ttl {
			return false
		}
	}
	m.recentlyProcessed.Store(mint, now)
	return true
}

func (m *Monitor) forgetMint(mint string) {
	m.recentlyProcessedMu.Lock()
	m.recentlyProcessed.Delete(mint)
	m.recentlyProcessedMu.Unlock()
}

func (m *Monitor) snipeEventExpired(event Event, now time.Time) bool {
	if event.queuedAt.IsZero() {
		return false
	}
	ttl := m.dedupeTTL
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	return !now.Before(event.queuedAt.Add(ttl))
}

func (m *Monitor) runMaxHoldChecker(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	reconcileTicker := time.NewTicker(1 * time.Second)
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
	m.runOperation(func() {
		m.reconcileUnknownTradesLocked(ctx)
	})
}

func (m *Monitor) reconcileUnknownTradesLocked(ctx context.Context) {
	reconciler, ok := m.entry.(unknownTradeReconciler)
	if !ok {
		return
	}
	if err := reconciler.ReconcileUnknown(ctx); err != nil {
		m.log.Warn("reconcile unknown live trades", zap.Error(err))
	}
}

func (m *Monitor) expireOpenPositions(ctx context.Context) {
	m.runOperation(func() {
		m.expireOpenPositionsLocked(ctx)
	})
}

func (m *Monitor) expireOpenPositionsLocked(ctx context.Context) {
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
				m.exitRetryExhausted.Delete(mint)
			}
		}
		return true
	})
	m.exitRetryExhausted.Range(func(key, _ any) bool {
		mint, ok := key.(string)
		if ok {
			if _, stillOpen := openMints[mint]; !stillOpen {
				m.exitRetryExhausted.Delete(mint)
			}
		}
		return true
	})
	for _, mint := range mints {
		if ctx.Err() != nil {
			return
		}
		if m.simulation {
			reconciled, reconcileErr := m.db.ReconcileStaleSimulationSells(mint, now.Add(-staleSimulationSellRecoveryAge))
			if reconcileErr != nil {
				m.log.Warn("reconcile stale simulated sell",
					zap.String("mint", mint),
					zap.Error(reconcileErr))
				continue
			}
			if reconciled > 0 {
				m.log.Warn("recovered stale simulated sell state",
					zap.String("mint", mint),
					zap.Int("attempts", reconciled))
			}

			stuck, stuckErr := m.db.StuckExitMint(mint, now.Add(-stuckExitQuarantineAge))
			if stuckErr != nil {
				m.log.Warn("check stuck exit mint", zap.String("mint", mint), zap.Error(stuckErr))
				continue
			}
			if stuck {
				cleaned, quarantineErr := m.db.QuarantineStuckSimulationMint(mint, now.Add(-stuckExitQuarantineAge))
				if quarantineErr != nil {
					m.log.Warn("quarantine stuck simulated mint",
						zap.String("mint", mint),
						zap.Error(quarantineErr))
					continue
				}
				if cleaned > 0 {
					m.log.Error("quarantined stuck simulated mint",
						zap.String("mint", mint),
						zap.Int("cleaned_sells", cleaned))
					payload, _ := json.Marshal(map[string]any{
						"mint": mint, "simulation": true, "cleanedSells": cleaned,
					})
					if logErr := m.db.LogOperational("error", "stuck_mint_quarantined",
						"stuck simulated exit quarantined; position closed and slot released",
						string(payload)); logErr != nil {
						m.log.Warn("record quarantine audit", zap.String("mint", mint), zap.Error(logErr))
					}
					m.bus.Publish("risk_alert", map[string]any{
						"mint": mint, "simulation": true,
						"reason":          "stuck simulated exit quarantined; position closed and slot released",
						"cleanedSells":    cleaned,
						"positionRemainsOpen": false,
					})
					m.tracked.Delete(mint)
					m.exitRetryAfter.Delete(mint)
					m.exitIntents.Delete(mint)
					m.exitRetryExhausted.Delete(mint)
				}
				continue
			}
		} else {
			reconciled, reconcileErr := m.db.ReconcileStaleLiveSells(mint, now.Add(-staleLiveSellRecoveryAge))
			if reconcileErr != nil {
				m.log.Warn("reconcile stale live sell",
					zap.String("mint", mint),
					zap.Error(reconcileErr))
				continue
			}
			if reconciled > 0 {
				m.log.Warn("recovered stale live sell state",
					zap.String("mint", mint),
					zap.Int("attempts", reconciled))
			}

			stuck, stuckErr := m.db.StuckExitMint(mint, now.Add(-stuckExitQuarantineAge))
			if stuckErr != nil {
				m.log.Warn("check stuck live exit mint", zap.String("mint", mint), zap.Error(stuckErr))
				continue
			}
			if stuck {
				cleaned, quarantineErr := m.db.QuarantineStuckLiveMint(mint, now.Add(-stuckExitQuarantineAge))
				if quarantineErr != nil {
					m.log.Warn("quarantine stuck live mint",
						zap.String("mint", mint),
						zap.Error(quarantineErr))
					continue
				}
				if cleaned > 0 {
					m.log.Error("quarantined stuck live mint",
						zap.String("mint", mint),
						zap.Int("cleaned_sells", cleaned))
					payload, _ := json.Marshal(map[string]any{
						"mint": mint, "simulation": false, "cleanedSells": cleaned,
					})
					if logErr := m.db.LogOperational("error", "stuck_mint_quarantined",
						"stuck live exit quarantined; position closed and slot released",
						string(payload)); logErr != nil {
						m.log.Warn("record quarantine audit", zap.String("mint", mint), zap.Error(logErr))
					}
					m.bus.Publish("risk_alert", map[string]any{
						"mint": mint, "simulation": false,
						"reason":          "stuck live exit quarantined; position closed and slot released",
						"cleanedSells":    cleaned,
						"positionRemainsOpen": false,
					})
					m.tracked.Delete(mint)
					m.exitRetryAfter.Delete(mint)
					m.exitIntents.Delete(mint)
					m.exitRetryExhausted.Delete(mint)
				}
				continue
			}
		}
		openedAt, open, err := m.db.PositionOpenedAt(mint)
		if err != nil {
			m.log.Warn("read position age", zap.String("mint", mint), zap.Error(err))
			continue
		}
		if !open {
			continue
		}

		var returnPct float64
		var exitMarketCap float64
		var known bool
		if m.simulation {
			returnPct, known, err = m.db.PaperReturnPct(mint)
		} else {
			returnPct, known, err = m.db.LiveReturnPct(mint)
		}
		_, exitMarketCap, _, markKnown, markErr := m.db.MarkMarketCaps(mint, m.simulation)
		if err != nil || markErr != nil || !known || !markKnown || exitMarketCap <= 0 {
			if err != nil {
				m.alertMissingMark(mint, err)
			} else {
				m.alertMissingMark(mint, markErr)
			}
			continue
		}
		m.missingMarks.Delete(mint)
		failedExitAttempts, _, failedExitErr := m.db.FailedSellRetryState(mint)
		if failedExitErr != nil {
			m.log.Warn("check previous exit failures", zap.String("mint", mint), zap.Error(failedExitErr))
			continue
		}
		reason := ""
		exitIntent, hasExitIntent := m.exitIntents.Load(mint)
		switch {
		case hasExitIntent:
			reason = exitIntent.(string)
		case failedExitAttempts > 0:
			reason = "reconcile pending exit"
		case known:
			reason = m.positionExitReason(mint, returnPct, openedAt, now)
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
		_, currentMarketCap, _, known, markErr := m.db.MarkMarketCaps(mint, m.simulation)
		if markErr != nil || !known || currentMarketCap <= 0 {
			m.alertMissingMark(mint, markErr)
		}
	}
	if len(mints) > 0 {
		m.log.Info("restored open positions", zap.Int("count", len(mints)), zap.Bool("simulation", m.simulation))
	}
}

func (m *Monitor) alertMissingMark(mint string, markErr error) {
	if _, alreadyReported := m.missingMarks.LoadOrStore(mint, struct{}{}); alreadyReported {
		return
	}
	reason := "open position has no usable market-cap mark; position remains open"
	payload, _ := json.Marshal(map[string]any{
		"mint": mint, "simulation": m.simulation, "reason": reason,
	})
	fields := []zap.Field{zap.String("mint", mint), zap.Bool("simulation", m.simulation)}
	if markErr != nil {
		fields = append(fields, zap.Error(markErr))
	}
	m.log.Warn("open position has no valid mark; keeping it open", fields...)
	m.bus.Publish("risk.alert", map[string]any{
		"mint": mint, "reason": reason, "positionRemainsOpen": true,
		"simulation": m.simulation,
	})
	if logErr := m.db.LogOperational("warn", "orphan_position_mark_missing", reason, string(payload)); logErr != nil {
		m.log.Warn("record missing-mark risk alert", zap.String("mint", mint), zap.Error(logErr))
	}
}
func (m *Monitor) runConnections(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		if err := m.connect(ctx); err != nil {
			m.log.Warn("token websocket disconnected; reconnect scheduled",
				zap.Error(err), zap.Duration("retry_in", backoff))
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
	m.connectionMu.Lock()
	m.connection = c
	m.connectionMu.Unlock()
	defer func() {
		m.connectionMu.Lock()
		if m.connection == c {
			m.connection = nil
		}
		m.connectionMu.Unlock()
		m.connected.Store(false)
		_ = c.Close()
	}()
	if err = c.WriteJSON(map[string]string{"method": "subscribeNewToken"}); err != nil {
		return err
	}
	m.connected.Store(true)
	m.log.Info("PumpPortal websocket connected; subscribed to new-token feed",
		zap.String("endpoint", u.Host))
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
					_ = c.Close()
					return
				}
				m.log.Info("PumpPortal subscribeTokenTrade request sent", zap.String("mint", mint))
			}
		}
	}()
	m.tracked.Range(func(key, _ any) bool {
		m.runOperation(func() {
			select {
			case m.subscriptions <- key.(string):
			default:
				m.tracked.Delete(key)
				m.log.Warn("open-position subscription queue full during reconnect", zap.String("mint", key.(string)))
			}
		})
		return true
	})
	for {
		_, b, err := c.ReadMessage()
		if err != nil {
			return err
		}
		m.logWebSocketRaw(b)
		var d struct {
			Mint             string          `json:"mint"`
			Creator          string          `json:"creator"`
			User             string          `json:"user"`
			TraderPublicKey  string          `json:"traderPublicKey"`
			TxType           string          `json:"txType"`
			CreatedTimestamp json.RawMessage `json:"createdTimestamp"`
			Timestamp        json.RawMessage `json:"timestamp"`
			BlockTime        json.RawMessage `json:"blockTime"`
			MarketCapSol     float64         `json:"marketCapSol"`
		}
		if err = json.Unmarshal(b, &d); err != nil {
			m.log.Warn("invalid PumpPortal websocket JSON",
				zap.Error(err),
				zap.String("payload", m.websocketPayloadPreview(b)))
			continue
		}
		if d.Mint == "" {
			var response struct {
				Error   json.RawMessage `json:"error"`
				Message json.RawMessage `json:"message"`
			}
			_ = json.Unmarshal(b, &response)
			switch {
			case len(response.Error) > 0 && string(response.Error) != "null" && string(response.Error) != `""`:
				detail := m.websocketPayloadPreview(b)
				m.log.Warn("PumpPortal websocket returned an error without a mint", zap.String("payload", detail))
				m.bus.Publish("operational_error", map[string]any{
					"component": "token_monitor",
					"message":   "PumpPortal websocket returned an error; no price update was processed",
					"detail":    detail,
				})
			case len(response.Message) > 0 && string(response.Message) != "null":
				m.log.Info("PumpPortal websocket control response", zap.String("payload", m.websocketPayloadPreview(b)))
			default:
				m.log.Debug("ignored PumpPortal websocket frame without a mint",
					zap.String("payload", m.websocketPayloadPreview(b)))
			}
			continue
		}
		if m.resetting.Load() {
			continue
		}
		_, tracked := m.tracked.Load(d.Mint)
		switch strings.ToLower(strings.TrimSpace(d.TxType)) {
		case "buy", "sell", "trade":
			m.log.Info("PumpPortal websocket trade event received",
				zap.String("mint", d.Mint),
				zap.String("tx_type", d.TxType),
				zap.Float64("mcap", d.MarketCapSol),
				zap.Bool("tracked", tracked))
		default:
			m.log.Debug("PumpPortal websocket market-cap event",
				zap.String("mint", d.Mint),
				zap.String("tx_type", d.TxType),
				zap.Float64("market_cap_sol", d.MarketCapSol),
				zap.Bool("tracked", tracked))
		}
		if tracked {
			if d.MarketCapSol > 0 {
				m.markTrade(ctx, d.Mint, d.MarketCapSol)
			} else {
				m.log.Warn("tracked token trade skipped without positive market cap",
					zap.String("mint", d.Mint),
					zap.Float64("mcap_sol", d.MarketCapSol),
					zap.Bool("simulation", m.simulation))
			}
			continue
		}
		creator := d.Creator
		if creator == "" {
			creator = d.TraderPublicKey
		}
		if creator == "" {
			creator = d.User
		}
		received := time.Now().UTC()
		created := timestampFromEvent(d.CreatedTimestamp, d.Timestamp, d.BlockTime)
		if created.IsZero() {
			created = received
		}

		if m.maxMarketCapSOL > 0 && d.MarketCapSol > (m.maxMarketCapSOL*1.15) {
			m.log.Info("token rejected instantly by websocket max market cap tolerance",
				zap.String("mint", d.Mint),
				zap.Float64("ws_mcap_sol", d.MarketCapSol),
				zap.Float64("max_allowed", m.maxMarketCapSOL*1.15))
			continue
		}

		e := Event{Mint: d.Mint, Creator: creator, Received: received, Created: created, MarketCapSOL: d.MarketCapSol, Raw: string(b)}
		m.runOperation(func() {
			if !m.rememberMint(e.Mint, received) {
				return
			}
			m.bus.Publish("token_detected", map[string]any{"mint": e.Mint, "creator": e.Creator, "receivedAt": e.Received.UTC()})
			if !m.addCandidate(e) {
				m.rejectCandidate(e, "candidate queue full")
				return
			}
		})
	}
}
func (m *Monitor) consume(ctx context.Context) {
	concurrency := m.snipeConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer workers.Done()
			m.consumeSnipeQueue(ctx)
		}()
	}
	<-ctx.Done()
	workers.Wait()
}

func (m *Monitor) consumeSnipeQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-m.q:
			if ctx.Err() != nil {
				return
			}
			select {
			case m.validationSlots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			func() {
				defer func() { <-m.validationSlots }()
				if m.snipeEventExpired(e, time.Now().UTC()) {
					m.rejectCandidate(e, "snipe queue item expired before processing")
					return
				}
				timeout := m.snipeTimeout
				if timeout <= 0 {
					timeout = 15 * time.Second
				}
				jobCtx, cancel := context.WithTimeout(ctx, timeout)
				m.runOperation(func() {
					if jobCtx.Err() == nil {
						m.consumeEvent(jobCtx, e)
					}
				})
				cancel()
			}()
		}
	}
}

func (m *Monitor) websocketPayloadPreview(raw []byte) string {
	preview := strings.TrimSpace(string(raw))
	if m.key != "" {
		preview = strings.ReplaceAll(preview, m.key, "[redacted]")
	}
	if len(preview) > 512 {
		preview = preview[:512] + "..."
	}
	return preview
}

func (m *Monitor) logWebSocketRaw(raw []byte) {
	var control struct {
		Type    string `json:"type"`
		Method  string `json:"method"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &control) == nil {
		for _, value := range []string{control.Type, control.Method, control.Message} {
			if strings.Contains(strings.ToLower(value), "subscrib") {
				return
			}
		}
	}
	payload := string(raw)
	if m.key != "" {
		payload = strings.ReplaceAll(payload, m.key, "[redacted]")
	}
	m.log.Debug("[WS-RAW] " + payload)
}

func (m *Monitor) consumeEvent(ctx context.Context, e Event) {
	latency := time.Since(e.Received).Milliseconds()
	if err := m.db.RecordEvent(e.Mint, e.Creator, e.Raw, e.Received, latency); err != nil {
		m.log.Warn("record event", zap.Error(err))
		m.bus.Publish("operational_error", map[string]string{"component": "database", "message": "unable to record token"})
	}
	if reason := m.creatorBlacklistReason(e.Creator); reason != "" {
		m.rejectCandidate(e, reason)
		return
	}
	if e.MarketCapSOL <= 0 {
		reason := "missing creation market cap for take-profit baseline"
		_ = m.db.RecordAssessment(e.Mint, false, reason)
		m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
		return
	}

	if m.maxTokenAge > 0 && !e.Created.IsZero() {
		age := time.Since(e.Created)
		maxToleranceAge := time.Duration(float64(m.maxTokenAge) * 1.15)
		if age > maxToleranceAge {
			reason := fmt.Sprintf("token age %s exceeds max tolerance %s",
				age.Truncate(time.Second), maxToleranceAge)
			_ = m.db.RecordAssessment(e.Mint, false, reason)
			m.bus.Publish("token_rejected", map[string]any{
				"mint": e.Mint, "reason": reason, "latencyMs": latency,
			})
			m.log.Info("token rejected by age gate",
				zap.String("mint", e.Mint),
				zap.Duration("age", age),
				zap.Duration("max_age", maxToleranceAge))
			return
		}
	}

	ok, reason := m.strategy.EvaluateFast(e.Mint, e.Created)
	m.log.Info("token evaluated", zap.String("mint", e.Mint), zap.Bool("eligible", ok), zap.String("reason", reason), zap.Int64("latency_ms", latency))
	if !ok || m.entry == nil {
		_ = m.db.RecordAssessment(e.Mint, false, reason)
		m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
		return
	}

	if m.maxMarketCapSOL > 0 && e.MarketCapSOL > (m.maxMarketCapSOL*1.15) {
		reason := "market cap exceeds max tolerance configured"
		_ = m.db.RecordAssessment(e.Mint, false, reason)
		m.bus.Publish("token_rejected", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
		m.log.Info("approved token not entered", zap.String("mint", e.Mint), zap.String("reason", reason))
		return
	}

	id, entryErr := m.entry.BuyNewTokenUSD(ctx, e.Mint, m.tradeUSD, m.slippage, m.priorityFee, "pump", reason)
	if entryErr != nil {
		_ = m.db.RecordAssessment(e.Mint, false, entryErr.Error())
		m.bus.Publish("entry_rejected", map[string]any{"mint": e.Mint, "reason": entryErr.Error()})
		m.log.Info("approved token not entered", zap.String("mint", e.Mint), zap.Error(entryErr))
		return
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
		m.tracked.Delete(e.Mint)
		m.log.Warn("token trade subscription queue full", zap.String("mint", e.Mint))
	}
	m.bus.Publish("token_approved", map[string]any{"mint": e.Mint, "reason": reason, "latencyMs": latency})
	m.bus.Publish("entry_submitted", map[string]any{"mint": e.Mint, "tradeId": id, "simulation": m.simulation})
	if m.strategy.HasScreen() {
		m.screenAfterEntry(context.Background(), e.Mint)
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
	m.markTradeWithSource(ctx, mint, marketCapSOL, "pumpportal_websocket_market_cap")
}

func (m *Monitor) markTradeWithSource(ctx context.Context, mint string, marketCapSOL float64, source string) {
	m.markTradeWithSourceAt(m.resetGeneration.Load(), ctx, mint, marketCapSOL, source)
}

func (m *Monitor) markTradeWithSourceAt(generation uint64, ctx context.Context, mint string, marketCapSOL float64, source string) {
	m.runOperationAt(generation, func() {
		m.markTradeLocked(ctx, mint, marketCapSOL, source)
	})
}

func (m *Monitor) markTradeLocked(ctx context.Context, mint string, marketCapSOL float64, source string) {
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
	if err != nil {
		m.log.Warn("read return after mark update",
			zap.String("mint", mint),
			zap.Float64("mcap_sol", marketCapSOL),
			zap.Bool("simulation", m.simulation),
			zap.Error(err))
		return
	}
	if !known {
		m.log.Warn("mark row missing after update",
			zap.String("mint", mint),
			zap.Float64("mcap_sol", marketCapSOL),
			zap.Bool("simulation", m.simulation))
		return
	}

	// CORREÇÃO: limita o returnPct a 1000% para evitar P&L absurdo
	if returnPct > 1000 {
		returnPct = 1000
	}

	m.log.Info("mark update",
		zap.String("mint", mint),
		zap.Float64("mcap_sol", marketCapSOL),
		zap.Float64("return_pct", returnPct),
		zap.Bool("simulation", m.simulation))
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
		"markSource": source, "tokenPriceAvailable": false,
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
		positions.Limits{TakeProfitPct: m.takeProfit, StopLossPct: m.stopLoss, TrailingStopPct: m.trailingStop, MaxHold: m.maxHold},
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
		m.exitRetryExhausted.Delete(mint)
		return
	}
	if _, exhausted := m.exitRetryExhausted.Load(mint); exhausted {
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
	lastSellState, _, stateErr := m.db.LastSellState(mint)
	if stateErr != nil {
		m.log.Warn("read last sell state", zap.String("mint", mint), zap.Error(stateErr))
		return
	}
	if lastSellState == "failed" || lastSellState == "unknown" {
		m.blockAutomaticExitRetries(mint, reason, 0)
		return
	}
	failedAttempts, lastFailure, retryErr := m.db.FailedSellRetryState(mint)
	if retryErr != nil {
		m.log.Warn("read persisted exit retry state", zap.String("mint", mint), zap.Error(retryErr))
		return
	}
	maxAttempts := maxAutomaticSimulationExitAttempts
	if !m.simulation {
		maxAttempts = maxAutomaticLiveExitAttempts
	}
	if failedAttempts >= maxAttempts {
		m.blockAutomaticExitRetries(mint, reason, failedAttempts)
		return
	}
	if failedAttempts > 0 {
		retryAt := lastFailure.Add(simulationExitRetryDelay(failedAttempts))
		if memoryRetryAt, ok := m.exitRetryAfter.Load(mint); ok && memoryRetryAt.(time.Time).After(retryAt) {
			retryAt = memoryRetryAt.(time.Time)
		}
		if time.Now().Before(retryAt) {
			m.exitRetryAfter.Store(mint, retryAt)
			return
		}
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
		active, activeErr := m.db.ActiveForMint(mint)
		if activeErr != nil {
			m.log.Warn("check exit after failure", zap.String("mint", mint), zap.Error(activeErr))
		} else if active {
			m.bus.Publish("risk_alert", map[string]any{
				"mint": mint, "tradeId": id, "simulation": m.simulation,
				"reason": "exit is unresolved and remains active; automatic retry is blocked",
			})
		} else if id == "" {
			m.blockAutomaticExitRetries(mint, reason, 0)
		} else {
			failedAttempts, lastFailure, retryErr := m.db.FailedSellRetryState(mint)
			if retryErr != nil {
				m.log.Warn("read exit retry state after failure", zap.String("mint", mint), zap.Error(retryErr))
				m.blockAutomaticExitRetries(mint, reason, failedAttempts)
			} else if failedAttempts == 0 {
				m.blockAutomaticExitRetries(mint, reason, failedAttempts)
			} else if failedAttempts >= maxAttempts {
				m.blockAutomaticExitRetries(mint, reason, failedAttempts)
			} else {
				retryAt := lastFailure.Add(simulationExitRetryDelay(failedAttempts))
				m.exitRetryAfter.Store(mint, retryAt)
				retryIn := time.Until(retryAt)
				if retryIn < 0 {
					retryIn = 0
				}
				m.bus.Publish("exit_retry_scheduled", map[string]any{
					"mint": mint, "tradeId": id, "reason": reason,
					"retryInSeconds": int(retryIn.Seconds()), "simulation": m.simulation,
					"attempt": failedAttempts + 1, "maxAttempts": maxAttempts,
				})
			}
		}
		return
	}
	writeExitAudit("position_exit_submitted", id, nil)
	m.tracked.Delete(mint)
	m.exitRetryAfter.Delete(mint)
	m.exitIntents.Delete(mint)
	m.exitRetryExhausted.Delete(mint)
	m.bus.Publish("exit_triggered", map[string]any{"mint": mint, "tradeId": id, "reason": reason, "returnPct": returnPct, "simulation": m.simulation})
}

func simulationExitRetryDelay(failedAttempts int) time.Duration {
	delay := exitRetryDelay
	for i := 1; i < failedAttempts && delay < 30*time.Second; i++ {
		delay *= 2
	}
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func (m *Monitor) blockAutomaticExitRetries(mint, reason string, failedAttempts int) {
	if _, alreadyReported := m.exitRetryExhausted.LoadOrStore(mint, struct{}{}); alreadyReported {
		return
	}
	maxAttempts := maxAutomaticSimulationExitAttempts
	if !m.simulation {
		maxAttempts = maxAutomaticLiveExitAttempts
	}
	m.log.Error("automatic exit retries stopped; manual reconciliation is required",
		zap.String("mint", mint),
		zap.String("reason", reason),
		zap.Bool("simulation", m.simulation),
		zap.Int("failed_attempts", failedAttempts),
		zap.Int("max_attempts", maxAttempts))
	m.bus.Publish("risk_alert", map[string]any{
		"mint": mint, "simulation": m.simulation,
		"reason": "automatic exit retries stopped; inspect the failed sell and database error before retrying",
		"failedAttempts": failedAttempts, "maxAttempts": maxAttempts,
	})
}

func (m *Monitor) blockAutomaticSimulationExitRetries(mint, reason string, failedAttempts int) {
	m.blockAutomaticExitRetries(mint, reason, failedAttempts)
}

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