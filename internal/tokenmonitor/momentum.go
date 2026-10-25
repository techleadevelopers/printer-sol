package tokenmonitor

import (
	"sort"
	"sync"
	"time"
)

// MomentumWindow define uma janela móvel de avaliação.
type MomentumWindow struct {
	Duration time.Duration
}

// MomentumConfig define os limites do filtro de momentum.
type MomentumConfig struct {
	Enabled             bool
	ShortWindow         time.Duration
	MediumWindow        time.Duration
	LongWindow          time.Duration
	MinVolumeSOL        float64
	MinBuyRatio         float64
	MinUniqueBuyers     int
	MinAcceleration     float64
	MaxTradeAge         time.Duration
}

func DefaultMomentumConfig() MomentumConfig {
	return MomentumConfig{
		Enabled:         false,
		ShortWindow:     10 * time.Second,
		MediumWindow:    30 * time.Second,
		LongWindow:      60 * time.Second,
		MinVolumeSOL:    0,
		MinBuyRatio:     0,
		MinUniqueBuyers: 0,
		MinAcceleration: 0,
		MaxTradeAge:     2 * time.Minute,
	}
}

// TradeEvent é o subconjunto do payload de subscribeTokenTrade que o filtro
// de momentum precisa.
type TradeEvent struct {
	Mint      string
	TxType    string
	SOLAmount float64
	Trader    string
	At        time.Time
}

// MomentumSnapshot é o resultado da avaliação de um mint.
type MomentumSnapshot struct {
	VolumeShort      float64
	VolumeMedium     float64
	VolumeLong       float64
	BuyVolume        float64
	SellVolume       float64
	BuyRatio         float64
	UniqueBuyers     int
	Acceleration     float64
	LastTradeAt      time.Time
	TradeCount       int
	Sufficient       bool
	RejectReason     string
}

// MomentumTracker mantém janelas móveis por mint.
type MomentumTracker struct {
	mu     sync.Mutex
	config MomentumConfig
	trades map[string][]TradeEvent
}

func NewMomentumTracker(config MomentumConfig) *MomentumTracker {
	if config.ShortWindow <= 0 {
		config.ShortWindow = 10 * time.Second
	}
	if config.MediumWindow <= 0 {
		config.MediumWindow = 30 * time.Second
	}
	if config.LongWindow <= 0 {
		config.LongWindow = 60 * time.Second
	}
	if config.MaxTradeAge <= 0 {
		config.MaxTradeAge = 2 * time.Minute
	}
	return &MomentumTracker{config: config, trades: make(map[string][]TradeEvent)}
}

// Record adiciona um trade à janela do mint e descarta os antigos.
func (t *MomentumTracker) Record(event TradeEvent) {
	if !t.config.Enabled {
		return
	}
	if event.Mint == "" || event.At.IsZero() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := event.At.Add(-t.config.MaxTradeAge)
	existing := t.trades[event.Mint]
	kept := existing[:0]
	for _, e := range existing {
		if e.At.After(cutoff) {
			kept = append(kept, e)
		}
	}
	kept = append(kept, event)
	t.trades[event.Mint] = kept
}

// Evaluate calcula o snapshot de momentum para o mint no instante now.
func (t *MomentumTracker) Evaluate(mint string, now time.Time) MomentumSnapshot {
	if !t.config.Enabled {
		return MomentumSnapshot{Sufficient: true}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	trades := t.trades[mint]
	if len(trades) == 0 {
		return MomentumSnapshot{
			Sufficient:   false,
			RejectReason: "no recent trades for token",
		}
	}

	shortCutoff := now.Add(-t.config.ShortWindow)
	mediumCutoff := now.Add(-t.config.MediumWindow)
	longCutoff := now.Add(-t.config.LongWindow)

	var short, medium, long, buyVol, sellVol float64
	buyers := make(map[string]struct{})
	var lastTrade time.Time
	count := 0
	for _, trade := range trades {
		if trade.At.After(now) {
			continue
		}
		if trade.At.After(shortCutoff) {
			short += trade.SOLAmount
		}
		if trade.At.After(mediumCutoff) {
			medium += trade.SOLAmount
		}
		if trade.At.After(longCutoff) {
			long += trade.SOLAmount
			count++
		}
		switch trade.TxType {
		case "buy":
			buyVol += trade.SOLAmount
			if trade.Trader != "" {
				buyers[trade.Trader] = struct{}{}
			}
		case "sell":
			sellVol += trade.SOLAmount
		}
		if trade.At.After(lastTrade) {
			lastTrade = trade.At
		}
	}

	snapshot := MomentumSnapshot{
		VolumeShort:  short,
		VolumeMedium: medium,
		VolumeLong:   long,
		BuyVolume:    buyVol,
		SellVolume:   sellVol,
		UniqueBuyers: len(buyers),
		LastTradeAt:  lastTrade,
		TradeCount:   count,
	}
	total := buyVol + sellVol
	if total > 0 {
		snapshot.BuyRatio = buyVol / total
	}
	if medium > 0 {
		snapshot.Acceleration = short / (medium / (t.config.MediumWindow.Seconds() / t.config.ShortWindow.Seconds()))
	}

	switch {
	case long < t.config.MinVolumeSOL:
		snapshot.RejectReason = "recent SOL volume below minimum"
	case snapshot.BuyRatio < t.config.MinBuyRatio:
		snapshot.RejectReason = "buy ratio below minimum"
	case snapshot.UniqueBuyers < t.config.MinUniqueBuyers:
		snapshot.RejectReason = "unique buyer count below minimum"
	case snapshot.Acceleration < t.config.MinAcceleration:
		snapshot.RejectReason = "volume acceleration below minimum"
	default:
		snapshot.Sufficient = true
	}
	return snapshot
}

// Forget remove o estado de um mint.
func (t *MomentumTracker) Forget(mint string) {
	t.mu.Lock()
	delete(t.trades, mint)
	t.mu.Unlock()
}

// Reset limpa todo o estado.
func (t *MomentumTracker) Reset() {
	t.mu.Lock()
	t.trades = make(map[string][]TradeEvent)
	t.mu.Unlock()
}

// SnapshotMints devolve os mints com trades registados (para debug).
func (t *MomentumTracker) SnapshotMints() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	mints := make([]string, 0, len(t.trades))
	for mint := range t.trades {
		mints = append(mints, mint)
	}
	sort.Strings(mints)
	return mints
}