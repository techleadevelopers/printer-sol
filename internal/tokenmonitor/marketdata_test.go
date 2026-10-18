package tokenmonitor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/dexscreener"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/strategy"
)

type fixedBondingCurveMarketCap float64

func (mcap fixedBondingCurveMarketCap) BondingCurveMarketCapSOL(context.Context, string) (float64, error) {
	return float64(mcap), nil
}

func newMarketDataTestMonitor(t *testing.T) (*Monitor, func()) {
	t.Helper()
	db, err := database.Open(filepath.Join(t.TempDir(), "market-data.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Migrate(); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	monitor := New("", "", 8, db, strategy.New(db, time.Minute), &recordingEntrier{},
		1, 0, 0, 50, 25, time.Minute, true, events.New(), zap.NewNop())
	return monitor, func() { _ = db.Close() }
}

func testCandidate(mint string) Event {
	now := time.Now().UTC()
	return Event{
		Mint: mint, Received: now, Created: now,
		Raw: `{"mint":"` + mint + `"}`,
	}
}

func TestCandidateGateRejectsTwoTradesWithoutPositiveMarketCap(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("zero-market-cap")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}

	monitor.recordCandidateTrade(event.Mint, "buy", 0)
	monitor.recordCandidateTrade(event.Mint, "sell", 0)
	monitor.finishCandidate(event.Mint, 0)
	if len(monitor.q) != 0 {
		t.Fatal("candidate without a positive WebSocket or RPC mark was queued for entry")
	}
}

func TestCandidateGateRequiresTwoWebSocketTrades(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("low-activity")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}

	monitor.recordCandidateTrade(event.Mint, "buy", 120)
	monitor.finishCandidate(event.Mint, 0)
	if len(monitor.q) != 0 {
		t.Fatal("candidate with only one WebSocket trade was queued for entry")
	}
}

func TestCandidateGateQueuesActiveTokenWithPositiveMarketCap(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("active-token")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}

	monitor.recordCandidateTrade(event.Mint, "buy", 110)
	monitor.recordCandidateTrade(event.Mint, "sell", 120)
	monitor.finishCandidate(event.Mint, 0)
	select {
	case queued := <-monitor.q:
		if queued.MarketCapSOL != 120 {
			t.Fatalf("entry baseline market cap = %f, want 120", queued.MarketCapSOL)
		}
	default:
		t.Fatal("active candidate was not queued after its delay gate")
	}
}

func TestCandidateGateUsesTwoPositivePriceWebSocketTradesWithoutRPC(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("websocket-fallback")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}
	monitor.recordCandidateTrade(event.Mint, "buy", 130)
	monitor.recordCandidateTrade(event.Mint, "sell", 140)

	monitor.finishCandidate(event.Mint, 0)
	select {
	case queued := <-monitor.q:
		if queued.MarketCapSOL != 140 {
			t.Fatalf("fallback market cap = %f, want latest positive trade mark 140", queued.MarketCapSOL)
		}
	default:
		t.Fatal("eligible WebSocket fallback was not queued")
	}
}

func TestCandidateGateUsesRPCAfterTwoWebSocketTradesWithoutMark(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("rpc-mark-fallback")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}
	monitor.recordCandidateTrade(event.Mint, "buy", 0)
	monitor.recordCandidateTrade(event.Mint, "sell", 0)
	monitor.bondingCurve = fixedBondingCurveMarketCap(150)

	monitor.checkCandidate(context.Background(), event.Mint)
	select {
	case queued := <-monitor.q:
		if queued.MarketCapSOL != 150 {
			t.Fatalf("RPC entry baseline market cap = %f, want 150", queued.MarketCapSOL)
		}
	default:
		t.Fatal("candidate with two WS trades and positive RPC mark was not queued")
	}
}

func TestCandidateGateRejectsWebSocketFallbackWithoutTwoTrades(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("idle-token")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}
	monitor.recordCandidateTrade(event.Mint, "buy", 140)

	monitor.finishCandidate(event.Mint, 0)
	if len(monitor.q) != 0 {
		t.Fatal("candidate with only one WebSocket trade was queued for entry")
	}
}

func TestRPCPriceSourceAlertLogsAfterThreeConsecutiveFailures(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	alertEvents, unsubscribe := monitor.bus.Subscribe()
	defer unsubscribe()

	for i := 0; i < 3; i++ {
		monitor.recordPriceSourceResult(errors.New("RPC unavailable"))
	}
	if monitor.priceSourceFailures != 3 || !monitor.priceSourceAlerted {
		t.Fatalf("alert state after three failures = (%d, %v)", monitor.priceSourceFailures, monitor.priceSourceAlerted)
	}
	select {
	case alert := <-alertEvents:
		if alert.Type != "risk.alert" {
			t.Fatalf("dashboard event type = %q, want risk.alert", alert.Type)
		}
	default:
		t.Fatal("third consecutive failure did not publish the dashboard risk alert")
	}
	var alerts int
	if err := monitor.db.QueryRow(`SELECT COUNT(*) FROM operational_logs WHERE event_type='risk_alert'`).Scan(&alerts); err != nil {
		t.Fatal(err)
	}
	if alerts != 1 {
		t.Fatalf("persisted risk alerts = %d, want exactly 1", alerts)
	}

	monitor.recordPriceSourceResult(errors.New("RPC unavailable"))
	monitor.recordPriceSourceResult(nil)
	if monitor.priceSourceFailures != 0 || monitor.priceSourceAlerted {
		t.Fatalf("recovery did not reset alert state: (%d, %v)", monitor.priceSourceFailures, monitor.priceSourceAlerted)
	}
}
