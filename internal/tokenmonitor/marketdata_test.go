package tokenmonitor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/strategy"
)

type fixedBondingCurveMarketCap struct {
	marketCapSOL float64
	reservesSOL  float64
	err          error
}

func (source fixedBondingCurveMarketCap) BondingCurveMarketCapSOL(context.Context, string) (float64, error) {
	return source.marketCapSOL, source.err
}

func (source fixedBondingCurveMarketCap) BondingCurveEntryMetrics(context.Context, string) (float64, float64, error) {
	return source.marketCapSOL, source.reservesSOL, source.err
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
		1, 0, 0, 50, 25, time.Minute, true, events.New(), zap.NewNop(),
		Settings{
			MinVolumeBeforeBuySOL: .02,
			MinMarketCapSOL:       20,
			BondingCurve:          fixedBondingCurveMarketCap{marketCapSOL: 30, reservesSOL: .05},
		})
	return monitor, func() { _ = db.Close() }
}

func testCandidate(mint string) Event {
	now := time.Now().UTC()
	return Event{
Mint: mint, Creator: "test-creator", Received: now, Created: now,
		Raw: `{"mint":"` + mint + `"}`,
	}
}

func TestCandidateGateUsesRPCWithoutWebSocketTrades(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	event := testCandidate("rpc-only-entry")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}
	monitor.checkCandidate(context.Background(), event.Mint)
	select {
	case queued := <-monitor.q:
		if queued.MarketCapSOL != 30 {
			t.Fatalf("entry baseline market cap = %f, want RPC value 30", queued.MarketCapSOL)
		}
	default:
		t.Fatal("candidate with RPC-confirmed reserve and market cap was not queued")
	}
}

func TestCandidateGateRejectsBelowRPCReserveOrMarketCapFloor(t *testing.T) {
	for _, test := range []struct {
		name        string
		marketCap   float64
		reservesSOL float64
	}{
		{name: "reserve proxy", marketCap: 30, reservesSOL: .01},
		{name: "market cap", marketCap: 19.99, reservesSOL: .05},
	} {
		t.Run(test.name, func(t *testing.T) {
			monitor, closeDB := newMarketDataTestMonitor(t)
			defer closeDB()
			monitor.bondingCurve = fixedBondingCurveMarketCap{
				marketCapSOL: test.marketCap,
				reservesSOL:  test.reservesSOL,
			}
			event := testCandidate(test.name)
			if !monitor.addCandidate(event) {
				t.Fatal("candidate was not queued")
			}
			monitor.checkCandidate(context.Background(), event.Mint)
			if len(monitor.q) != 0 {
				t.Fatal("candidate below an RPC threshold was queued")
			}
		})
	}
}

func TestCandidateGateRejectsRPCFailure(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	monitor.bondingCurve = fixedBondingCurveMarketCap{err: errors.New("RPC unavailable")}
	event := testCandidate("rpc-failure")
	if !monitor.addCandidate(event) {
		t.Fatal("candidate was not queued")
	}
	monitor.checkCandidate(context.Background(), event.Mint)
	if len(monitor.q) != 0 {
		t.Fatal("candidate without RPC confirmation was queued")
	}
}

func TestSnipeQueueDeduplicationExpiresAfterConfiguredTTL(t *testing.T) {
	monitor, closeDB := newMarketDataTestMonitor(t)
	defer closeDB()
	monitor.dedupeTTL = 20 * time.Millisecond
	event := testCandidate("deduplicated-snipe")

	if queued, reason := monitor.enqueueSnipe(event); !queued {
		t.Fatalf("first enqueue rejected: %s", reason)
	}
	if queued, reason := monitor.enqueueSnipe(event); queued || reason != "duplicate snipe suppressed within dedupe TTL" {
		t.Fatalf("duplicate enqueue = (%v, %q); want a TTL duplicate rejection", queued, reason)
	}
	<-monitor.q

	monitor.pruneSnipeDedupe(time.Now().UTC().Add(21 * time.Millisecond))
	if queued, reason := monitor.enqueueSnipe(event); !queued {
		t.Fatalf("enqueue after dedupe TTL rejected: %s", reason)
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
