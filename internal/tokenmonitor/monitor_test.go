package tokenmonitor

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/execution"
	"solana-sniper-backend/internal/strategy"
)

type recordingEntrier struct {
	liveExitCalls    int
	liveExitFailures int
	buyCalls         int
}

type concurrentEntrier struct {
	active  atomic.Int32
	maximum atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (e *concurrentEntrier) BuyNewTokenUSD(ctx context.Context, _ string, _, _, _ float64, _, _ string) (string, error) {
	active := e.active.Add(1)
	for {
		previous := e.maximum.Load()
		if active <= previous || e.maximum.CompareAndSwap(previous, active) {
			break
		}
	}
	e.started <- struct{}{}
	defer e.active.Add(-1)
	select {
	case <-e.release:
		return "trade", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (e *concurrentEntrier) ClosePaperPosition(context.Context, string, float64, string) (string, error) {
	return "", nil
}
func (e *concurrentEntrier) CloseLivePosition(context.Context, string, float64, float64, string) (string, error) {
	return "", nil
}

func TestSeenMintIsDeduplicatedOnlyWithinRetentionWindow(t *testing.T) {
	monitor := &Monitor{dedupeTTL: time.Minute}
	first := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	if !monitor.rememberMint("mint", first) {
		t.Fatal("first observation was treated as a duplicate")
	}
	if monitor.rememberMint("mint", first.Add(59*time.Second)) {
		t.Fatal("duplicate inside retention window was accepted")
	}
	if !monitor.rememberMint("mint", first.Add(time.Minute)) {
		t.Fatal("mint was not eligible for memory cleanup after retention")
	}
}

func (e *recordingEntrier) BuyNewTokenUSD(context.Context, string, float64, float64, float64, string, string) (string, error) {
	e.buyCalls++
	return "", nil
}
func (e *recordingEntrier) ClosePaperPosition(context.Context, string, float64, string) (string, error) {
	return "", nil
}
func (e *recordingEntrier) CloseLivePosition(context.Context, string, float64, float64, string) (string, error) {
	e.liveExitCalls++
	if e.liveExitCalls <= e.liveExitFailures {
		return "", errors.New("simulated live sell failure")
	}
	return "live-sell", nil
}

func TestConsumeRejectsCreationEventWithoutTakeProfitBaseline(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "missing-entry-mark.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	bus := events.New()
	received, unsubscribe := bus.Subscribe()
	defer unsubscribe()
	entry := &recordingEntrier{}
	monitor := New("", "", 1, db, strategy.New(db, 5*time.Minute), entry, 1, 0, 0, 50, 30, 0, false, bus, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitor.consume(ctx)
		close(done)
	}()
	monitor.q <- Event{
		Mint: "missing-market-cap", Received: time.Now().UTC(),
		Created: time.Now().UTC(), Raw: `{"mint":"missing-market-cap"}`,
	}

	select {
	case event := <-received:
		if event.Type != "token_rejected" {
			t.Fatalf("unexpected event %q; want token_rejected", event.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("creation event without market cap was not rejected")
	}
	cancel()
	<-done
	if entry.buyCalls != 0 {
		t.Fatalf("entry attempted without a take-profit baseline: %d buys", entry.buyCalls)
	}
}

func TestConsumeRejectsCreatorBlockedByClosedStopLoss(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "blocked-creator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	const creator = "creator-with-stop-loss"
	if err = db.RecordEvent("stopped-mint", creator, "{}", time.Now().UTC(), 1); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("stopped-mint", 0.1); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(database.Trade{
		ID: "stop-loss-sell", Mint: "stopped-mint", Side: "sell", Quantity: "100",
		Requested: 0.075, State: "prepared", ExitReason: "stop loss",
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.FinalizeSimulationSell("stop-loss-sell", "stopped-mint", 0.075); err != nil {
		t.Fatal(err)
	}

	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, strategy.New(db, time.Minute), entry,
		1, 0, 0, 50, 25, time.Minute, true, events.New(), zap.NewNop())
	now := time.Now().UTC()
	monitor.consumeEvent(context.Background(), Event{
		Mint: "new-mint-from-blocked-creator", Creator: creator, Received: now,
		Created: now, MarketCapSOL: 100, Raw: `{"mint":"new-mint-from-blocked-creator"}`,
	})
	if entry.buyCalls != 0 {
		t.Fatalf("blacklisted creator submitted %d entries", entry.buyCalls)
	}
	var reason string
	if err = db.QueryRow(`SELECT reason FROM risk_assessments WHERE mint=? ORDER BY id DESC LIMIT 1`,
		"new-mint-from-blocked-creator").Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reason, "creator in blacklist") {
		t.Fatalf("blacklist rejection reason = %q", reason)
	}
}

func TestConsumeSubmitsEntryBeforeRunningRiskScreen(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "async-entry-screen.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}

	screenStarted := make(chan struct{}, 1)
	releaseScreen := make(chan struct{})
	screenDone := make(chan struct{})
	strat := strategy.New(db, 5*time.Minute)
	strat.SetScreen(func(context.Context, string) (string, error) {
		screenStarted <- struct{}{}
		<-releaseScreen
		close(screenDone)
		return "RugCheck passed: test", nil
	})
	entry := &recordingEntrier{}
	monitor := New("", "", 1, db, strat, entry, 1, 0, 0, 50, 25, 0, true, events.New(), zap.NewNop())
	event := Event{
		Mint: "async-screen-mint", Received: time.Now().UTC(), Created: time.Now().UTC(),
		MarketCapSOL: 100, Raw: `{"mint":"async-screen-mint","marketCapSol":100}`,
	}

	consumeDone := make(chan struct{})
	go func() {
		monitor.consumeEvent(context.Background(), event)
		close(consumeDone)
	}()
	select {
	case <-consumeDone:
	case <-time.After(time.Second):
		t.Fatal("entry processing waited for the risk screen")
	}
	if entry.buyCalls != 1 {
		t.Fatalf("entry was not submitted before screening: %d buys", entry.buyCalls)
	}
	select {
	case <-screenStarted:
	case <-time.After(time.Second):
		t.Fatal("post-entry risk screen did not start")
	}
	close(releaseScreen)
	select {
	case <-screenDone:
	case <-time.After(time.Second):
		t.Fatal("post-entry risk screen did not finish")
	}
}

func TestSnipeQueueProcessesNoMoreThanConfiguredConcurrency(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "snipe-concurrency.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	entry := &concurrentEntrier{started: make(chan struct{}, 4), release: make(chan struct{}, 4)}
	monitor := New("", "", 8, db, strategy.New(db, time.Minute), entry,
		1, 0, 0, 50, 25, time.Minute, true, events.New(), zap.NewNop(),
		Settings{SnipeConcurrency: 2, SnipeTimeout: 2 * time.Second, DedupeTTL: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitor.consume(ctx)
		close(done)
	}()
	for _, mint := range []string{"parallel-mint-one", "parallel-mint-two"} {
		queued, reason := monitor.enqueueSnipe(Event{
			Mint: mint, Received: time.Now().UTC(), Created: time.Now().UTC(),
			MarketCapSOL: 100, Raw: `{"mint":"` + mint + `"}`,
		})
		if !queued {
			cancel()
			t.Fatalf("enqueue %s: %s", mint, reason)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entry.started:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("both configured snipe workers did not start")
		}
	}
	if maximum := entry.maximum.Load(); maximum != 2 {
		cancel()
		t.Fatalf("maximum concurrent entries = %d; want 2", maximum)
	}
	entry.release <- struct{}{}
	entry.release <- struct{}{}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("snipe workers did not stop after cancellation")
	}
}

func TestSnipeQueueExpiresStaleEventsBeforeEntry(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "snipe-ttl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	bus := events.New()
	eventStream, unsubscribe := bus.Subscribe()
	defer unsubscribe()
	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, strategy.New(db, time.Minute), entry,
		1, 0, 0, 50, 25, time.Minute, true, bus, zap.NewNop(),
		Settings{SnipeConcurrency: 1, SnipeTimeout: time.Second, DedupeTTL: 40 * time.Millisecond})
	queued, reason := monitor.enqueueSnipe(Event{
		Mint: "expired-snipe", Received: time.Now().UTC(), Created: time.Now().UTC(),
		MarketCapSOL: 100, Raw: `{"mint":"expired-snipe"}`,
	})
	if !queued {
		t.Fatalf("enqueue stale-event test: %s", reason)
	}
	time.Sleep(60 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		monitor.consume(ctx)
		close(done)
	}()
	select {
	case event := <-eventStream:
		if event.Type != "token_rejected" {
			t.Fatalf("stale queue event type = %q, want token_rejected", event.Type)
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("expired queue item was not rejected")
	}
	cancel()
	<-done
	if entry.buyCalls != 0 {
		t.Fatalf("expired queue item attempted %d entries", entry.buyCalls)
	}
}

func TestSimulationMarksTriggerVirtualTakeProfitAndStopLoss(t *testing.T) {
	tests := []struct {
		name          string
		takeProfitPct float64
		stopLossPct   float64
		currentCap    float64
		wantPnL       float64
	}{
		{name: "take profit", takeProfitPct: 70, stopLossPct: 30, currentCap: 170, wantPnL: 0.035},
		{name: "stop loss", takeProfitPct: 70, stopLossPct: 30, currentCap: 70, wantPnL: -0.015},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := database.Open(filepath.Join(t.TempDir(), "paper-exit.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err = db.Migrate(); err != nil {
				t.Fatal(err)
			}
			if err = db.OpenSimulationPosition("paper-mint", 0.05); err != nil {
				t.Fatal(err)
			}
			if err = db.RecordPaperMark("paper-mint", 100); err != nil {
				t.Fatal(err)
			}

			bus := events.New()
			engine := execution.New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, bus, execution.Limits{})
			monitor := New("", "", 8, db, nil, engine, 0, 0, 0, test.takeProfitPct, test.stopLossPct, 0, true, bus, zap.NewNop())
			monitor.markTrade(context.Background(), "paper-mint", test.currentCap)

			open, err := db.OpenPositionCount()
			if err != nil {
				t.Fatal(err)
			}
			if open != 0 {
				t.Fatalf("virtual %s left %d positions open", test.name, open)
			}
			var side, state string
			var pnl float64
			if err = db.QueryRow(`SELECT side,state,realized_pnl_sol FROM trades WHERE side='sell'`).Scan(&side, &state, &pnl); err != nil {
				t.Fatalf("virtual %s did not record a sell: %v", test.name, err)
			}
			if side != "sell" || state != "simulated" {
				t.Fatalf("unexpected virtual exit: side=%q state=%q", side, state)
			}
			if diff := pnl - test.wantPnL; diff < -1e-9 || diff > 1e-9 {
				t.Fatalf("virtual %s P&L = %.9f SOL, want %.9f SOL", test.name, pnl, test.wantPnL)
			}
			available, err := db.SimulationAvailableBalanceSOL(1)
			if err != nil {
				t.Fatal(err)
			}
			if diff := available - (1 + test.wantPnL); diff < -1e-9 || diff > 1e-9 {
				t.Fatalf("bankroll after virtual %s = %.9f SOL, want %.9f SOL", test.name, available, 1+test.wantPnL)
			}
		})
	}
}

func TestLiveMonitorRestoresOpenPositionsAndWaitsForConfirmedExit(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "live-monitor.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("live-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordLiveMark("live-mint", 100); err != nil {
		t.Fatal(err)
	}

	bus := events.New()
	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, nil, entry, 0, 0, 0, 50, 30, time.Minute, false, bus, zap.NewNop())
	monitor.restoreOpenPositions()
	if _, tracked := monitor.tracked.Load("live-mint"); !tracked {
		t.Fatal("live open position was not restored for price monitoring")
	}

	monitor.markTrade(context.Background(), "live-mint", 149)
	if entry.liveExitCalls != 0 {
		t.Fatalf("take-profit fired below configured 50%% threshold: %d exits", entry.liveExitCalls)
	}
	monitor.markTrade(context.Background(), "live-mint", 150)
	if entry.liveExitCalls != 1 {
		t.Fatalf("take-profit should submit one live exit, got %d", entry.liveExitCalls)
	}
	if _, tracked := monitor.tracked.Load("live-mint"); !tracked {
		t.Fatal("live position was untracked before its sell was confirmed")
	}

	if err = db.InsertTrade(database.Trade{ID: "live-sell", Mint: "live-mint", Side: "sell", Quantity: "100", Requested: 100, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	monitor.markTrade(context.Background(), "live-mint", 175)
	if entry.liveExitCalls != 1 {
		t.Fatalf("pending sell caused a duplicate submission: %d exits", entry.liveExitCalls)
	}

	if err = db.FinalizeLiveSell("live-sell", "live-mint", "confirmed", "signature", 0.075, 0.001); err != nil {
		t.Fatal(err)
	}
	monitor.markTrade(context.Background(), "live-mint", 175)
	if _, tracked := monitor.tracked.Load("live-mint"); tracked {
		t.Fatal("closed live position remained subscribed after the next price update")
	}
}

func TestLivePendingBuyKeepsMarkTrackingUntilConfirmation(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "live-pending-buy-mark.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(database.Trade{ID: "pending-buy", Mint: "pending-mint", Side: "buy", Quantity: "0.05", Requested: 0.05, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordLiveMark("pending-mint", 100); err != nil {
		t.Fatal(err)
	}

	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, nil, entry, 0, 0, 0, 50, 30, 5*time.Minute, false, events.New(), zap.NewNop())
	monitor.tracked.Store("pending-mint", struct{}{})
	monitor.markTrade(context.Background(), "pending-mint", 150)
	if _, tracked := monitor.tracked.Load("pending-mint"); !tracked {
		t.Fatal("pending live buy lost its price subscription before confirmation")
	}
	if entry.liveExitCalls != 0 {
		t.Fatalf("pending buy triggered a sell before confirmation: %d exits", entry.liveExitCalls)
	}

	if err = db.FinalizeLiveBuy("pending-buy", "pending-mint", "confirmed", "buy-signature", 0.05, 0); err != nil {
		t.Fatal(err)
	}
	monitor.markTrade(context.Background(), "pending-mint", 150)
	if entry.liveExitCalls != 1 {
		t.Fatalf("confirmed position did not exit at its configured 50%% target: %d exits", entry.liveExitCalls)
	}
}

func TestLiveMaxHoldDoesNotForceExitWithoutPriceMark(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "live-max-hold-no-feed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("stale-live-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-11*time.Minute), "stale-live-mint"); err != nil {
		t.Fatal(err)
	}

	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, nil, entry, 0, 0, 0, 50, 30, 10*time.Minute, false, events.New(), zap.NewNop())
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 0 {
		t.Fatalf("live position without a price mark should remain open, got %d sells", entry.liveExitCalls)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-21*time.Minute), "stale-live-mint"); err != nil {
		t.Fatal(err)
	}
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 0 {
		t.Fatalf("unknown price after twice max-hold must not submit a sell, got %d", entry.liveExitCalls)
	}

	if err = db.InsertTrade(database.Trade{ID: "pending-max-hold-sell", Mint: "stale-live-mint", Side: "sell", Quantity: "100", Requested: 100, State: "submitted"}); err != nil {
		t.Fatal(err)
	}
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 1 {
		t.Fatalf("pending timed exit caused a duplicate sell: %d", entry.liveExitCalls)
	}
}

func TestLiveTakeProfitRetriesAfterSellFailureWithoutNewPriceEvent(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "live-take-profit-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("retry-live-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordLiveMark("retry-live-mint", 100); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateLiveMark("retry-live-mint", 150); err != nil {
		t.Fatal(err)
	}

	entry := &recordingEntrier{liveExitFailures: 1}
	monitor := New("", "", 8, db, nil, entry, 0, 0, 0, 50, 30, 5*time.Minute, false, events.New(), zap.NewNop())
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 1 {
		t.Fatalf("take-profit should attempt the first live exit, got %d", entry.liveExitCalls)
	}

	if err = db.UpdateLiveMark("retry-live-mint", 120); err != nil {
		t.Fatal(err)
	}
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 1 {
		t.Fatal("retry backoff did not suppress an immediate repeated request")
	}
	monitor.exitRetryAfter.Store("retry-live-mint", time.Now().Add(-time.Second))
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 2 {
		t.Fatalf("take-profit was not retried from the persisted mark: %d exits", entry.liveExitCalls)
	}
}

func TestLiveFailedSellRetriesEvenAfterPriceFallsBelowTakeProfit(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "live-failed-sell-retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenPosition("failed-sell-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordLiveMark("failed-sell-mint", 100); err != nil {
		t.Fatal(err)
	}
	if err = db.UpdateLiveMark("failed-sell-mint", 120); err != nil {
		t.Fatal(err)
	}
	if err = db.InsertTrade(database.Trade{ID: "confirmed-failed-sell", Mint: "failed-sell-mint", Side: "sell", Quantity: "100", Requested: 100, State: "prepared"}); err != nil {
		t.Fatal(err)
	}
	if err = db.MarkTradeFailed("confirmed-failed-sell"); err != nil {
		t.Fatal(err)
	}

	entry := &recordingEntrier{}
	monitor := New("", "", 8, db, nil, entry, 0, 0, 0, 50, 30, 5*time.Minute, false, events.New(), zap.NewNop())
	monitor.expireOpenPositions(context.Background())
	if entry.liveExitCalls != 1 {
		t.Fatalf("persisted failed sell should be retried below take-profit: %d exits", entry.liveExitCalls)
	}
}

func TestSimulationMaxHoldReleasesFlatPosition(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "paper-max-hold.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("paper-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if err = db.RecordPaperMark("paper-mint", 100); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-time.Hour), "paper-mint"); err != nil {
		t.Fatal(err)
	}

	bus := events.New()
	engine := execution.New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, bus, execution.Limits{})
	monitor := New("", "", 8, db, nil, engine, 0, 0, 0, 70, 30, time.Minute, true, bus, zap.NewNop())
	monitor.markTrade(context.Background(), "paper-mint", 100)

	open, err := db.OpenPositionCount()
	if err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatal("simulation max-hold exit left the position open")
	}
}

func TestSimulationMaxHoldDoesNotForceExitWithoutPriceUpdates(t *testing.T) {
	db, err := database.Open(filepath.Join(t.TempDir(), "paper-max-hold-no-feed.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err = db.OpenSimulationPosition("stale-paper-mint", 0.05); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-6*time.Minute), "stale-paper-mint"); err != nil {
		t.Fatal(err)
	}

	bus := events.New()
	engine := execution.New(db, nil, nil, nil, nil, zap.NewNop(), true, 1, nil, bus, execution.Limits{})
	monitor := New("", "", 8, db, nil, engine, 0, 0, 0, 50, 30, 5*time.Minute, true, bus, zap.NewNop())
	monitor.expireOpenPositions(context.Background())

	open, err := db.OpenPositionCount()
	if err != nil {
		t.Fatal(err)
	}
	if open != 1 {
		t.Fatalf("simulation position without a price mark should remain open: %d", open)
	}
	if _, err = db.Exec(`UPDATE positions SET opened_at=? WHERE mint=?`, time.Now().UTC().Add(-11*time.Minute), "stale-paper-mint"); err != nil {
		t.Fatal(err)
	}
	monitor.expireOpenPositions(context.Background())
	open, err = db.OpenPositionCount()
	if err != nil {
		t.Fatal(err)
	}
	if open != 1 {
		t.Fatalf("unknown price after twice max-hold must not force-close simulation position: %d", open)
	}
}
