package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"solana-sniper-backend/internal/api"
	"solana-sniper-backend/internal/bot"
	"solana-sniper-backend/internal/config"
	"solana-sniper-backend/internal/database"
	"solana-sniper-backend/internal/events"
	"solana-sniper-backend/internal/execution"
	"solana-sniper-backend/internal/logging"
	"solana-sniper-backend/internal/pricefeed"
	"solana-sniper-backend/internal/pumpportal"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/screener"
	"solana-sniper-backend/internal/strategy"
	"solana-sniper-backend/internal/tokenmonitor"
	"solana-sniper-backend/internal/wallet"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		// Invalid deployment variables must not produce a Go stack trace or
		// trigger a noisy crash loop with no actionable configuration message.
		_, _ = os.Stderr.WriteString("configuration error: " + err.Error() + "\n")
		os.Exit(1)
	}
	log, closeLog := logging.New()
	defer closeLog()
	var paperDB *database.DB
	if !cfg.Simulation && cfg.LiveUnlockRequiresPaper {
		paperDB, err = database.Open(cfg.SimulationDatabaseURL)
		if err != nil {
			log.Fatal("paper live-unlock database", logging.Err(err))
		}
		defer paperDB.Close()
		if migrateErr := paperDB.Migrate(); migrateErr != nil {
			log.Fatal("paper live-unlock migrations", logging.Err(migrateErr))
		}
		_, unlockErr := paperDB.RequireLiveUnlock()
		if unlockErr != nil {
			log.Fatal("live trading blocked by paper evidence", logging.Err(unlockErr))
		}
		log.Info("paper evidence accepted for live mode")
	}
	databaseTarget := cfg.DatabaseURL
	if databaseTarget == "" {
		databaseTarget = cfg.DatabasePath
	}
	db, err := database.Open(databaseTarget)
	if err != nil {
		log.Fatal("database", logging.Err(err))
	}
	defer db.Close()
	if err := db.Migrate(); err != nil {
		log.Fatal("migration", logging.Err(err))
	}
	simulationBalanceSOL := cfg.SimulationBalanceSOL
	if cfg.Simulation {
		run, runErr := db.EnsureSimulationRun(cfg.SimulationBalanceSOL)
		if runErr != nil {
			log.Fatal("simulation session", logging.Err(runErr))
		}
		simulationBalanceSOL = run.BalanceStartSOL
	}
	w, err := wallet.Load(cfg.PrivateKey, cfg.Simulation, cfg.FeeReserveSOL)
	if err != nil {
		log.Fatal("wallet configuration", logging.Err(err))
	}
	if !cfg.Simulation && w.Address() != cfg.WalletPublicAddress {
		log.Fatal("wallet configuration", logging.Err(errors.New("WALLET_PUBLIC_ADDRESS does not match BOT_PRIVATE_KEY")))
	}
	bus := events.New()
	rpcs := rpcmanager.New(cfg.RPCEndpoints, log, db, bus)
	portal := pumpportal.New(cfg.PumpPortalURL)
	controller := bot.New(bus)
	// The worker is autonomous. Console credentials do not control its startup.
	controller.Start()
	limits := execution.Limits{MaxTradeSOL: cfg.MaxTradeSOL, MaxExposureSOL: cfg.MaxExposureSOL, MaxDailyLossSOL: cfg.MaxDailyLossSOL, MaxSlippagePct: cfg.MaxSlippagePct, MaxPriorityFeeSOL: cfg.MaxPriorityFeeSOL, FeeReserveSOL: cfg.FeeReserveSOL, SimulationBaseFeeSOL: cfg.SimulationBaseFeeSOL, MaxOpenPositions: cfg.MaxOpenPositions, MaxWaitSeconds: cfg.MaxConfirmationWaitSeconds, MaxAttempts: cfg.MaxSubmissionAttempts, SkipPreflight: cfg.SkipPreflight}
	prices := pricefeed.New(cfg.SOLUSDPriceURL)
	engine := execution.New(db, w, portal, rpcs, prices, log, cfg.Simulation, simulationBalanceSOL, controller, bus, limits)
	strat := strategy.New(db, time.Duration(cfg.TokenMaxAgeSeconds)*time.Second)
	strat.SetScreen(screener.RugCheck)
	monitor := tokenmonitor.New(
		cfg.PumpPortalWS, cfg.PumpPortalKey, cfg.TokenQueueSize, db, strat, engine,
		cfg.TradeUSD, cfg.MaxSlippagePct, cfg.MaxPriorityFeeSOL,
		cfg.TakeProfitGrossPct, cfg.StopLossPct,
		time.Duration(cfg.MaxHoldSeconds)*time.Second, cfg.Simulation, bus, log,
		tokenmonitor.Settings{
			EntryDelay:        time.Duration(cfg.EntryDelaySeconds) * time.Second,
			MinTransactions:   cfg.MinTxnsBeforeBuy,
			PricePollInterval: time.Duration(cfg.PricePollSeconds) * time.Second,
			SnipeConcurrency:  cfg.SnipeConcurrency,
			SnipeTimeout:      time.Duration(cfg.SnipeTimeoutMS) * time.Millisecond,
			DedupeTTL:         time.Duration(cfg.DedupeTTLMS) * time.Millisecond,
			BondingCurve:      rpcs,
		},
	)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	engine.SetRootContext(ctx)
	monitor.SetTrailingStopPct(cfg.TrailingStopPct)
	startupRPC, startupCancel := context.WithTimeout(ctx, 12*time.Second)
	rpcs.Check(startupRPC)
	startupCancel()
	if err := engine.Reconcile(ctx); err != nil {
		log.Warn("startup reconciliation", logging.Err(err))
	}
	go rpcs.Run(ctx, time.Duration(cfg.RPCHealthCheckIntervalSeconds)*time.Second)
	go monitor.Run(ctx)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api.New(db, w, rpcs, engine, controller, monitor, bus, cfg, log, paperDB), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = srv.Shutdown(shutdown)
	}()
	log.Info("server starting")
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("server", logging.Err(err))
	}
}
