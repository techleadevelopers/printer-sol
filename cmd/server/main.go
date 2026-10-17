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
	"solana-sniper-backend/internal/pumpportal"
	"solana-sniper-backend/internal/pricefeed"
	"solana-sniper-backend/internal/rpcmanager"
	"solana-sniper-backend/internal/strategy"
	"solana-sniper-backend/internal/tokenmonitor"
	"solana-sniper-backend/internal/wallet"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log, closeLog := logging.New()
	defer closeLog()
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
	limits := execution.Limits{MaxTradeSOL: cfg.MaxTradeSOL, MaxExposureSOL: cfg.MaxExposureSOL, MaxDailyLossSOL: cfg.MaxDailyLossSOL, MaxSlippagePct: cfg.MaxSlippagePct, MaxPriorityFeeSOL: cfg.MaxPriorityFeeSOL, FeeReserveSOL: cfg.FeeReserveSOL, MaxOpenPositions: cfg.MaxOpenPositions, MaxWaitSeconds: cfg.MaxConfirmationWaitSeconds, MaxAttempts: cfg.MaxSubmissionAttempts}
	prices := pricefeed.New(cfg.SOLUSDPriceURL)
	engine := execution.New(db, w, portal, rpcs, prices, log, cfg.Simulation, controller, bus, limits)
	strat := strategy.New(db, w, cfg.MaxTradeSOL, cfg.MaxDailyLossSOL)
	monitor := tokenmonitor.New(cfg.PumpPortalWS, cfg.PumpPortalKey, cfg.TokenQueueSize, db, strat, bus, log)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startupRPC, startupCancel := context.WithTimeout(ctx, 12*time.Second)
	rpcs.Check(startupRPC)
	startupCancel()
	if err := engine.Reconcile(ctx); err != nil {
		log.Warn("startup reconciliation", logging.Err(err))
	}
	go rpcs.Run(ctx, time.Duration(cfg.RPCHealthCheckIntervalSeconds)*time.Second)
	go monitor.Run(ctx)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: api.New(db, w, rpcs, engine, controller, monitor, bus, cfg, log), ReadHeaderTimeout: 5 * time.Second}
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
