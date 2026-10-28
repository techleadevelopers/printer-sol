package main

import (
	"context"
	"errors"
	"fmt"
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

type rpcSellConfirmer struct {
	rpcs *rpcmanager.Manager
}

func (c rpcSellConfirmer) ConfirmSellState(ctx context.Context, signature string) (string, error) {
	return c.rpcs.Status(ctx, signature)
}

// executionEntrier adapta *execution.Engine à interface tokenmonitor.Entrier.
// O único ponto de incompatibilidade é o tipo de retorno de ResolveMarketRoute
// (execution.MarketDecision vs tokenmonitor.MarketDecisionLike). Este adapter
// converte um no outro sem tocar em nenhum dos dois pacotes.
type executionEntrier struct {
	engine *execution.Engine
}

func (e executionEntrier) ResolveMarketRoute(ctx context.Context, mint string) (tokenmonitor.MarketDecisionLike, error) {
	decision, err := e.engine.ResolveMarketRoute(ctx, mint)
	if err != nil {
		return tokenmonitor.MarketDecisionLike{}, err
	}
	return tokenmonitor.MarketDecisionLike{
		Kind:   string(decision.Kind),
		Pool:   decision.Pool,
		Reason: decision.Reason,
	}, nil
}

// Os restantes métodos de Entrier delegam diretamente no engine.

func (e executionEntrier) BuyNewTokenUSD(ctx context.Context, mint string, usd, slippage, priorityFee float64, pool, reason string) (string, error) {
	return e.engine.BuyNewTokenUSD(ctx, mint, usd, slippage, priorityFee, pool, reason)
}

func (e executionEntrier) ClosePaperPosition(ctx context.Context, mint string, returnPct float64, reason string) (string, error) {
	return e.engine.ClosePaperPosition(ctx, mint, returnPct, reason)
}

func (e executionEntrier) CloseLivePosition(ctx context.Context, mint string, slippage, priorityFee float64, reason string) (string, error) {
	return e.engine.CloseLivePosition(ctx, mint, slippage, priorityFee, reason)
}

// rpcLogAdapter adapta *rpcmanager.Manager à interface tokenmonitor.LogSubscriber.
// O único ponto de incompatibilidade é o tipo de LogNotification, que difere
// entre os dois pacotes (intencionalmente, para manter o tokenmonitor
// desacoplado do rpcmanager). Este adapter converte um no outro.
//
// Nota: rpcmanager.LogNotification.Err é `any`, enquanto
// tokenmonitor.LogNotification.Err é `error` (necessário para zap.Error no
// monitor.go). A conversão é feita aqui, no único ponto de contacto entre os
// dois pacotes.
type rpcLogAdapter struct {
	rpcs *rpcmanager.Manager
}

func (a rpcLogAdapter) SubscribeNewTokenLogs(ctx context.Context, programID string, handler func(tokenmonitor.LogNotification)) error {
	return a.rpcs.SubscribeNewTokenLogs(ctx, programID, func(n rpcmanager.LogNotification) {
		handler(tokenmonitor.LogNotification{
			Signature: n.Signature,
			Logs:      n.Logs,
			Err:       toError(n.Err),
			Slot:      n.Slot,
		})
	})
}

// toError normaliza o campo Err vindo do rpcmanager (que é `any`) para `error`,
// que é o tipo que o tokenmonitor.LogNotification espera. Aceita nil, error e
// string; qualquer outro valor é formatado com %v.
func toError(v any) error {
	switch e := v.(type) {
	case nil:
		return nil
	case error:
		return e
	case string:
		if e == "" {
			return nil
		}
		return errors.New(e)
	default:
		return fmt.Errorf("%v", e)
	}
}

// rpcEntryMetricsAdapter adapta *rpcmanager.Manager à assinatura esperada pelo
// tokenmonitor para obter o market cap de entrada no fluxo de descoberta via RPC.
// Sem esta injeção, o handleRPCNotification injeta MarketCapSOL=0 e o
// consumeEvent rejeita o token com "missing creation market cap for take-profit
// baseline". Delegamos diretamente em BondingCurveEntryMetrics (mesma assinatura:
// (ctx, mint) -> (marketCapSOL, tokenPriceSOL, err)).
type rpcEntryMetricsAdapter struct {
	rpcs *rpcmanager.Manager
}

func (a rpcEntryMetricsAdapter) FetchEntryMetrics(ctx context.Context, mint string) (float64, float64, error) {
	return a.rpcs.BondingCurveEntryMetrics(ctx, mint)
}

func main() {
	cfg, err := config.Load()
	if err != nil {
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
		if _, unlockErr := paperDB.RequireLiveUnlock(); unlockErr != nil {
			log.Warn("live trading bypassed paper evidence", logging.Err(unlockErr))
		} else {
			log.Info("paper evidence accepted for live mode")
		}
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
	db.SetSellConfirmer(rpcSellConfirmer{rpcs: rpcs})
	portal := pumpportal.New(cfg.PumpPortalURL)
	controller := bot.New(bus)
	controller.Start()

	limits := execution.Limits{
		MaxTradeSOL: cfg.MaxTradeSOL, MaxExposureSOL: cfg.MaxExposureSOL, MaxDailyLossSOL: cfg.MaxDailyLossSOL,
		MaxSlippagePct: cfg.MaxSlippagePct, MaxSlippageBPS: cfg.MaxSlippageBPS,
		MaxQuoteAgeMS: cfg.MaxQuoteAgeMS, MaxCurveImpactBPS: cfg.MaxCurveImpactBPS,
		MaxLossPerTradePct: cfg.StopLossPct, MaxPriorityFeeSOL: cfg.MaxPriorityFeeSOL, FeeReserveSOL: cfg.FeeReserveSOL,
		SimulationBaseFeeSOL: cfg.SimulationBaseFeeSOL, MaxOpenPositions: cfg.MaxOpenPositions,
		MaxWaitSeconds: cfg.MaxConfirmationWaitSeconds, MaxAttempts: cfg.MaxSubmissionAttempts,
		SkipPreflight:          cfg.SkipPreflight,
		MinEntryScoreFirstPump: cfg.MinEntryScoreFirstPump,
		MinEntryScoreMomentum:  cfg.MinEntryScoreMomentum,
	}
	prices := pricefeed.New(cfg.SOLUSDPriceURL)
	engine := execution.New(db, w, portal, rpcs, prices, log, cfg.Simulation, simulationBalanceSOL, controller, bus, limits)
	strat := strategy.New(db, time.Duration(cfg.TokenMaxAgeSeconds)*time.Second)
	if cfg.EnableRugCheck {
		strat.SetScreen(screener.RugCheck)
	}

	profiles := tokenmonitor.DefaultProfiles()
	profiles.Mode = tokenmonitor.EntryProfileMode(cfg.EntryProfileMode)
	profiles.FirstPump.Enabled = cfg.FirstPumpEnabled
	profiles.FirstPump.MaxTokenAge = time.Duration(cfg.FirstPumpMaxTokenAgeSecs) * time.Second
	profiles.FirstPump.MinMarketCapSOL = cfg.FirstPumpMinMarketCapSOL
	profiles.FirstPump.MaxMarketCapSOL = cfg.FirstPumpMaxMarketCapSOL
	profiles.FirstPump.MinVolumeBeforeBuySOL = cfg.FirstPumpMinVolumeSOL
	profiles.FirstPump.MaxVolumeBeforeBuySOL = cfg.FirstPumpMaxVolumeSOL
	profiles.FirstPump.ScoreWeightVolume = cfg.FirstPumpScoreWeightVolume
	profiles.FirstPump.ScoreWeightBuyRatio = cfg.FirstPumpScoreWeightBuyRatio
	profiles.FirstPump.ScoreWeightBuyers = cfg.FirstPumpScoreWeightBuyers
	profiles.FirstPump.ScoreWeightAccel = cfg.FirstPumpScoreWeightAccel
	profiles.FirstPump.ScoreVolumeCapSOL = cfg.FirstPumpScoreVolumeCapSOL
	profiles.FirstPump.ScoreBuyersCap = cfg.FirstPumpScoreBuyersCap

	profiles.Momentum.Enabled = cfg.MomentumProfileEnabled
	profiles.Momentum.MaxTokenAge = time.Duration(cfg.MomentumProfileMaxAgeSecs) * time.Second
	profiles.Momentum.MinMarketCapSOL = cfg.MomentumProfileMinMcapSOL
	profiles.Momentum.MaxMarketCapSOL = cfg.MomentumProfileMaxMcapSOL
	profiles.Momentum.MinVolumeBeforeBuySOL = cfg.MomentumProfileMinVolSOL
	profiles.Momentum.MaxVolumeBeforeBuySOL = cfg.MomentumProfileMaxVolSOL
	profiles.Momentum.MinHolders = cfg.MomentumProfileMinHolders
	profiles.Momentum.RequireMomentum = cfg.MomentumProfileRequireMom
	profiles.Momentum.ScoreWeightVolume = cfg.MomentumScoreWeightVolume
	profiles.Momentum.ScoreWeightBuyRatio = cfg.MomentumScoreWeightBuyRatio
	profiles.Momentum.ScoreWeightBuyers = cfg.MomentumScoreWeightBuyers
	profiles.Momentum.ScoreWeightAccel = cfg.MomentumScoreWeightAccel
	profiles.Momentum.ScoreVolumeCapSOL = cfg.MomentumScoreVolumeCapSOL
	profiles.Momentum.ScoreBuyersCap = cfg.MomentumScoreBuyersCap

	var holders tokenmonitor.HolderSource = tokenmonitor.NoopHolderSource{}
	if cfg.SolscanAPIKey != "" {
		holders = tokenmonitor.NewCachedHolderSource(
			tokenmonitor.NewSolscanHolderSource(cfg.SolscanAPIKey),
			time.Duration(cfg.HoldersCacheSeconds)*time.Second,
		)
	}

	// Aqui está a correção: passamos executionEntrier{engine} em vez de engine.
	monitor := tokenmonitor.New(
		cfg.PumpPortalWS, cfg.PumpPortalKey, cfg.TokenQueueSize, db, strat,
		executionEntrier{engine: engine},
		cfg.TradeUSD, float64(cfg.MaxSlippageBPS)/100, cfg.MaxPriorityFeeSOL,
		cfg.TakeProfitGrossPct, cfg.StopLossPct,
		time.Duration(cfg.MaxHoldSeconds)*time.Second, cfg.Simulation, bus, log,
		tokenmonitor.Settings{
			EntryDelay:            time.Duration(cfg.EntryDelaySeconds) * time.Second,
			MinVolumeBeforeBuySOL: cfg.MinVolumeBeforeBuySOL,
			MaxVolumeBeforeBuySOL: cfg.MaxVolumeBeforeBuySOL,
			MinMarketCapSOL:       cfg.MinMarketCapSOL,
			MaxMarketCapSOL:       cfg.MaxMarketCapSOL,
			StuckExitQuarantine:   time.Duration(cfg.StuckExitQuarantineSeconds) * time.Second,
			TrailingStopArmPct:    cfg.TrailingStopArmPct,
			ExitTriggerBufferPct:  cfg.ExitTriggerBufferPct,
			UseWSMarketCap:        cfg.UseWSMarketCap,
			MaxTokenAge:           time.Duration(cfg.TokenMaxAgeSeconds) * time.Second,
			PricePollInterval:     time.Duration(cfg.PricePollSeconds) * time.Second,
			SnipeConcurrency:      cfg.SnipeConcurrency,
			SnipeTimeout:          time.Duration(cfg.SnipeTimeoutMS) * time.Millisecond,
			DedupeTTL:             time.Duration(cfg.DedupeTTLMS) * time.Millisecond,
			BondingCurve:          rpcs,
			CreationResolver:      rpcs,
			Profiles:              profiles,
			Holders:               holders,
			MaxOpenPositions:      cfg.MaxOpenPositions, // NOVO: vem de MAX_OPEN_POSITIONS
			PumpPortalDetection:   cfg.PumpPortalDetection, // CORREÇÃO: respeita PUMPPORTAL_DETECTION
			LogSubscriber:         rpcLogAdapter{rpcs: rpcs}, // NOVO
			FetchEntryMetrics:     rpcEntryMetricsAdapter{rpcs: rpcs}.FetchEntryMetrics, // NOVO: preenche MarketCapSOL no fluxo RPC
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
	if err := engine.ReconcileUnknown(ctx); err != nil {
		log.Warn("startup unknown-trade reconciliation", logging.Err(err))
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