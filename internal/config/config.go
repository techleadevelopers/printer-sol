package config

import (
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	HTTPAddr, DatabaseURL, DatabasePath, SimulationDatabaseURL, RealDatabaseURL, PrivateKey, KeyProviderRef, WalletPublicAddress, PumpPortalKey, PumpPortalURL, PumpPortalWS, AdminPassword, SOLUSDPriceURL string
	Simulation, EnableLiveTrading, LiveUnlockRequiresPaper, AsyncRugCheckAfterEntry, SkipPreflight                                                                                                           bool
	MaxTradeSOL, MaxDailyLossSOL, FeeReserveSOL, MaxExposureSOL, MaxSlippagePct, MaxPriorityFeeSOL, TradeUSD, SimulationBalanceSOL, TakeProfitGrossPct, StopLossPct, TrailingStopPct float64
	SimulationBaseFeeSOL                                                                                                                                                                    float64
	TokenQueueSize, MaxOpenPositions, MaxConfirmationWaitSeconds, MaxSubmissionAttempts, RPCHealthCheckIntervalSeconds, TokenMaxAgeSeconds, MaxHoldSeconds                                                  int
	RPCEndpoints                                                                                                                                                                                            []string
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL")), DatabasePath: env("DATABASE_PATH", "./data/sniper.db"), SimulationDatabaseURL: strings.TrimSpace(os.Getenv("SIMULATION_DATABASE_URL")), RealDatabaseURL: strings.TrimSpace(os.Getenv("REAL_DATABASE_URL")), PrivateKey: os.Getenv("BOT_PRIVATE_KEY"), KeyProviderRef: os.Getenv("KEY_PROVIDER_REF"), WalletPublicAddress: os.Getenv("WALLET_PUBLIC_ADDRESS"), PumpPortalKey: os.Getenv("PUMPPORTAL_API_KEY"), AdminPassword: os.Getenv("ADMIN_PASSWORD"), PumpPortalURL: env("PUMPPORTAL_URL", "https://pumpportal.fun/api/trade-local"), PumpPortalWS: env("PUMPPORTAL_WS", "wss://pumpportal.fun/api/data"), SOLUSDPriceURL: env("SOL_USD_PRICE_URL", "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"), RPCEndpoints: splitEndpoints(env("RPC_ENDPOINTS", "https://api.mainnet.solana.com,https://solana-rpc.publicnode.com"))}
	var err error
	if c.Simulation, err = boolEnv("SIMULATION", true); err != nil {
		return c, err
	}
	if c.EnableLiveTrading, err = boolEnv("ENABLE_LIVE_TRADING", false); err != nil {
		return c, err
	}
	if c.LiveUnlockRequiresPaper, err = boolEnv("LIVE_UNLOCK_REQUIRES_PAPER", true); err != nil {
		return c, err
	}
	if c.AsyncRugCheckAfterEntry, err = boolEnv("ASYNC_RUGCHECK_AFTER_ENTRY", true); err != nil {
		return c, err
	}
	if c.SkipPreflight, err = boolEnv("SKIP_PREFLIGHT", true); err != nil {
		return c, err
	}
	if c.SimulationDatabaseURL != "" || c.RealDatabaseURL != "" {
		if c.SimulationDatabaseURL == "" || c.RealDatabaseURL == "" {
			return c, fmt.Errorf("SIMULATION_DATABASE_URL and REAL_DATABASE_URL must both be set")
		}
		if c.SimulationDatabaseURL == c.RealDatabaseURL {
			return c, fmt.Errorf("simulation and real databases must be different")
		}
		c.DatabaseURL = c.RealDatabaseURL
		if c.Simulation {
			c.DatabaseURL = c.SimulationDatabaseURL
		}
		c.DatabasePath = ""
	} else if c.DatabaseURL != "" {
		return c, fmt.Errorf("DATABASE_URL cannot be shared between simulation and real modes; configure SIMULATION_DATABASE_URL and REAL_DATABASE_URL")
	} else if !c.Simulation {
		// A local SQLite fallback is convenient for paper trading, but it must
		// never become the live ledger: it could contain simulated positions,
		// P&L, and trade history from a previous run.
		return c, fmt.Errorf("live trading requires separate SIMULATION_DATABASE_URL and REAL_DATABASE_URL; DATABASE_PATH is simulation-only")
	}
	if c.MaxTradeSOL, err = floatEnv("MAX_TRADE_SOL", .035); err != nil {
		return c, err
	}
	if c.MaxDailyLossSOL, err = floatEnv("MAX_DAILY_LOSS_SOL", .15); err != nil {
		return c, err
	}
	if c.FeeReserveSOL, err = floatEnv("FEE_RESERVE_SOL", .02); err != nil {
		return c, err
	}
	if c.MaxExposureSOL, err = floatEnv("MAX_EXPOSURE_SOL", .5); err != nil {
		return c, err
	}
	if c.MaxSlippagePct, err = floatEnv("MAX_SLIPPAGE_PCT", 15); err != nil {
		return c, err
	}
	if c.MaxPriorityFeeSOL, err = floatEnv("MAX_PRIORITY_FEE_SOL", .0015); err != nil {
		return c, err
	}
	if c.TokenQueueSize, err = intEnv("TOKEN_QUEUE_SIZE", 1024); err != nil {
		return c, err
	}
	if c.MaxOpenPositions, err = intEnv("MAX_OPEN_POSITIONS", 20); err != nil {
		return c, err
	}
	if c.MaxConfirmationWaitSeconds, err = intEnv("MAX_CONFIRMATION_WAIT_SECONDS", 12); err != nil {
		return c, err
	}
	if c.MaxSubmissionAttempts, err = intEnv("MAX_SUBMISSION_ATTEMPTS", 3); err != nil {
		return c, err
	}
	if c.RPCHealthCheckIntervalSeconds, err = intEnv("RPC_HEALTH_CHECK_INTERVAL_SECONDS", 45); err != nil {
		return c, err
	}
	tradeUSDDefault := 3.0
	if !c.Simulation {
		// Live notional must be an explicit operator decision. Do not silently
		// carry the paper-trading default into a real wallet.
		tradeUSDDefault = 0
	}
	if c.TradeUSD, err = floatEnv("TRADE_USD", tradeUSDDefault); err != nil {
		return c, err
	}
	if c.SimulationBalanceSOL, err = floatEnv("SIMULATION_BALANCE_SOL", 1); err != nil {
		return c, err
	}
	if c.SimulationBaseFeeSOL, err = floatEnv("SIMULATION_BASE_FEE_SOL", 0.000005); err != nil {
		return c, err
	}
	if c.TokenMaxAgeSeconds, err = intEnv("TOKEN_MAX_AGE_SECONDS", 5); err != nil {
		return c, err
	}
	if c.TakeProfitGrossPct, err = floatEnv("TAKE_PROFIT_GROSS_PCT", 50); err != nil {
		return c, err
	}
	stopLossDefault := 0.0
	if c.Simulation {
		stopLossDefault = 25
	}
	if c.StopLossPct, err = floatEnv("STOP_LOSS_PCT", stopLossDefault); err != nil {
		return c, err
	}
	if c.MaxHoldSeconds, err = intEnv("MAX_HOLD_SECONDS", 90); err != nil {
		return c, err
	}
	if c.TrailingStopPct, err = floatEnv("TRAILING_STOP_PCT", 12); err != nil {
		return c, err
	}
	if strings.TrimSpace(c.HTTPAddr) == "" || (c.DatabaseURL == "" && strings.TrimSpace(c.DatabasePath) == "") || strings.TrimSpace(c.PumpPortalURL) == "" || strings.TrimSpace(c.PumpPortalWS) == "" || len(c.RPCEndpoints) == 0 {
		return c, fmt.Errorf("HTTP_ADDR, DATABASE_URL (or DATABASE_PATH locally), PumpPortal URLs and at least one RPC endpoint are required")
	}
	if c.DatabaseURL != "" && !strings.HasPrefix(c.DatabaseURL, "postgres://") && !strings.HasPrefix(c.DatabaseURL, "postgresql://") {
		return c, fmt.Errorf("DATABASE_URL must use postgres:// or postgresql://")
	}
	if strings.TrimSpace(c.AdminPassword) == "" {
		return c, fmt.Errorf("ADMIN_PASSWORD is required")
	}
	if err := validateURL(c.PumpPortalURL, "http", "https"); err != nil {
		return c, fmt.Errorf("PUMPPORTAL_URL: %w", err)
	}
	if err := validateURL(c.PumpPortalWS, "ws", "wss"); err != nil {
		return c, fmt.Errorf("PUMPPORTAL_WS: %w", err)
	}
	if err := validateURL(c.SOLUSDPriceURL, "http", "https"); err != nil {
		return c, fmt.Errorf("SOL_USD_PRICE_URL: %w", err)
	}
	for _, endpoint := range c.RPCEndpoints {
		if err := validateURL(endpoint, "http", "https"); err != nil {
			return c, fmt.Errorf("RPC_ENDPOINTS: %w", err)
		}
	}
	if c.TokenQueueSize < 1 || c.MaxOpenPositions < 1 || c.MaxSubmissionAttempts < 1 || c.MaxSubmissionAttempts > 6 || c.MaxConfirmationWaitSeconds < 1 || c.RPCHealthCheckIntervalSeconds < 1 || c.TokenMaxAgeSeconds < 0 || c.MaxHoldSeconds < 0 {
		return c, fmt.Errorf("queue, position, wait and RPC health-check limits must be positive; submission attempts must be 1-6 and token age cannot be negative")
	}
	if !c.Simulation && c.TokenMaxAgeSeconds == 0 {
		return c, fmt.Errorf("live trading requires TOKEN_MAX_AGE_SECONDS > 0")
	}
	for _, value := range []float64{c.MaxTradeSOL, c.MaxDailyLossSOL, c.FeeReserveSOL, c.MaxExposureSOL, c.MaxSlippagePct, c.MaxPriorityFeeSOL, c.TradeUSD, c.SimulationBalanceSOL, c.SimulationBaseFeeSOL, c.TakeProfitGrossPct, c.StopLossPct, c.TrailingStopPct} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return c, fmt.Errorf("financial limits must be finite and non-negative")
		}
	}
	if !c.Simulation && (!c.EnableLiveTrading || c.PrivateKey == "" || c.KeyProviderRef == "" || c.WalletPublicAddress == "" || c.PumpPortalKey == "") {
		return c, fmt.Errorf("live trading requires ENABLE_LIVE_TRADING=true, BOT_PRIVATE_KEY via KEY_PROVIDER_REF, WALLET_PUBLIC_ADDRESS and PUMPPORTAL_API_KEY")
	}
	if !c.Simulation && c.TradeUSD <= 0 {
		return c, fmt.Errorf("live trading requires an explicit TRADE_USD greater than zero")
	}
	if c.Simulation && strings.TrimSpace(c.PumpPortalKey) == "" {
		return c, fmt.Errorf("simulation requires PUMPPORTAL_API_KEY to receive token price updates for virtual exits")
	}
	if !c.Simulation && (c.StopLossPct <= 0 || c.MaxHoldSeconds <= 0) {
		return c, fmt.Errorf("live trading requires STOP_LOSS_PCT > 0 and MAX_HOLD_SECONDS > 0")
	}
	if c.Simulation && (c.StopLossPct <= 0 || c.TakeProfitGrossPct <= 0 || c.TrailingStopPct <= 0) {
		return c, fmt.Errorf("simulation requires STOP_LOSS_PCT, TAKE_PROFIT_GROSS_PCT and TRAILING_STOP_PCT to be positive")
	}
	if !c.Simulation && c.TrailingStopPct <= 0 {
		return c, fmt.Errorf("live trading requires TRAILING_STOP_PCT > 0")
	}
	if c.Simulation && c.EnableLiveTrading {
		return c, fmt.Errorf("SIMULATION=true and ENABLE_LIVE_TRADING=true cannot be enabled together")
	}
	if c.Simulation && c.SimulationBalanceSOL <= 0 {
		return c, fmt.Errorf("SIMULATION_BALANCE_SOL must be positive when SIMULATION=true")
	}
	return c, nil
}
func validateURL(raw string, allowed ...string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" {
		return fmt.Errorf("must be an absolute URL")
	}
	for _, scheme := range allowed {
		if u.Scheme == scheme {
			return nil
		}
	}
	return fmt.Errorf("unsupported URL scheme %q", u.Scheme)
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func boolEnv(k string, d bool) (bool, error) {
	v := cleanScalar(env(k, strconv.FormatBool(d)))
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false (got %q): %w", k, v, err)
	}
	return parsed, nil
}
func floatEnv(k string, d float64) (float64, error) {
	v := cleanScalar(env(k, fmt.Sprint(d)))
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number (got %q): %w", k, v, err)
	}
	return parsed, nil
}
func intEnv(k string, d int) (int, error) {
	v := cleanScalar(env(k, strconv.Itoa(d)))
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer (got %q): %w", k, v, err)
	}
	return parsed, nil
}

// cleanScalar accepts values copied from JSON, Docker/Railway forms, and .env
// files. Those interfaces commonly leave a leading escaped or unclosed quote
// around otherwise valid scalar values (for example \"1 or "1).
func cleanScalar(v string) string {
	v = strings.TrimSpace(v)
	for strings.HasPrefix(v, `\"`) {
		v = strings.TrimPrefix(v, `\`)
	}
	v = strings.TrimPrefix(v, `\"`)
	v = strings.Trim(v, "\"'")
	return strings.TrimSpace(v)
}
func splitEndpoints(v string) []string {
	var out []string
	for _, endpoint := range strings.Split(v, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			out = append(out, endpoint)
		}
	}
	return out
}
