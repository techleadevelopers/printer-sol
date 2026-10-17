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
	HTTPAddr, DatabaseURL, DatabasePath, PrivateKey, KeyProviderRef, WalletPublicAddress, PumpPortalKey, PumpPortalURL, PumpPortalWS, AdminToken, SOLUSDPriceURL string
	Simulation, EnableLiveTrading                                                                                                   bool
	MaxTradeSOL, MaxDailyLossSOL, FeeReserveSOL, MaxExposureSOL, MaxSlippagePct, MaxPriorityFeeSOL, TradeUSD                       float64
	TokenQueueSize, MaxOpenPositions, MaxConfirmationWaitSeconds, MaxSubmissionAttempts                                             int
	RPCEndpoints                                                                                                                    []string
}

func Load() (Config, error) {
	c := Config{HTTPAddr: env("HTTP_ADDR", ":8080"), DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL")), DatabasePath: env("DATABASE_PATH", "./data/sniper.db"), PrivateKey: os.Getenv("BOT_PRIVATE_KEY"), KeyProviderRef: os.Getenv("KEY_PROVIDER_REF"), WalletPublicAddress: os.Getenv("WALLET_PUBLIC_ADDRESS"), PumpPortalKey: os.Getenv("PUMPPORTAL_API_KEY"), AdminToken: os.Getenv("ADMIN_API_TOKEN"), PumpPortalURL: env("PUMPPORTAL_URL", "https://pumpportal.fun/api/trade-local"), PumpPortalWS: env("PUMPPORTAL_WS", "wss://pumpportal.fun/api/data"), SOLUSDPriceURL: env("SOL_USD_PRICE_URL", "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"), RPCEndpoints: splitEndpoints(env("RPC_ENDPOINTS", "https://api.mainnet.solana.com,https://solana-rpc.publicnode.com"))}
	var err error
	if c.Simulation, err = boolEnv("SIMULATION", true); err != nil {
		return c, err
	}
	if c.EnableLiveTrading, err = boolEnv("ENABLE_LIVE_TRADING", false); err != nil {
		return c, err
	}
	if c.MaxTradeSOL, err = floatEnv("MAX_TRADE_SOL", .05); err != nil {
		return c, err
	}
	if c.MaxDailyLossSOL, err = floatEnv("MAX_DAILY_LOSS_SOL", .2); err != nil {
		return c, err
	}
	if c.FeeReserveSOL, err = floatEnv("FEE_RESERVE_SOL", .01); err != nil {
		return c, err
	}
	if c.MaxExposureSOL, err = floatEnv("MAX_EXPOSURE_SOL", .2); err != nil {
		return c, err
	}
	if c.MaxSlippagePct, err = floatEnv("MAX_SLIPPAGE_PCT", 10); err != nil {
		return c, err
	}
	if c.MaxPriorityFeeSOL, err = floatEnv("MAX_PRIORITY_FEE_SOL", .01); err != nil {
		return c, err
	}
	if c.TokenQueueSize, err = intEnv("TOKEN_QUEUE_SIZE", 1024); err != nil {
		return c, err
	}
	if c.MaxOpenPositions, err = intEnv("MAX_OPEN_POSITIONS", 3); err != nil {
		return c, err
	}
	if c.MaxConfirmationWaitSeconds, err = intEnv("MAX_CONFIRMATION_WAIT_SECONDS", 45); err != nil {
		return c, err
	}
	if c.MaxSubmissionAttempts, err = intEnv("MAX_SUBMISSION_ATTEMPTS", 1); err != nil {
		return c, err
	}
	if c.TradeUSD, err = floatEnv("TRADE_USD", 1); err != nil {
		return c, err
	}
	if strings.TrimSpace(c.HTTPAddr) == "" || (c.DatabaseURL == "" && strings.TrimSpace(c.DatabasePath) == "") || strings.TrimSpace(c.PumpPortalURL) == "" || strings.TrimSpace(c.PumpPortalWS) == "" || len(c.RPCEndpoints) == 0 {
		return c, fmt.Errorf("HTTP_ADDR, DATABASE_URL (or DATABASE_PATH locally), PumpPortal URLs and at least one RPC endpoint are required")
	}
	if c.DatabaseURL != "" && !strings.HasPrefix(c.DatabaseURL, "postgres://") && !strings.HasPrefix(c.DatabaseURL, "postgresql://") {
		return c, fmt.Errorf("DATABASE_URL must use postgres:// or postgresql://")
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
	if c.TokenQueueSize < 1 || c.MaxOpenPositions < 1 || c.MaxSubmissionAttempts < 1 || c.MaxConfirmationWaitSeconds < 1 {
		return c, fmt.Errorf("queue, position, attempt and wait limits must be positive")
	}
	for _, value := range []float64{c.MaxTradeSOL, c.MaxDailyLossSOL, c.FeeReserveSOL, c.MaxExposureSOL, c.MaxSlippagePct, c.MaxPriorityFeeSOL, c.TradeUSD} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return c, fmt.Errorf("financial limits must be finite and non-negative")
		}
	}
	if !c.Simulation && (!c.EnableLiveTrading || c.PrivateKey == "" || c.KeyProviderRef == "" || c.WalletPublicAddress == "" || c.AdminToken == "") {
		return c, fmt.Errorf("live trading requires ENABLE_LIVE_TRADING=true, BOT_PRIVATE_KEY via KEY_PROVIDER_REF, WALLET_PUBLIC_ADDRESS and ADMIN_API_TOKEN")
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
	v := env(k, strconv.FormatBool(d))
	return strconv.ParseBool(v)
}
func floatEnv(k string, d float64) (float64, error) {
	return strconv.ParseFloat(strings.TrimSpace(env(k, fmt.Sprint(d))), 64)
}
func intEnv(k string, d int) (int, error) { return strconv.Atoi(env(k, strconv.Itoa(d))) }
func splitEndpoints(v string) []string {
	var out []string
	for _, endpoint := range strings.Split(v, ",") {
		if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
			out = append(out, endpoint)
		}
	}
	return out
}
