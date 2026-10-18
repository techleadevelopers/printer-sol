package config

import (
	"os"
	"strings"
	"testing"
)

func TestSplitEndpointsDropsEmptyValues(t *testing.T) {
	got := splitEndpoints(" https://one.example, ,https://two.example ")
	if len(got) != 2 || got[0] != "https://one.example" || got[1] != "https://two.example" {
		t.Fatalf("unexpected endpoints: %#v", got)
	}
}

func TestScalarEnvironmentValuesAcceptAccidentalQuotes(t *testing.T) {
	cases := []string{`"1"`, `"1`, `\"1`, `  \"1\"  `}
	old, wasSet := os.LookupEnv("TEST_FLOAT")
	defer func() {
		if wasSet {
			_ = os.Setenv("TEST_FLOAT", old)
			return
		}
		_ = os.Unsetenv("TEST_FLOAT")
	}()
	for _, raw := range cases {
		_ = os.Setenv("TEST_FLOAT", raw)
		got, err := floatEnv("TEST_FLOAT", 0)
		if err != nil || got != 1 {
			t.Fatalf("floatEnv(%q) = %v, %v; want 1, nil", raw, got, err)
		}
	}
}

func TestFloatEnvironmentValueNamesInvalidSetting(t *testing.T) {
	_ = os.Setenv("TEST_FLOAT", "not-a-number")
	defer os.Unsetenv("TEST_FLOAT")
	_, err := floatEnv("TEST_FLOAT", 0)
	if err == nil || !strings.Contains(err.Error(), "TEST_FLOAT") {
		t.Fatalf("expected named configuration error, got %v", err)
	}
}

func TestLiveTradingRequiresSafetyConfiguration(t *testing.T) {
	keys := []string{"SIMULATION", "ENABLE_LIVE_TRADING", "BOT_PRIVATE_KEY", "KEY_PROVIDER_REF", "WALLET_PUBLIC_ADDRESS", "ADMIN_PASSWORD"}
	old := map[string]string{}
	for _, k := range keys {
		old[k] = os.Getenv(k)
	}
	defer func() {
		for _, k := range keys {
			_ = os.Setenv(k, old[k])
		}
	}()
	_ = os.Setenv("SIMULATION", "false")
	_ = os.Setenv("ENABLE_LIVE_TRADING", "true")
	_ = os.Setenv("BOT_PRIVATE_KEY", "")
	_ = os.Setenv("KEY_PROVIDER_REF", "")
	_ = os.Setenv("WALLET_PUBLIC_ADDRESS", "")
	_ = os.Setenv("ADMIN_PASSWORD", "test-master-password")
	if _, err := Load(); err == nil {
		t.Fatal("expected live trading validation error")
	}
}

func TestLiveTradingRefusesLocalSimulationLedger(t *testing.T) {
	keys := []string{"SIMULATION", "ENABLE_LIVE_TRADING", "BOT_PRIVATE_KEY", "KEY_PROVIDER_REF", "WALLET_PUBLIC_ADDRESS", "ADMIN_PASSWORD", "DATABASE_URL", "SIMULATION_DATABASE_URL", "REAL_DATABASE_URL", "DATABASE_PATH"}
	old := map[string]string{}
	for _, k := range keys {
		old[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	defer func() {
		for _, k := range keys {
			_ = os.Setenv(k, old[k])
		}
	}()
	_ = os.Setenv("SIMULATION", "false")
	_ = os.Setenv("ENABLE_LIVE_TRADING", "true")
	_ = os.Setenv("BOT_PRIVATE_KEY", "test-key")
	_ = os.Setenv("KEY_PROVIDER_REF", "test-provider")
	_ = os.Setenv("WALLET_PUBLIC_ADDRESS", "test-wallet")
	_ = os.Setenv("ADMIN_PASSWORD", "test-master-password")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "separate SIMULATION_DATABASE_URL and REAL_DATABASE_URL") {
		t.Fatalf("expected isolated-ledger error, got %v", err)
	}
}

func TestLiveTradingRequiresStopLossAndMaxHold(t *testing.T) {
	keys := []string{"SIMULATION", "ENABLE_LIVE_TRADING", "BOT_PRIVATE_KEY", "KEY_PROVIDER_REF", "WALLET_PUBLIC_ADDRESS", "PUMPPORTAL_API_KEY", "ADMIN_PASSWORD", "SIMULATION_DATABASE_URL", "REAL_DATABASE_URL", "STOP_LOSS_PCT", "MAX_HOLD_SECONDS"}
	old := map[string]string{}
	for _, k := range keys {
		old[k] = os.Getenv(k)
	}
	defer func() {
		for _, k := range keys {
			_ = os.Setenv(k, old[k])
		}
	}()
	_ = os.Setenv("SIMULATION", "false")
	_ = os.Setenv("ENABLE_LIVE_TRADING", "true")
	_ = os.Setenv("BOT_PRIVATE_KEY", "test-key")
	_ = os.Setenv("KEY_PROVIDER_REF", "test-provider")
	_ = os.Setenv("WALLET_PUBLIC_ADDRESS", "test-wallet")
	_ = os.Setenv("PUMPPORTAL_API_KEY", "test-key")
	_ = os.Setenv("ADMIN_PASSWORD", "test-master-password")
	_ = os.Setenv("SIMULATION_DATABASE_URL", "postgresql://sim.example/db")
	_ = os.Setenv("REAL_DATABASE_URL", "postgresql://real.example/db")
	_ = os.Setenv("STOP_LOSS_PCT", "0")
	_ = os.Setenv("MAX_HOLD_SECONDS", "0")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "STOP_LOSS_PCT") {
		t.Fatalf("expected exit-control error, got %v", err)
	}
}

func TestValidateURLRejectsWrongSchemeAndRelativeURL(t *testing.T) {
	if err := validateURL("https://rpc.example", "http", "https"); err != nil {
		t.Fatalf("expected valid HTTPS URL: %v", err)
	}
	if err := validateURL("ftp://rpc.example", "http", "https"); err == nil {
		t.Fatal("expected unsupported scheme to be rejected")
	}
	if err := validateURL("/relative", "http", "https"); err == nil {
		t.Fatal("expected relative URL to be rejected")
	}
}
