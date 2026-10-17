package config

import (
	"os"
	"testing"
)

func TestSplitEndpointsDropsEmptyValues(t *testing.T) {
	got := splitEndpoints(" https://one.example, ,https://two.example ")
	if len(got) != 2 || got[0] != "https://one.example" || got[1] != "https://two.example" {
		t.Fatalf("unexpected endpoints: %#v", got)
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
