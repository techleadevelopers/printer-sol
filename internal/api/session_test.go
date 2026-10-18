package api

import (
	"testing"
	"time"

	"solana-sniper-backend/internal/config"
)

func TestSessionTokenExpiresAndCannotBeTamperedWith(t *testing.T) {
	server := &Server{cfg: config.Config{AdminPassword: "test-admin-password"}}
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	token := server.sessionTokenAt(now)

	if !server.validSessionToken(token, now.Add(59*time.Minute)) {
		t.Fatal("valid session was rejected before its one-hour expiry")
	}
	if server.validSessionToken(token, now.Add(time.Hour+time.Second)) {
		t.Fatal("expired session was accepted")
	}
	if server.validSessionToken(token+"x", now) {
		t.Fatal("tampered session token was accepted")
	}
	if server.validSessionToken(server.sessionTokenAt(now.Add(time.Minute)), now) {
		t.Fatal("session issued too far in the future was accepted")
	}
}
