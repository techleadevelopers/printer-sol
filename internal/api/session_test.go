package api

import (
	"net/http"
	"net/http/httptest"
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

func TestPrivateReadRoutesRequireAdminToken(t *testing.T) {
	handler := New(nil, nil, nil, nil, nil, nil, nil, config.Config{AdminPassword: "test-admin-password"}, nil)
	paths := []string{
		"/api/dashboard",
		"/api/tokens",
		"/api/positions",
		"/api/trades",
		"/api/rpcs",
		"/api/config",
		"/api/wallet",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated GET %s returned %d; want %d", path, response.Code, http.StatusUnauthorized)
			}
		})
	}
}
