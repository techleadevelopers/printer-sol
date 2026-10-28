package rpcmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// LogNotification é o subconjunto relevante de uma notificação logsSubscribe.
// O tokenmonitor define a sua própria struct equivalente para manter o
// desacoplamento entre pacotes; o main.go faz a conversão.
type LogNotification struct {
	Signature string
	Logs      []string
	Err       any
	Slot      uint64
}

// LogHandler recebe cada notificação de logs. Deve retornar rapidamente;
// o rpcmanager não bloqueia o leitor do WebSocket à espera do handler.
type LogHandler func(LogNotification)

// SubscribeNewTokenLogs abre uma ligação WebSocket ao RPC atualmente
// selecionado (via currentURL) e subscreve logsSubscribe para o programID
// indicado. Reconecta automaticamente com backoff exponencial (máx 30s).
// Só termina quando o ctx é cancelado.
//
// IMPORTANTE:
//   - Usa gorilla/websocket diretamente (dialer próprio), sem partilhar estado
//     com o cliente HTTP do rpcmanager.
//   - A conversão http(s):// → ws(s):// é feita aqui porque os endpoints do
//     Manager são URLs HTTP.
//   - O handler corre sequencialmente no goroutine de leitura; mantém-no leve.
func (m *Manager) SubscribeNewTokenLogs(ctx context.Context, programID string, handler LogHandler) error {
	if strings.TrimSpace(programID) == "" {
		return fmt.Errorf("programID is required for logsSubscribe")
	}
	backoff := time.Second
	for ctx.Err() == nil {
		endpoint, err := m.currentURL()
		if err != nil {
			m.log.Warn("rpcmanager: no healthy RPC endpoint for WS discovery", zap.Error(err))
		} else if err = m.runLogsSubscription(ctx, endpoint, programID, handler); err != nil && ctx.Err() == nil {
			m.log.Warn("rpcmanager: logs subscription ended",
				zap.String("endpoint", endpoint), zap.Error(err))
		} else if err == nil {
			// runLogsSubscription devolve nil apenas se o ctx terminou.
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
	return ctx.Err()
}

// runLogsSubscription abre o WS, envia logsSubscribe e processa mensagens até
// o ctx terminar ou a ligação cair.
func (m *Manager) runLogsSubscription(ctx context.Context, httpEndpoint, programID string, handler LogHandler) error {
	wsURL, err := httpToWS(httpEndpoint)
	if err != nil {
		return err
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()

	subRequest := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "logsSubscribe",
		"params": []any{
			map[string]any{"mentions": []string{programID}},
			map[string]any{"commitment": "confirmed"},
		},
	}
	if err := conn.WriteJSON(subRequest); err != nil {
		return fmt.Errorf("send logsSubscribe: %w", err)
	}

	done := make(chan struct{})
	defer close(done)

	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { _ = conn.Close() }) }

	go func() {
		select {
		case <-ctx.Done():
			closeConn()
		case <-done:
		}
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read WS: %w", err)
		}
		var envelope struct {
			Method string `json:"method"`
			Params struct {
				Result struct {
					Context struct {
						Slot uint64 `json:"slot"`
					} `json:"context"`
					Value struct {
						Signature string   `json:"signature"`
						Err       any      `json:"err"`
						Logs      []string `json:"logs"`
					} `json:"value"`
				} `json:"result"`
			} `json:"params"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			continue
		}
		if envelope.Method != "logsNotification" {
			// resposta ao pedido de subscrição, heartbeat, etc.
			continue
		}
		handler(LogNotification{
			Signature: envelope.Params.Result.Value.Signature,
			Logs:      envelope.Params.Result.Value.Logs,
			Err:       envelope.Params.Result.Value.Err,
			Slot:      envelope.Params.Result.Context.Slot,
		})
	}
}

func httpToWS(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	return u.String(), nil
}