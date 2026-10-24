package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Pump fun program id (bonding curve). Ajuste se usar outro programa.
const pumpFunProgramID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

type CreationTimeResolver struct {
	endpoint string
	client   *http.Client
}

func NewCreationTimeResolver(endpoint string) *CreationTimeResolver {
	return &CreationTimeResolver{
		endpoint: endpoint,
		client:   &http.Client{Timeout: 4 * time.Second},
	}
}

func (r *CreationTimeResolver) ResolveCreationTime(ctx context.Context, mint, signature string) (time.Time, error) {
	if signature != "" {
		if t, err := r.blockTimeOf(ctx, signature); err == nil && !t.IsZero() {
			return t, nil
		}
	}
	if mint == "" {
		return time.Time{}, fmt.Errorf("no signature and no mint to resolve creation time")
	}
	sig, err := r.firstSignatureForMint(ctx, mint)
	if err != nil {
		return time.Time{}, err
	}
	return r.blockTimeOf(ctx, sig)
}

func (r *CreationTimeResolver) firstSignatureForMint(ctx context.Context, mint string) (string, error) {
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getSignaturesForAddress",
		"params": []any{
			mint,
			map[string]any{"limit": 1, "commitment": "confirmed"},
		},
	}
	var out struct {
		Result []struct {
			Signature string `json:"signature"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := r.call(ctx, body, &out); err != nil {
		return "", err
	}
	if out.Error != nil {
		return "", fmt.Errorf("getSignaturesForAddress: %v", out.Error)
	}
	if len(out.Result) == 0 || out.Result[0].Signature == "" {
		return "", fmt.Errorf("no signatures for mint %s", mint)
	}
	return out.Result[0].Signature, nil
}

func (r *CreationTimeResolver) blockTimeOf(ctx context.Context, signature string) (time.Time, error) {
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTransaction",
		"params": []any{
			signature,
			map[string]any{
				"encoding":                       "json",
				"commitment":                     "confirmed",
				"maxSupportedTransactionVersion": 0,
			},
		},
	}
	var out struct {
		Result *struct {
			BlockTime *int64 `json:"blockTime"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := r.call(ctx, body, &out); err != nil {
		return time.Time{}, err
	}
	if out.Error != nil {
		return time.Time{}, fmt.Errorf("getTransaction %s: %v", signature, out.Error)
	}
	if out.Result == nil || out.Result.BlockTime == nil || *out.Result.BlockTime <= 0 {
		return time.Time{}, fmt.Errorf("getTransaction %s: missing blockTime", signature)
	}
	return time.Unix(*out.Result.BlockTime, 0).UTC(), nil
}

func (r *CreationTimeResolver) call(ctx context.Context, body any, out any) error {
	buf, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rpc rate limited (429)")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("rpc http status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}