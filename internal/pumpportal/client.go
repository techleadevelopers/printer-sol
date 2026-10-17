package pumpportal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Request struct {
	PublicKey        string  `json:"publicKey"`
	Action           string  `json:"action"`
	Mint             string  `json:"mint"`
	Amount           any     `json:"amount"`
	DenominatedInSOL string  `json:"denominatedInSol"`
	Slippage         float64 `json:"slippage"`
	PriorityFee      float64 `json:"priorityFee"`
	Pool             string  `json:"pool,omitempty"`
}
type Client struct {
	url  string
	http *http.Client
}

func New(url string) *Client { return &Client{url: url, http: &http.Client{Timeout: 12 * time.Second}} }
func (c *Client) Prepare(ctx context.Context, r Request) ([]byte, error) {
	b, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	q, e := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(b))
	if e != nil {
		return nil, e
	}
	q.Header.Set("Content-Type", "application/json")
	res, e := c.http.Do(q)
	if e != nil {
		return nil, e
	}
	defer res.Body.Close()
	out, e := io.ReadAll(io.LimitReader(res.Body, 8192))
	if e != nil {
		return nil, e
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("PumpPortal preparation HTTP %d: %s", res.StatusCode, string(out))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty serialized transaction")
	}
	return out, nil
}
