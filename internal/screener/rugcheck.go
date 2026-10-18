package screener

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// RugCheck fails closed: an unavailable report, high score, unlocked LP, or a
// danger/critical flag rejects the entry before it reaches the signing path.
func RugCheck(ctx context.Context, mint string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.rugcheck.xyz/v1/tokens/"+mint+"/report/summary", nil)
	if err != nil {
		return "", err
	}
	res, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return "", fmt.Errorf("risk screen unavailable: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("risk screen HTTP %d", res.StatusCode)
	}
	var report struct {
		Score    float64                        `json:"score_normalised"`
		LPLocked float64                        `json:"lpLockedPct"`
		Risks    []struct{ Name, Level string } `json:"risks"`
	}
	if err = json.NewDecoder(http.MaxBytesReader(nil, res.Body, 64<<10)).Decode(&report); err != nil {
		return "", err
	}
	if report.Score > 20 || report.LPLocked < 50 {
		return "", fmt.Errorf("risk score %.0f or locked liquidity %.1f%% rejected", report.Score, report.LPLocked)
	}
	for _, risk := range report.Risks {
		if strings.EqualFold(risk.Level, "danger") || strings.EqualFold(risk.Level, "critical") {
			return "", fmt.Errorf("risk screen flagged %s", risk.Name)
		}
	}
	return fmt.Sprintf("RugCheck passed: score %.0f, LP locked %.1f%%", report.Score, report.LPLocked), nil
}
