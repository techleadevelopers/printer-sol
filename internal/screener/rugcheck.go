package screener

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
		Score    *float64                       `json:"score_normalised"`
		LPLocked *float64                       `json:"lpLockedPct"`
		Risks    []struct{ Name, Level string } `json:"risks"`
	}
	if err = json.NewDecoder(http.MaxBytesReader(nil, res.Body, 64<<10)).Decode(&report); err != nil {
		return "", err
	}
	if report.Score == nil || report.LPLocked == nil {
		return "", fmt.Errorf("risk screen returned incomplete score or liquidity data")
	}
	score := *report.Score
	if score > 1 && score <= 100 {
		score /= 100
	}
	lpLocked := *report.LPLocked
	if math.IsNaN(score) || math.IsInf(score, 0) || score < 0 || score > 1 ||
		math.IsNaN(lpLocked) || math.IsInf(lpLocked, 0) || lpLocked < 0 || lpLocked > 100 {
		return "", fmt.Errorf("risk screen returned invalid score or liquidity data")
	}
	if score > .20 || lpLocked < 50 {
		return "", fmt.Errorf("normalized risk score %.3f or locked liquidity %.1f%% rejected", score, lpLocked)
	}
	for _, risk := range report.Risks {
		if strings.EqualFold(risk.Level, "danger") || strings.EqualFold(risk.Level, "critical") {
			return "", fmt.Errorf("risk screen flagged %s", risk.Name)
		}
	}
	return fmt.Sprintf("RugCheck passed: normalized score %.3f, LP locked %.1f%%", score, lpLocked), nil
}
