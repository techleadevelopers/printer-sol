package risk

import "testing"

func TestValidateRiskLimits(t *testing.T) {
	l := Limits{MaxExposureSOL: 1, MaxDailyLossSOL: .25}
	if err := Validate(.2, .9, 0, l); err == nil {
		t.Fatal("expected exposure rejection")
	}
	if err := Validate(.1, 0, .25, l); err == nil {
		t.Fatal("expected daily loss rejection")
	}
	if err := Validate(.1, .2, 0, l); err != nil {
		t.Fatalf("unexpected rejection: %v", err)
	}
}
