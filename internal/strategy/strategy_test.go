package strategy

import (
	"context"
	"testing"
	"time"
)

func TestNewTokenIsObservationOnly(t *testing.T) {
	s := &Strategy{}
	ok, reason := s.Evaluate(nil, "", time.Now())
	if ok || reason != "missing mint" {
		t.Fatal("empty mint must be rejected")
	}
}

func TestEvaluateRejectsMissingStaleAndFutureCreationTimes(t *testing.T) {
	s := New(nil, 5*time.Minute)
	tests := []struct {
		name      string
		createdAt time.Time
		reason    string
	}{
		{name: "missing", reason: "missing token creation timestamp"},
		{name: "older than configured maximum", createdAt: time.Now().Add(-5*time.Minute - time.Second), reason: "token creation exceeded maximum entry age"},
		{name: "implausibly in the future", createdAt: time.Now().Add(time.Minute), reason: "token creation timestamp is in the future"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, reason := s.Evaluate(context.Background(), "mint", test.createdAt)
			if ok || reason != test.reason {
				t.Fatalf("Evaluate() = %v, %q; want false, %q", ok, reason, test.reason)
			}
		})
	}
}
