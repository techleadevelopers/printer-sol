package strategy

import (
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
