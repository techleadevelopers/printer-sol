package strategy

import "testing"

func TestNewTokenIsObservationOnly(t *testing.T) {
	s := &Strategy{}
	ok, reason := s.Evaluate(nil, "")
	if ok || reason != "missing mint" {
		t.Fatal("empty mint must be rejected")
	}
}
