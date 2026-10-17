package events

import (
	"testing"
	"time"
)

func TestBusDeliversConsistentEvent(t *testing.T) {
	b := New()
	c, unsubscribe := b.Subscribe()
	defer unsubscribe()
	b.Publish("token_detected", map[string]string{"mint": "mint"})
	select {
	case e := <-c:
		if e.Type != "token_detected" || e.At.IsZero() {
			t.Fatalf("unexpected event: %#v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("event was not delivered")
	}
}
