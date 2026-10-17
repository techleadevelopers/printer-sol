package wallet

import "testing"

func TestCanAffordReservesFees(t *testing.T) {
	w := &Wallet{feeReserve: 10}
	if w.CanAfford(109, 90, 10) {
		t.Fatal("must reserve fee buffer")
	}
	if !w.CanAfford(110, 90, 10) {
		t.Fatal("exact funds should pass")
	}
}
func TestSOLToLamports(t *testing.T) {
	v, e := SOLToLamports(.001)
	if e != nil || v != 1_000_000 {
		t.Fatalf("got %d, %v", v, e)
	}
}
