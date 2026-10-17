package wallet

import (
	"context"
	"fmt"
	"github.com/gagliardetto/solana-go"
	"math"
)

const lamportsPerSOL = 1_000_000_000

type Wallet struct {
	key        solana.PrivateKey
	public     solana.PublicKey
	simulation bool
	feeReserve uint64
}

func Load(secret string, simulation bool, reserve float64) (*Wallet, error) {
	if reserve < 0 {
		return nil, fmt.Errorf("negative fee reserve")
	}
	if secret == "" {
		if simulation {
			return &Wallet{simulation: true, feeReserve: uint64(reserve * lamportsPerSOL)}, nil
		}
		return nil, fmt.Errorf("BOT_PRIVATE_KEY is required when SIMULATION=false")
	}
	k, e := solana.PrivateKeyFromBase58(secret)
	if e != nil {
		return nil, fmt.Errorf("invalid BOT_PRIVATE_KEY: %w", e)
	}
	return &Wallet{key: k, public: k.PublicKey(), simulation: simulation, feeReserve: uint64(reserve * lamportsPerSOL)}, nil
}
func (w *Wallet) Address() string {
	if len(w.key) == 0 {
		return ""
	}
	return w.public.String()
}
func (w *Wallet) PublicKey() solana.PublicKey               { return w.public }
func (w *Wallet) CanAfford(balance, spend, fee uint64) bool { return balance >= spend+fee+w.feeReserve }
func (w *Wallet) Sign(tx *solana.Transaction) error {
	if w.simulation {
		return fmt.Errorf("signing prohibited in simulation")
	}
	if len(w.key) == 0 {
		return fmt.Errorf("wallet unavailable")
	}
	_, e := tx.Sign(func(k solana.PublicKey) *solana.PrivateKey {
		if k.Equals(w.public) {
			return &w.key
		}
		return nil
	})
	return e
}
func (w *Wallet) Balance(ctx context.Context, get func(context.Context, string) (uint64, error)) (uint64, error) {
	if w.Address() == "" {
		return 0, fmt.Errorf("wallet unavailable")
	}
	return get(ctx, w.Address())
}
func SOLToLamports(v float64) (uint64, error) {
	if v < 0 || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("invalid SOL amount")
	}
	return uint64(math.Round(v * lamportsPerSOL)), nil
}
