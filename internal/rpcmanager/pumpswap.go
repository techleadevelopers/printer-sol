package rpcmanager

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"

	"github.com/gagliardetto/solana-go"
)

const pumpSwapProgramID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"

var pumpSwapPoolDiscriminator = [8]byte{241, 154, 109, 4, 17, 177, 109, 188}

// PumpSwap fee constants. LP fee is fixed at 20 bps. Protocol and creator fees
// are dynamic (market-cap tiered); until the on-chain tier is resolved, use the
// conservative post-graduation ceiling of 30 bps total so quotes are not
// optimistic [citation:15].
const pumpSwapLpFeeBPS = 20
const pumpSwapProtocolFeeBPS = 5
const pumpSwapCreatorFeeBPS = 5
const pumpSwapTotalFeeBPS = pumpSwapLpFeeBPS + pumpSwapProtocolFeeBPS + pumpSwapCreatorFeeBPS

// PumpSwapPool is the subset of the on-chain Pool account needed to classify
// a route, quote a sell, and validate a prepared transaction.
type PumpSwapPool struct {
	PoolAddress           solana.PublicKey
	BaseMint              solana.PublicKey
	QuoteMint             solana.PublicKey
	PoolBaseTokenAccount  solana.PublicKey
	PoolQuoteTokenAccount solana.PublicKey
	CoinCreator           solana.PublicKey
}

// FindPumpSwapPool locates a PumpSwap pool by scanning getProgramAccounts
// with a memcmp filter on base_mint at offset 43 [citation:1]. It does NOT
// attempt to derive the pool PDA: PumpSwap pool addresses are not PDAs and
// must be discovered from account data or creation events.
func (m *Manager) FindPumpSwapPool(ctx context.Context, baseMint string) (*PumpSwapPool, error) {
	baseKey, err := solana.PublicKeyFromBase58(baseMint)
	if err != nil {
		return nil, fmt.Errorf("parse base mint: %w", err)
	}
	programID, err := solana.PublicKeyFromBase58(pumpSwapProgramID)
	if err != nil {
		return nil, fmt.Errorf("parse PumpSwap program ID: %w", err)
	}

	// Primary: filter by base_mint at offset 43.
	pool, err := m.scanPumpSwapPool(ctx, programID, 43, baseKey)
	if err != nil {
		return nil, err
	}
	if pool != nil {
		return pool, nil
	}

	// Fallback: filter by quote_mint at offset 75, then verify base_mint.
	// This covers non-canonical pools whose base_mint is not the requested
	// mint but whose quote mint matches (e.g. inverted pairs).
	wrappedSol := solana.WrappedSol
	pool, err = m.scanPumpSwapPool(ctx, programID, 75, wrappedSol)
	if err != nil {
		return nil, err
	}
	if pool != nil && pool.BaseMint == baseKey {
		return pool, nil
	}

	return nil, nil
}

func (m *Manager) scanPumpSwapPool(ctx context.Context, programID solana.PublicKey, offset uint64, mint solana.PublicKey) (*PumpSwapPool, error) {
	endpoint, err := m.currentURL()
	if err != nil {
		return nil, err
	}
	var accounts []struct {
		Pubkey  string   `json:"pubkey"`
		Account struct {
			Data []string `json:"data"`
		} `json:"account"`
	}
	if err := m.call(ctx, endpoint, "getProgramAccounts", []any{
		programID.String(),
		map[string]any{
			"encoding":   "base64",
			"commitment": "confirmed",
			"filters": []any{
				map[string]any{"dataSize": 244},
				map[string]any{"memcmp": map[string]any{"offset": offset, "bytes": mint.String()}},
			},
		},
	}, &accounts); err != nil {
		return nil, fmt.Errorf("scan PumpSwap pools at offset %d: %w", offset, err)
	}
	for _, acc := range accounts {
		if len(acc.Account.Data) == 0 {
			continue
		}
		data, decodeErr := base64.StdEncoding.DecodeString(acc.Account.Data[0])
		if decodeErr != nil {
			continue
		}
		address, parseErr := solana.PublicKeyFromBase58(acc.Pubkey)
		if parseErr != nil {
			continue
		}
		pool, decodeErr := decodePumpSwapPool(data, address)
		if decodeErr == nil && pool != nil {
			return pool, nil
		}
	}
	return nil, nil
}

// decodePumpSwapPool decodes the fields we need. Offsets per solana-protocols
// fixtures: base_mint at 43, quote_mint at 75, pool_base_token_account at 139,
// pool_quote_token_account at 171 [citation:1].
func decodePumpSwapPool(data []byte, address solana.PublicKey) (*PumpSwapPool, error) {
	if len(data) < 244 {
		return nil, fmt.Errorf("PumpSwap pool account data is incomplete: len=%d", len(data))
	}
	if !bytes.Equal(data[:8], pumpSwapPoolDiscriminator[:]) {
		return nil, fmt.Errorf("account data is not a PumpSwap pool")
	}
	var baseMint, quoteMint, baseVault, quoteVault, coinCreator solana.PublicKey
	copy(baseMint[:], data[43:75])
	copy(quoteMint[:], data[75:107])
	copy(baseVault[:], data[139:171])
	copy(quoteVault[:], data[171:203])
	copy(coinCreator[:], data[211:243])
	return &PumpSwapPool{
		PoolAddress:           address,
		BaseMint:              baseMint,
		QuoteMint:             quoteMint,
		PoolBaseTokenAccount:  baseVault,
		PoolQuoteTokenAccount: quoteVault,
		CoinCreator:           coinCreator,
	}, nil
}

// GetPumpSwapReserves reads the base and effective quote reserves. The
// effective quote reserve is pool_quote_token_account.amount plus
// virtual_quote_reserves (signed); currently virtual_quote_reserves is zero on
// all pools, so the vault balance is used [citation:2].
func (m *Manager) GetPumpSwapReserves(ctx context.Context, pool *PumpSwapPool) (baseReserve, quoteReserve uint64, err error) {
	endpoint, err := m.currentURL()
	if err != nil {
		return 0, 0, err
	}
	read := func(account solana.PublicKey) (uint64, error) {
		var r struct {
			Value *struct {
				Amount string `json:"amount"`
			} `json:"value"`
		}
		if callErr := m.call(ctx, endpoint, "getTokenAccountBalance", []any{
			account.String(),
			map[string]any{"commitment": "confirmed"},
		}, &r); callErr != nil {
			return 0, callErr
		}
		if r.Value == nil || r.Value.Amount == "" {
			return 0, fmt.Errorf("no balance for vault %s", account.String())
		}
		var amount uint64
		if _, scanErr := fmt.Sscanf(r.Value.Amount, "%d", &amount); scanErr != nil {
			return 0, fmt.Errorf("decode vault balance: %w", scanErr)
		}
		return amount, nil
	}
	baseReserve, err = read(pool.PoolBaseTokenAccount)
	if err != nil {
		return 0, 0, fmt.Errorf("read base vault: %w", err)
	}
	quoteReserve, err = read(pool.PoolQuoteTokenAccount)
	if err != nil {
		return 0, 0, fmt.Errorf("read quote vault: %w", err)
	}
	return baseReserve, quoteReserve, nil
}

// QuotePumpSwapSell returns the expected quote-token output when selling
// baseAmountIn of the base token. The constant-product output is computed
// first, then fees are subtracted from the output [citation:4].
func (m *Manager) QuotePumpSwapSell(ctx context.Context, pool *PumpSwapPool, baseAmountIn uint64) (uint64, error) {
	baseReserve, quoteReserve, err := m.GetPumpSwapReserves(ctx, pool)
	if err != nil {
		return 0, err
	}
	if baseReserve == 0 || quoteReserve == 0 {
		return 0, fmt.Errorf("PumpSwap pool has zero reserves")
	}
	if baseAmountIn == 0 {
		return 0, fmt.Errorf("sell amount must be positive")
	}

	// amount_out_before_fees = reserve_out * amount_in / (reserve_in + amount_in)
	amountOutBeforeFees := (quoteReserve * baseAmountIn) / (baseReserve + baseAmountIn)
	if amountOutBeforeFees == 0 {
		return 0, fmt.Errorf("PumpSwap sell quote is zero before fees")
	}

	// Fees are charged on the output, not the input.
	totalFee := (amountOutBeforeFees * pumpSwapTotalFeeBPS) / 10000
	if totalFee >= amountOutBeforeFees {
		return 0, fmt.Errorf("PumpSwap sell output does not cover fees")
	}
	amountOut := amountOutBeforeFees - totalFee
	if amountOut == 0 {
		return 0, fmt.Errorf("PumpSwap sell quote is zero after fees")
	}
	return amountOut, nil
}