package tokenmonitor

import (
	"encoding/base64"
	"strings"

	"github.com/gagliardetto/solana-go"
)

// pumpFunProgramIDForDiscovery é o Program ID do Pump.fun usado na subscrição
// logsSubscribe quando PUMPPORTAL_DETECTION=false.
const pumpFunProgramIDForDiscovery = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"

// pumpFunCreateEventDiscriminator é o discriminador Anchor do evento
// CreateEvent do Pump.fun: [27, 234, 178, 133, 56, 102, 68, 241].
var pumpFunCreateEventDiscriminator = [8]byte{27, 234, 178, 133, 56, 102, 68, 241}

// extractMintFromPumpFunLogs procura o mint numa das linhas de log do Pump.fun.
//
// O evento Anchor "CreateEvent" do Pump.fun tem o layout:
//
//	discriminator (8) | name(String) | symbol(String) | uri(String) |
//	mint (32) | bonding_curve (32) | user (32) | ...
//
// Onde String = u32 LE length + bytes UTF-8.
//
// Os logs do programa costumam vir como "Program data: <base64>".
func extractMintFromPumpFunLogs(logs []string) (string, bool) {
	const prefix = "Program data: "
	for _, line := range logs {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		if raw == "" {
			continue
		}
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil || len(decoded) < 8+3*4+32 {
			continue
		}
		if !equal8(decoded[:8], pumpFunCreateEventDiscriminator[:]) {
			continue
		}
		offset := 8
		ok := true
		for i := 0; i < 3; i++ {
			if len(decoded) < offset+4 {
				ok = false
				break
			}
			strLen := int(uint32(decoded[offset]) |
				uint32(decoded[offset+1])<<8 |
				uint32(decoded[offset+2])<<16 |
				uint32(decoded[offset+3])<<24)
			offset += 4 + strLen
		}
		if !ok || len(decoded) < offset+32 {
			continue
		}
		mint := solana.PublicKeyFromBytes(decoded[offset : offset+32]).String()
		return mint, true
	}
	return "", false
}

func equal8(a, b []byte) bool {
	if len(a) != 8 || len(b) != 8 {
		return false
	}
	for i := 0; i < 8; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}