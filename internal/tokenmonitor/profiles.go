package tokenmonitor

import (
	"fmt"
	"time"
)

// EntryProfile identifica qual estratégia autorizou a entrada.
type EntryProfile string

const (
	ProfileFirstPump EntryProfile = "first_pump"
	ProfileMomentum  EntryProfile = "momentum"
)

// EntryProfileMode controla como o bot escolhe o perfil.
type EntryProfileMode string

const (
	ModeAuto          EntryProfileMode = "auto"
	ModeFirstPumpOnly EntryProfileMode = "first_pump_only"
	ModeMomentumOnly  EntryProfileMode = "momentum_only"
	ModeBoth          EntryProfileMode = "both"
)

// ProfileConfig define os critérios de um perfil.
type ProfileConfig struct {
	Enabled bool

	MaxTokenAge time.Duration

	MinMarketCapSOL float64
	MaxMarketCapSOL float64

	MinVolumeBeforeBuySOL float64
	MaxVolumeBeforeBuySOL float64

	// Só Perfil B.
	MinHolders      int
	RequireMomentum bool
	Momentum        MomentumConfig

	// Pesos do score (0 = score não calculado para este perfil).
	ScoreWeightVolume   float64
	ScoreWeightBuyRatio float64
	ScoreWeightBuyers   float64
	ScoreWeightAccel    float64
	ScoreVolumeCapSOL   float64
	ScoreBuyersCap      float64
}

// Profiles agrega os dois perfis e o modo de seleção.
type Profiles struct {
	Mode      EntryProfileMode
	FirstPump ProfileConfig
	Momentum  ProfileConfig
}

// DefaultProfiles devolve perfis desligados, sem filtros adicionais.
func DefaultProfiles() Profiles {
	return Profiles{
		Mode: ModeAuto,
		FirstPump: ProfileConfig{
			Enabled:               false,
			MaxTokenAge:           30 * time.Second,
			MinMarketCapSOL:       0,
			MaxMarketCapSOL:       4,
			MinVolumeBeforeBuySOL: 0,
			MaxVolumeBeforeBuySOL: 1.5,
		},
		Momentum: ProfileConfig{
			Enabled:               false,
			MaxTokenAge:           0,
			MinMarketCapSOL:       0,
			MaxMarketCapSOL:       0,
			MinVolumeBeforeBuySOL: 0,
			MaxVolumeBeforeBuySOL: 0,
			MinHolders:            0,
			RequireMomentum:       true,
			Momentum:              DefaultMomentumConfig(),
		},
	}
}

// ProfileContext reúne os dados de um candidato para a decisão de perfil.
type ProfileContext struct {
	Mint               string
	TokenAge           time.Duration
	MarketCapSOL       float64
	RealSOLReservesSOL float64
	HolderCount        int
	HolderCountKnown   bool
	Momentum           MomentumSnapshot
	MomentumEvaluated  bool
}

// Decision é o resultado da avaliação de um perfil.
type Decision struct {
	Profile  EntryProfile
	Approved bool
	Reason   string
	Score    float64
}

// computeProfileScore devolve um score 0–100 a partir dos indicadores
// disponíveis. Só deve ser chamada depois de todos os filtros obrigatórios
// terem passado.
func computeProfileScore(cfg ProfileConfig, ctx ProfileContext) float64 {
	var total, weight float64

	if cfg.ScoreWeightVolume > 0 && cfg.ScoreVolumeCapSOL > 0 {
		vol := ctx.Momentum.VolumeLong
		if vol > cfg.ScoreVolumeCapSOL {
			vol = cfg.ScoreVolumeCapSOL
		}
		total += cfg.ScoreWeightVolume * (vol / cfg.ScoreVolumeCapSOL) * 100
		weight += cfg.ScoreWeightVolume
	}
	if cfg.ScoreWeightBuyRatio > 0 {
		total += cfg.ScoreWeightBuyRatio * ctx.Momentum.BuyRatio * 100
		weight += cfg.ScoreWeightBuyRatio
	}
	if cfg.ScoreWeightBuyers > 0 && cfg.ScoreBuyersCap > 0 {
		buyers := float64(ctx.Momentum.UniqueBuyers)
		if buyers > cfg.ScoreBuyersCap {
			buyers = cfg.ScoreBuyersCap
		}
		total += cfg.ScoreWeightBuyers * (buyers / cfg.ScoreBuyersCap) * 100
		weight += cfg.ScoreWeightBuyers
	}
	if cfg.ScoreWeightAccel > 0 {
		accel := ctx.Momentum.Acceleration
		if accel < 0 {
			accel = 0
		}
		if accel > 1 {
			accel = 1
		}
		total += cfg.ScoreWeightAccel * accel * 100
		weight += cfg.ScoreWeightAccel
	}
	if weight == 0 {
		return 0
	}
	return total / weight
}

// evaluateFirstPump aplica os critérios do Perfil A.
func evaluateFirstPump(cfg ProfileConfig, ctx ProfileContext) Decision {
	if !cfg.Enabled {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: "first-pump profile disabled"}
	}
	if cfg.MaxTokenAge > 0 && ctx.TokenAge > cfg.MaxTokenAge {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: fmt.Sprintf("token age %s exceeds first-pump maximum %s", ctx.TokenAge.Truncate(time.Second), cfg.MaxTokenAge)}
	}
	if ctx.MarketCapSOL <= 0 {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: "market cap is not positive"}
	}
	if ctx.MarketCapSOL < cfg.MinMarketCapSOL {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: fmt.Sprintf("market cap %.6f SOL is below first-pump minimum %.6f SOL", ctx.MarketCapSOL, cfg.MinMarketCapSOL)}
	}
	if cfg.MaxMarketCapSOL > 0 && ctx.MarketCapSOL > cfg.MaxMarketCapSOL {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: fmt.Sprintf("market cap %.6f SOL exceeds first-pump maximum %.6f SOL", ctx.MarketCapSOL, cfg.MaxMarketCapSOL)}
	}
	if ctx.RealSOLReservesSOL < cfg.MinVolumeBeforeBuySOL {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: fmt.Sprintf("real SOL reserves %.6f SOL are below first-pump minimum %.6f SOL", ctx.RealSOLReservesSOL, cfg.MinVolumeBeforeBuySOL)}
	}
	if ctx.RealSOLReservesSOL > cfg.MaxVolumeBeforeBuySOL {
		return Decision{Profile: ProfileFirstPump, Approved: false, Reason: fmt.Sprintf("real SOL reserves %.6f SOL exceed first-pump maximum %.6f SOL", ctx.RealSOLReservesSOL, cfg.MaxVolumeBeforeBuySOL)}
	}
	score := computeProfileScore(cfg, ctx)
	return Decision{Profile: ProfileFirstPump, Approved: true, Reason: "first-pump criteria satisfied", Score: score}
}

// evaluateMomentum aplica os critérios do Perfil B.
func evaluateMomentum(cfg ProfileConfig, ctx ProfileContext) Decision {
	if !cfg.Enabled {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: "momentum profile disabled"}
	}
	if cfg.MaxTokenAge > 0 && ctx.TokenAge > cfg.MaxTokenAge {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("token age %s exceeds momentum maximum %s", ctx.TokenAge.Truncate(time.Second), cfg.MaxTokenAge)}
	}
	if ctx.MarketCapSOL <= 0 {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: "market cap is not positive"}
	}
	if ctx.MarketCapSOL < cfg.MinMarketCapSOL {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("market cap %.6f SOL is below momentum minimum %.6f SOL", ctx.MarketCapSOL, cfg.MinMarketCapSOL)}
	}
	if cfg.MaxMarketCapSOL > 0 && ctx.MarketCapSOL > cfg.MaxMarketCapSOL {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("market cap %.6f SOL exceeds momentum maximum %.6f SOL", ctx.MarketCapSOL, cfg.MaxMarketCapSOL)}
	}
	if cfg.MinVolumeBeforeBuySOL > 0 && ctx.RealSOLReservesSOL < cfg.MinVolumeBeforeBuySOL {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("real SOL reserves %.6f SOL are below momentum minimum %.6f SOL", ctx.RealSOLReservesSOL, cfg.MinVolumeBeforeBuySOL)}
	}
	if cfg.MaxVolumeBeforeBuySOL > 0 && ctx.RealSOLReservesSOL > cfg.MaxVolumeBeforeBuySOL {
		return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("real SOL reserves %.6f SOL exceed momentum maximum %.6f SOL", ctx.RealSOLReservesSOL, cfg.MaxVolumeBeforeBuySOL)}
	}
	if cfg.MinHolders > 0 {
		if !ctx.HolderCountKnown {
			return Decision{Profile: ProfileMomentum, Approved: false, Reason: "holder count is unavailable but momentum profile requires it"}
		}
		if ctx.HolderCount < cfg.MinHolders {
			return Decision{Profile: ProfileMomentum, Approved: false, Reason: fmt.Sprintf("holder count %d is below momentum minimum %d", ctx.HolderCount, cfg.MinHolders)}
		}
	}
	if cfg.RequireMomentum {
		if !ctx.MomentumEvaluated {
			return Decision{Profile: ProfileMomentum, Approved: false, Reason: "momentum gate was not evaluated"}
		}
		if !ctx.Momentum.Sufficient {
			return Decision{Profile: ProfileMomentum, Approved: false, Reason: "momentum gate: " + ctx.Momentum.RejectReason}
		}
	}
	score := computeProfileScore(cfg, ctx)
	return Decision{Profile: ProfileMomentum, Approved: true, Reason: "momentum criteria satisfied", Score: score}
}

// SelectProfile decide qual perfil autoriza a entrada, conforme o modo.
func SelectProfile(profiles Profiles, ctx ProfileContext) Decision {
	switch profiles.Mode {
	case ModeFirstPumpOnly:
		return evaluateFirstPump(profiles.FirstPump, ctx)
	case ModeMomentumOnly:
		return evaluateMomentum(profiles.Momentum, ctx)
	case ModeBoth:
		a := evaluateFirstPump(profiles.FirstPump, ctx)
		if a.Approved {
			return a
		}
		b := evaluateMomentum(profiles.Momentum, ctx)
		if b.Approved {
			return b
		}
		return Decision{Approved: false, Reason: "first-pump: " + a.Reason + "; momentum: " + b.Reason}
	default: // ModeAuto
		if profiles.FirstPump.Enabled && ctx.TokenAge <= profiles.FirstPump.MaxTokenAge {
			return evaluateFirstPump(profiles.FirstPump, ctx)
		}
		return evaluateMomentum(profiles.Momentum, ctx)
	}
}