package risk

import "fmt"

type Limits struct{ MaxExposureSOL, MaxDailyLossSOL float64 }

func Validate(amount, openExposure, realizedDailyLoss float64, l Limits) error {
	if amount <= 0 {
		return fmt.Errorf("amount must be positive")
	}
	if l.MaxExposureSOL > 0 && openExposure+amount > l.MaxExposureSOL {
		return fmt.Errorf("exposure limit exceeded")
	}
	if l.MaxDailyLossSOL > 0 && realizedDailyLoss >= l.MaxDailyLossSOL {
		return fmt.Errorf("daily loss limit reached")
	}
	return nil
}
