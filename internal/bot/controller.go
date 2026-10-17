package bot

import (
	"solana-sniper-backend/internal/events"
	"sync"
)

type State string

const (
	Stopped          State = "stopped"
	Running          State = "running"
	Paused           State = "paused"
	EmergencyStopped State = "emergency_stopped"
)

type Controller struct {
	mu    sync.RWMutex
	state State
	bus   *events.Bus
}

func New(bus *events.Bus) *Controller {
	if bus == nil {
		bus = events.New()
	}
	return &Controller{state: Stopped, bus: bus}
}
func (c *Controller) State() State { c.mu.RLock(); defer c.mu.RUnlock(); return c.state }
func (c *Controller) set(s State) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
	c.bus.Publish("bot_state_changed", map[string]string{"state": string(s)})
}
func (c *Controller) Start()  { c.set(Running) }
func (c *Controller) Stop()   { c.set(Stopped) } // Stop only disables entries; it never liquidates positions.
func (c *Controller) Pause()  { c.set(Paused); c.bus.Publish("bot_paused", nil) }
func (c *Controller) Resume() { c.set(Running) }
func (c *Controller) EmergencyStop() {
	c.set(EmergencyStopped)
	c.bus.Publish("risk_alert", map[string]string{"reason": "emergency stop: entries disabled; positions retained"})
}
func (c *Controller) AllowsEntries() bool { return c.State() == Running }
