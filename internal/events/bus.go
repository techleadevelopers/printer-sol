// Package events broadcasts non-secret operational events to panel clients.
package events

import (
	"sync"
	"time"
)

type Event struct {
	Type string    `json:"type"`
	Data any       `json:"data,omitempty"`
	At   time.Time `json:"at"`
}
type Bus struct {
	mu   sync.RWMutex
	subs map[chan Event]struct{}
}

func New() *Bus { return &Bus{subs: make(map[chan Event]struct{})} }
func (b *Bus) Subscribe() (chan Event, func()) {
	c := make(chan Event, 32)
	b.mu.Lock()
	b.subs[c] = struct{}{}
	b.mu.Unlock()
	return c, func() { b.mu.Lock(); delete(b.subs, c); close(c); b.mu.Unlock() }
}
func (b *Bus) Publish(kind string, data any) {
	e := Event{Type: kind, Data: data, At: time.Now().UTC()}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for c := range b.subs {
		select {
		case c <- e:
		default:
		}
	}
}
