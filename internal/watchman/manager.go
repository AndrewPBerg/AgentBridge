package watchman

import (
	"context"
	"os/exec"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type engine interface {
	coordination
	Sessions(bool) []protocol.Actor
}

// Manager maintains one Watchman subscription per active Agent Bridge
// workspace. Watchman is only a wake-up source; Engine remains authoritative.
type Manager struct {
	engine engine
	binary string

	mu       sync.Mutex
	watching map[string]context.CancelFunc
}

// New constructs a Watchman workspace manager.
func New(target engine) *Manager {
	binary, err := exec.LookPath("watchman")
	if err != nil {
		binary = ""
	}
	return &Manager{engine: target, binary: binary, watching: make(map[string]context.CancelFunc)}
}

// Available reports whether the Watchman executable was found.
func (m *Manager) Available() bool { return m.binary != "" }

// Run discovers active workspaces and maintains their subscriptions until cancellation.
func (m *Manager) Run(ctx context.Context) {
	if m.binary == "" {
		return
	}
	m.discover(ctx)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.stopAll()
			return
		case <-ticker.C:
			m.discover(ctx)
		}
	}
}

func (m *Manager) discover(ctx context.Context) {
	actors := m.engine.Sessions(false)
	for index := range actors {
		actor := actors[index]
		if actor.WorkspaceUUID == "" || actor.ActorKind == protocol.ActorKindUnknown || !actor.Addressable || actor.Git == nil && actor.JJ == nil {
			continue
		}
		m.mu.Lock()
		if _, exists := m.watching[actor.WorkspaceUUID]; exists {
			m.mu.Unlock()
			continue
		}
		workspaceCtx, cancel := context.WithCancel(ctx)
		m.watching[actor.WorkspaceUUID] = cancel
		m.mu.Unlock()
		watcher := &workspaceWatcher{binary: m.binary, actor: actor, processor: newProcessor(m.engine, &actor)}
		go watcher.run(workspaceCtx)
	}
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for uuid, cancel := range m.watching {
		cancel()
		delete(m.watching, uuid)
	}
}
