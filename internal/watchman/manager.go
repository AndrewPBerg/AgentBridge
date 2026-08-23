package watchman

import (
	"context"
	"os/exec"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

const idleGracePeriod = 30 * time.Second

type engine interface {
	coordination
	Sessions(bool) []protocol.Actor
}

// Manager maintains one Watchman subscription per active Agent Bridge
// workspace. Watchman is only a wake-up source; Engine remains authoritative.
type Manager struct {
	engine engine
	binary string

	mu         sync.Mutex
	watching   map[string]watchState
	now        func() time.Time
	runWatcher func(context.Context, protocol.Actor)
}

type watchState struct {
	cancel    context.CancelFunc
	idleSince time.Time
	// stopping remains in the map until done is closed by the watcher. This
	// prevents a reactivation from starting a second subscription for the
	// same workspace while the old watcher is still unwinding.
	stopping bool
	done     chan struct{}
}

// New constructs a Watchman workspace manager.
func New(target engine) *Manager {
	binary, err := exec.LookPath("watchman")
	if err != nil {
		binary = ""
	}
	return &Manager{engine: target, binary: binary, watching: make(map[string]watchState), now: time.Now}
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
	active := make(map[string]protocol.Actor)
	for index := range actors {
		actor := actors[index]
		if actor.WorkspaceUUID == "" || actor.ActorKind == protocol.ActorKindUnknown || !actor.Addressable || actor.Git == nil && actor.JJ == nil {
			continue
		}
		if _, exists := active[actor.WorkspaceUUID]; !exists {
			active[actor.WorkspaceUUID] = actor
		}
	}
	now := m.now()
	m.mu.Lock()
	m.retireIdleLocked(now, active)
	for workspace := range active {
		actor := active[workspace]
		if _, exists := m.watching[workspace]; exists {
			// A stopping watcher owns this workspace until it acknowledges
			// completion. In particular, do not reset its state or start a
			// replacement here.
			continue
		}
		workspaceCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		m.watching[workspace] = watchState{cancel: cancel, done: done}
		runWatcher := m.runWatcher
		if runWatcher == nil {
			watcher := &workspaceWatcher{binary: m.binary, actor: actor, processor: newProcessor(m.engine, &actor)}
			runWatcher = func(ctx context.Context, _ protocol.Actor) { watcher.run(ctx) }
		}
		go func() {
			runWatcher(workspaceCtx, actor)
			m.watcherStopped(workspace, done)
		}()
	}
	m.mu.Unlock()
}

// retireIdleLocked keeps a workspace watch alive briefly after the last
// addressable lease disappears. Synthetic and stale actors are excluded by the
// active set supplied by discover.
func (m *Manager) retireIdleLocked(now time.Time, active map[string]protocol.Actor) {
	for workspace, state := range m.watching {
		if state.stopping {
			// Completion owns removal, including when the actor has already
			// returned. Never cancel or recycle this state a second time.
			continue
		}
		if _, ok := active[workspace]; ok {
			state.idleSince = time.Time{}
			m.watching[workspace] = state
			continue
		}
		if state.idleSince.IsZero() {
			state.idleSince = now
			m.watching[workspace] = state
			continue
		}
		if now.Sub(state.idleSince) < idleGracePeriod {
			continue
		}
		state.stopping = true
		state.cancel()
		m.watching[workspace] = state
	}
}

// watcherStopped is the watcher goroutine's completion acknowledgment. The
// entry is removed only after the goroutine has returned, so a subsequent
// discover cannot overlap subscriptions for one workspace.
func (m *Manager) watcherStopped(workspace string, done chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state, ok := m.watching[workspace]
	if !ok || state.done != done {
		return
	}
	close(done)
	delete(m.watching, workspace)
}

func (m *Manager) stopAll() {
	m.mu.Lock()
	states := make([]watchState, 0, len(m.watching))
	for workspace, state := range m.watching {
		state.stopping = true
		m.watching[workspace] = state
		state.cancel()
		states = append(states, state)
	}
	m.mu.Unlock()

	// Run owns the watcher goroutines; wait for every cancellation to be
	// acknowledged before returning, avoiding leaked subscriptions on shutdown.
	for _, state := range states {
		if state.done != nil {
			<-state.done
		}
	}
}
