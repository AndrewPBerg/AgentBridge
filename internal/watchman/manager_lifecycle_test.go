package watchman

import (
	"context"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestRetireIdleKeepsGraceAndRetiresAddressableWorkspace(t *testing.T) {
	manager := &Manager{watching: make(map[string]watchState)}
	canceled := false
	done := make(chan struct{})
	manager.watching["workspace"] = watchState{cancel: func() { canceled = true }, done: done}
	base := time.Unix(100, 0)
	manager.retireIdleLocked(base, map[string]protocol.Actor{})
	if canceled || manager.watching["workspace"].idleSince != base {
		t.Fatal("workspace was retired before the grace period")
	}
	manager.retireIdleLocked(base.Add(idleGracePeriod-time.Nanosecond), map[string]protocol.Actor{})
	if canceled {
		t.Fatal("workspace was retired at the grace boundary")
	}
	manager.retireIdleLocked(base.Add(idleGracePeriod), map[string]protocol.Actor{})
	if !canceled {
		t.Fatal("idle workspace was not retired after the grace period")
	}
	state, ok := manager.watching["workspace"]
	if !ok || !state.stopping {
		t.Fatal("retired workspace was not retained in stopping state")
	}
	manager.watcherStopped("workspace", done)
	if _, ok := manager.watching["workspace"]; ok {
		t.Fatal("stopped workspace subscription remains tracked")
	}
}

func TestRetireIdleResetsGraceWhenAddressableActorReturns(t *testing.T) {
	manager := &Manager{watching: make(map[string]watchState)}
	manager.watching["workspace"] = watchState{cancel: func() {}, done: make(chan struct{})}
	base := time.Unix(100, 0)
	manager.retireIdleLocked(base, map[string]protocol.Actor{})
	manager.retireIdleLocked(base.Add(idleGracePeriod-time.Nanosecond), map[string]protocol.Actor{
		"workspace": {Address: "actor", Addressable: true, ActorKind: protocol.ActorKindAgent},
	})
	if !manager.watching["workspace"].idleSince.IsZero() {
		t.Fatal("live addressable actor did not reset idle grace")
	}
	manager.retireIdleLocked(base.Add(idleGracePeriod*2), map[string]protocol.Actor{})
	if _, ok := manager.watching["workspace"]; !ok {
		t.Fatal("workspace was retired immediately after actor departure")
	}
}

func TestReactivationDuringStoppingWaitsForCompletion(t *testing.T) {
	manager := &Manager{watching: make(map[string]watchState)}
	canceled := 0
	done := make(chan struct{})
	manager.watching["workspace"] = watchState{cancel: func() { canceled++ }, done: done}
	base := time.Unix(100, 0)
	manager.retireIdleLocked(base, map[string]protocol.Actor{})
	manager.retireIdleLocked(base.Add(idleGracePeriod), map[string]protocol.Actor{})
	manager.retireIdleLocked(base.Add(idleGracePeriod), map[string]protocol.Actor{
		"workspace": {Addressable: true, ActorKind: protocol.ActorKindAgent},
	})
	if canceled != 1 {
		t.Fatalf("cancel count = %d, want 1", canceled)
	}
	if !manager.watching["workspace"].stopping {
		t.Fatal("reactivation replaced the stopping state")
	}
	manager.watcherStopped("workspace", done)
	if _, ok := manager.watching["workspace"]; ok {
		t.Fatal("workspace was not removed after completion acknowledgment")
	}
}

func TestWatcherStoppedIgnoresStaleCompletion(t *testing.T) {
	manager := &Manager{watching: make(map[string]watchState)}
	current := make(chan struct{})
	stale := make(chan struct{})
	manager.watching["workspace"] = watchState{done: current}
	manager.watcherStopped("workspace", stale)
	if _, ok := manager.watching["workspace"]; !ok {
		t.Fatal("stale watcher removed the replacement state")
	}
}

func TestDiscoverReactivationDoesNotOverlapStoppingWatcher(t *testing.T) {
	coordination := &fakeCoordination{}
	actor := testActor(t.TempDir())
	actor.Git = &protocol.GitContext{}
	coordination.sessions = []protocol.Actor{actor}
	manager := &Manager{
		engine:   coordination,
		binary:   "test-watchman",
		watching: make(map[string]watchState),
		now:      func() time.Time { return time.Unix(100, 0) },
	}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	manager.runWatcher = func(_ context.Context, _ protocol.Actor) {
		started <- struct{}{}
		<-release
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.discover(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("watcher did not start")
	}
	coordination.sessions = nil
	manager.now = func() time.Time { return time.Unix(100, 0) }
	manager.discover(ctx) // starts the bounded grace period.
	manager.now = func() time.Time { return time.Unix(100, 0).Add(idleGracePeriod) }
	manager.discover(ctx) // requests cancellation, but the fake watcher is still running.
	coordination.sessions = []protocol.Actor{actor}
	manager.discover(ctx)
	if !manager.watching[actor.WorkspaceUUID].stopping {
		t.Fatal("reactivation replaced the stopping state")
	}
	select {
	case <-started:
		t.Fatal("reactivation started a duplicate watcher")
	default:
	}

	close(release)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.mu.Lock()
		_, exists := manager.watching[actor.WorkspaceUUID]
		manager.mu.Unlock()
		if !exists {
			break
		}
		time.Sleep(time.Millisecond)
	}
	manager.discover(ctx)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("watcher was not recreated after shutdown acknowledgment")
	}
	cancel()
	manager.stopAll()
}
