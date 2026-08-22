package watchman

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type fakeCoordination struct {
	mu         sync.Mutex
	sessions   []protocol.Actor
	changes    []protocol.ExternalChange
	notified   int
	match      bool
	active     bool
	restoredCh chan struct{}
	changeCh   chan protocol.ExternalChange
}

func (f *fakeCoordination) Sessions(bool) []protocol.Actor {
	return append([]protocol.Actor(nil), f.sessions...)
}

//nolint:gocritic // fake implements the public value-based coordination API.
func (f *fakeCoordination) ObserveExternalChange(change protocol.ExternalChange) (protocol.ExternalChange, error) {
	change.UnknownActor = "33333333-3333-5333-8333-333333333333"
	change.Actor = change.UnknownActor
	f.mu.Lock()
	f.changes = append(f.changes, change)
	f.mu.Unlock()
	if f.changeCh != nil {
		select {
		case f.changeCh <- change:
		default:
		}
	}
	return change, nil
}

func (f *fakeCoordination) HasActiveIntent(string, string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active
}

func (f *fakeCoordination) MatchIntentTransition(string, string, *protocol.FileSnapshot, *protocol.FileSnapshot, time.Time, time.Time) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.match {
		return []string{"intent"}
	}
	return nil
}

func (f *fakeCoordination) NotifyExternalChange(protocol.ExternalChange) error {
	f.notified++
	return nil
}

//nolint:gocritic // fake implements the public value-based coordination API.
func (f *fakeCoordination) WatchContinuityLost(status protocol.WatchContinuity) (protocol.WatchContinuity, error) {
	status.State = "lost"
	return status, nil
}

//nolint:gocritic // fake implements the public value-based coordination API.
func (f *fakeCoordination) WatchContinuityRestored(status protocol.WatchContinuity) (protocol.WatchContinuity, error) {
	status.State = "restored"
	if f.restoredCh != nil {
		select {
		case f.restoredCh <- struct{}{}:
		default:
		}
	}
	return status, nil
}

func testActor(root string) protocol.Actor {
	return protocol.Actor{
		Address: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", SessionUUID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		Harness: "pi", CWD: root, State: "active", ActorKind: protocol.ActorKindAgent, Addressable: true, PresenceKind: protocol.PresenceKindLease,
		RepositoryUUID: "11111111-1111-4111-8111-111111111111", WorkspaceUUID: "22222222-2222-4222-8222-222222222222",
		RepositoryRoot: root, HeartbeatAt: time.Now(),
	}
}

func TestProcessorRecordsOnlyUnexplainedStateTransitions(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "file.txt")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	coordination := &fakeCoordination{}
	actor := testActor(root)
	processor := newProcessor(coordination, &actor)
	if err := processor.reconcile([]string{path}, "c:1", "current"); err != nil {
		t.Fatal(err)
	}
	if len(coordination.changes) != 0 {
		t.Fatal("initial baseline fabricated a change")
	}
	if err := os.WriteFile(path, []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := processor.observe([]string{path}, "c:2"); err != nil {
		t.Fatal(err)
	}
	if len(coordination.changes) != 1 || coordination.changes[0].ChangeKind != "modified" || coordination.notified != 1 {
		t.Fatalf("changes = %#v, notified=%d", coordination.changes, coordination.notified)
	}
	coordination.mu.Lock()
	coordination.active = true
	coordination.mu.Unlock()
	if err := os.WriteFile(path, []byte("agent"), 0o600); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		coordination.mu.Lock()
		coordination.active = false
		coordination.match = true
		coordination.mu.Unlock()
	}()
	if err := processor.observe([]string{path}, "c:3"); err != nil {
		t.Fatal(err)
	}
	if len(coordination.changes) != 1 {
		t.Fatal("instrumented transition was duplicated as unknown")
	}
}
