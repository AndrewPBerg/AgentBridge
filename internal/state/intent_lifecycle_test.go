package state

import (
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestExpiredIntentIsIgnoredAfterReplay(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	first := register(t, engine, testActorUUID("first"))
	second := register(t, engine, testActorUUID("second"))
	intent := protocol.Intent{ID: "expired-intent", Actor: first.Address, ToolCallID: "first-call", Tool: "edit", Operation: "edit", Paths: []string{"/repo/file"}, CWD: "/repo", ExpiresAt: now.Add(time.Minute)}
	mustBeginIntent(t, engine, &intent)
	beforeFresh := append([]protocol.Event(nil), journal.events...)
	*now = now.Add(2 * time.Minute)
	collisions, err := engine.BeginIntent(protocol.Intent{ID: "fresh-intent", Actor: second.Address, ToolCallID: "second-call", Tool: "edit", Operation: "edit", Paths: []string{"/repo/file"}, CWD: "/repo"})
	if err != nil {
		t.Fatal(err)
	}
	if len(collisions) != 0 {
		t.Fatalf("expired intent produced collisions: %#v", collisions)
	}
	replayed, err := New(&memoryJournal{}, beforeFresh, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	if collisions, err := replayed.BeginIntent(protocol.Intent{ID: "replay-fresh", Actor: first.Address, ToolCallID: "replay-call", Tool: "edit", Operation: "edit", Paths: []string{"/repo/file"}, CWD: "/repo"}); err != nil {
		t.Fatal(err)
	} else if len(collisions) != 0 {
		t.Fatalf("replayed expired intent produced collisions: %#v", collisions)
	}
}
