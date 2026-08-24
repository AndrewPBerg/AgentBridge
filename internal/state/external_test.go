package state

import (
	"fmt"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestExternalChangeUsesDeterministicUnknownActorAndWorkspaceScope(t *testing.T) {
	engine, _, now := newTestEngine(t)
	actor := register(t, engine, "external-observer")
	path := "/repo/file.txt"
	change := protocol.ExternalChange{
		ID:                "33333333-3333-4333-8333-333333333333",
		RepositoryUUID:    actor.RepositoryUUID,
		WorkspaceUUID:     actor.WorkspaceUUID,
		IntervalStartedAt: *now,
		IntervalEndedAt:   now.Add(time.Millisecond),
		ContinuityState:   "current",
		ChangeKind:        "modified",
		Path:              path,
		Before:            &protocol.FileSnapshot{Path: path, Exists: true, Kind: "file", SHA256: "before"},
		After:             &protocol.FileSnapshot{Path: path, Exists: true, Kind: "file", SHA256: "after"},
		WatchmanClock:     "c:1:2",
	}
	observed, err := engine.ObserveExternalChange(change)
	if err != nil {
		t.Fatal(err)
	}
	wantUnknown := UnknownActorUUID(actor.WorkspaceUUID)
	if observed.Actor != wantUnknown || observed.UnknownActor != wantUnknown {
		t.Fatalf("unknown actor = %q/%q, want %q", observed.Actor, observed.UnknownActor, wantUnknown)
	}
	if retry, err := engine.ObserveExternalChange(observed); err != nil || retry.ID != observed.ID {
		t.Fatalf("idempotent observation = %#v, %v", retry, err)
	}
	conflict := observed
	conflict.ChangeKind = "deleted"
	if _, err := engine.ObserveExternalChange(conflict); err == nil {
		t.Fatal("conflicting external observation ID was accepted")
	}
	outside := change
	outside.ID = "44444444-4444-4444-8444-444444444444"
	outside.Path = "/outside/file.txt"
	outside.Before, outside.After = nil, nil
	outside.ChangeKind = "created"
	if _, err := engine.ObserveExternalChange(outside); err == nil {
		t.Fatal("out-of-workspace external path was accepted")
	}
}

func TestProjectedExternalChangeReplayDoesNotRequirePayload(t *testing.T) {
	engine, err := New(&memoryJournal{}, []protocol.Event{{Version: protocol.Version, Sequence: 1, Type: "external_change.observed"}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(engine.externalChanges) != 0 {
		t.Fatalf("projected external replay populated cache: %#v", engine.externalChanges)
	}
}

func TestExternalChangeCacheIsBounded(t *testing.T) {
	engine, _, now := newTestEngine(t)
	actor := register(t, engine, "external-cache")
	firstID := testActorUUID("external-cache-0")
	for index := 0; index <= externalChangeCacheLimit; index++ {
		id := testActorUUID(fmt.Sprintf("external-cache-%d", index))
		if _, err := engine.ObserveExternalChange(protocol.ExternalChange{
			ID: id, RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID,
			IntervalStartedAt: *now, IntervalEndedAt: *now, ContinuityState: "current", ChangeKind: "modified",
			Path: "/repo/file.txt",
		}); err != nil {
			t.Fatal(err)
		}
	}
	if len(engine.externalChanges) != externalChangeCacheLimit {
		t.Fatalf("external cache size = %d", len(engine.externalChanges))
	}
	if _, err := engine.ExternalChange(firstID); err == nil {
		t.Fatal("oldest external change was not evicted")
	}
	if _, err := engine.ExternalChange(testActorUUID(fmt.Sprintf("external-cache-%d", externalChangeCacheLimit))); err != nil {
		t.Fatalf("latest external change missing: %v", err)
	}
}

func TestExternalChangeNotifiesOnlyActorsWithExactActivePathIntent(t *testing.T) {
	engine, _, now := newTestEngine(t)
	first := register(t, engine, "external-first")
	second := register(t, engine, "external-second")
	unrelated := register(t, engine, "external-unrelated")
	path := "/repo/target.txt"
	mustBeginIntent(t, engine, &protocol.Intent{ID: "external-first-intent", Actor: first.Address, ToolCallID: "first-call", Tool: "edit", Operation: "edit", Paths: []string{path}, CWD: "/repo"})
	mustBeginIntent(t, engine, &protocol.Intent{ID: "external-second-intent", Actor: second.Address, ToolCallID: "second-call", Tool: "edit", Operation: "edit", Paths: []string{path}, CWD: "/repo"})
	mustBeginIntent(t, engine, &protocol.Intent{ID: "external-unrelated-intent", Actor: unrelated.Address, ToolCallID: "unrelated-call", Tool: "edit", Operation: "edit", Paths: []string{"/repo/other.txt"}, CWD: "/repo"})

	change, err := engine.ObserveExternalChange(protocol.ExternalChange{
		ID: "55555555-5555-4555-8555-555555555555", RepositoryUUID: first.RepositoryUUID, WorkspaceUUID: first.WorkspaceUUID,
		IntervalStartedAt: *now, IntervalEndedAt: *now, ContinuityState: "current", ChangeKind: "modified", Path: path,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ExternalChange(change.ID); err != nil {
		t.Fatalf("durable external observation missing: %v", err)
	}
	if err := engine.NotifyExternalChange(change); err != nil {
		t.Fatal(err)
	}
	for _, actor := range []protocol.Actor{first, second} {
		var externalMessages []protocol.Message
		for _, message := range mustPoll(t, engine, actor.Address) {
			if message.Kind == "external_change" {
				externalMessages = append(externalMessages, message)
			}
		}
		if len(externalMessages) != 1 || externalMessages[0].From != change.UnknownActor {
			t.Fatalf("target actor %s external messages = %#v", actor.Address, externalMessages)
		}
	}
	for _, message := range mustPoll(t, engine, unrelated.Address) {
		if message.Kind == "external_change" {
			t.Fatalf("unrelated actor received external message %#v", message)
		}
	}
}

func TestWatchContinuityRequiresKnownWorkspace(t *testing.T) {
	engine, _, now := newTestEngine(t)
	actor := register(t, engine, "continuity")
	status := protocol.WatchContinuity{RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, At: *now, WatchmanClock: "c:1:1"}
	lost, err := engine.WatchContinuityLost(status)
	if err != nil || lost.State != "lost" {
		t.Fatalf("lost continuity = %#v, %v", lost, err)
	}
	restored, err := engine.WatchContinuityRestored(status)
	if err != nil || restored.State != "restored" {
		t.Fatalf("restored continuity = %#v, %v", restored, err)
	}
	status.WorkspaceUUID = "55555555-5555-4555-8555-555555555555"
	if _, err := engine.WatchContinuityLost(status); err == nil {
		t.Fatal("unknown workspace continuity was accepted")
	}
}
