package state

import (
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
