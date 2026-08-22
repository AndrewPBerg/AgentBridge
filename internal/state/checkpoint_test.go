package state

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointReplaySkipsLegacyPreScopeCheckpoint(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, "legacy-pre-scope")
	checkpoint := protocol.CheckpointRequest{ID: "legacy-pre-scope", Actor: "pi:01234567-89ab-4def-8123-456789abcdef", CheckpointKind: "settled"}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	events := append(append([]protocol.Event(nil), journal.events...), protocol.Event{Version: protocol.Version, Sequence: 2, Type: "checkpoint.requested", At: time.Now().UTC(), Data: data})
	replayed, err := New(&memoryJournal{}, events, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := replayed.checkpoints[checkpoint.ID]; exists {
		t.Fatal("legacy pre-scope checkpoint entered coordination state")
	}
}

func TestCheckpointReplayBackfillsLegacyUnknownWorkUnitAsStandalone(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "legacy-work-unit")
	checkpoint := protocol.CheckpointRequest{
		ID: "legacy-work-unit-checkpoint", Actor: actor.Address, SessionGeneration: actor.Generation,
		RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID,
		WorkUnitUUID: "33333333-3333-5333-8333-333333333333", CheckpointKind: "settled", JournalStart: 1, JournalEnd: 1,
	}
	data, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	events := append(append([]protocol.Event(nil), journal.events...), protocol.Event{Version: protocol.Version, Sequence: 2, Type: "checkpoint.requested", At: time.Now().UTC(), Data: data})
	replayed, err := New(&memoryJournal{}, events, Options{})
	if err != nil {
		t.Fatal(err)
	}
	projected := replayed.checkpoints[checkpoint.ID]
	if projected.WorkUnitUUID != "" {
		t.Fatalf("legacy unknown WorkUnit relation survived replay: %#v", projected)
	}
}

func TestCheckpointRequestIsIdempotentAndCapturesRange(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "checkpoint")
	request := protocol.CheckpointRequest{ID: "checkpoint-request-1", Actor: actor.Address, CheckpointKind: "settled"}
	first, err := engine.RequestCheckpoint(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.RequestCheckpoint(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || first.JournalStart != first.JournalEnd || first.RepositoryUUID != actor.RepositoryUUID || first.WorkspaceUUID != actor.WorkspaceUUID {
		t.Fatalf("checkpoint request = %#v, duplicate = %#v", first, second)
	}
	if len(journal.events) != 2 {
		t.Fatalf("journal events = %d, want registration plus one request", len(journal.events))
	}
	turnIndex := 3
	for name, mutate := range map[string]func(*protocol.CheckpointRequest){
		"kind":       func(value *protocol.CheckpointRequest) { value.CheckpointKind = "handoff" },
		"turn index": func(value *protocol.CheckpointRequest) { value.TurnIndex = &turnIndex },
		"git":        func(value *protocol.CheckpointRequest) { value.Git = &protocol.GitContext{Head: "different"} },
		"jj":         func(value *protocol.CheckpointRequest) { value.JJ = &protocol.JJContext{ChangeID: "different"} },
	} {
		t.Run("conflicting "+name, func(t *testing.T) {
			conflict := request
			mutate(&conflict)
			if _, err := engine.RequestCheckpoint(conflict); err == nil {
				t.Fatalf("conflicting duplicate checkpoint %s was accepted", name)
			}
		})
	}
	future := protocol.CheckpointRequest{ID: "checkpoint-future", Actor: actor.Address, CheckpointKind: "settled", JournalEnd: 99}
	if _, err := engine.RequestCheckpoint(future); err == nil {
		t.Fatal("future checkpoint journal range was accepted")
	}
	wrongScope := protocol.CheckpointRequest{ID: "checkpoint-wrong-scope", Actor: actor.Address, CheckpointKind: "settled", RepositoryUUID: "11111111-1111-4111-8111-111111111111", WorkspaceUUID: actor.WorkspaceUUID}
	if _, err := engine.RequestCheckpoint(wrongScope); err == nil {
		t.Fatal("checkpoint from a different repository scope was accepted")
	}
}

func TestCheckpointFirstRangeIncludesAllExplicitTestResults(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "checkpoint-results")
	zero := 0
	if _, err := engine.RecordTestResult(protocol.TestResult{ID: "result-one", Actor: actor.Address, Command: "go test", ExitCode: &zero}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RecordTestResult(protocol.TestResult{ID: "result-two", Actor: actor.Address, Command: "go test", ExitCode: &zero}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "first-results", Actor: actor.Address, CheckpointKind: "settled", TestResultIDs: []string{"result-one", "result-two"}})
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.JournalStart != 2 || checkpoint.JournalEnd != 3 {
		t.Fatalf("checkpoint range = %d..%d, want 2..3", checkpoint.JournalStart, checkpoint.JournalEnd)
	}
	if len(journal.events) != 4 {
		t.Fatalf("journal events = %d, want registration, two results, checkpoint", len(journal.events))
	}
}

func TestCheckpointRangesAreSequentialAcrossKinds(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	actor := register(t, engine, "checkpoint-kinds")
	zero := 0
	if _, err := engine.RecordTestResult(protocol.TestResult{ID: "kind-result", Actor: actor.Address, Command: "go test", ExitCode: &zero}); err != nil {
		t.Fatal(err)
	}
	first, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "kind-first", Actor: actor.Address, CheckpointKind: "settled", TestResultIDs: []string{"kind-result"}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "kind-second", Actor: actor.Address, CheckpointKind: "handoff"})
	if err != nil {
		t.Fatal(err)
	}
	if first.JournalStart != 2 || first.JournalEnd != 2 || second.JournalStart != 3 || second.JournalEnd != 3 {
		t.Fatalf("ranges = %d..%d then %d..%d, want 2..2 then 3..3", first.JournalStart, first.JournalEnd, second.JournalStart, second.JournalEnd)
	}
}

func TestCheckpointExplicitEvidenceMustFitExplicitRange(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	actor := register(t, engine, "checkpoint-explicit-range")
	zero := 0
	if _, err := engine.RecordTestResult(protocol.TestResult{ID: "out-of-range", Actor: actor.Address, Command: "go test", ExitCode: &zero}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "reject-range", Actor: actor.Address, CheckpointKind: "settled", JournalStart: 1, JournalEnd: 1, TestResultIDs: []string{"out-of-range"}}); err == nil {
		t.Fatal("explicit evidence outside the requested range was accepted")
	}
}

func TestCheckpointJournalStartUsesLatestEligibleCheckpoint(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	actor := register(t, engine, "checkpoint-range")
	if _, err := engine.RecordSessionEvent(protocol.SessionEvent{ID: "before-one", Actor: actor.Address, Type: "turn"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "prior-one", Actor: actor.Address, CheckpointKind: "settled", JournalStart: 2, JournalEnd: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RecordSessionEvent(protocol.SessionEvent{ID: "before-two", Actor: actor.Address, Type: "turn"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "prior-two", Actor: actor.Address, CheckpointKind: "settled", JournalStart: 4, JournalEnd: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RecordSessionEvent(protocol.SessionEvent{ID: "before-final", Actor: actor.Address, Type: "turn"}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "final", Actor: actor.Address, CheckpointKind: "settled"})
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.JournalStart != 5 || checkpoint.JournalEnd != 6 {
		t.Fatalf("checkpoint range = %d..%d, want 5..6", checkpoint.JournalStart, checkpoint.JournalEnd)
	}
}
