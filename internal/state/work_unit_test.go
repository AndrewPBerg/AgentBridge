//nolint:cyclop // package average includes out-of-scope production functions.
package state

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestWorkUnitUpdatesAcrossStatesAreIdempotentAndReplayable(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	actor := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, Objective: "one", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: actor.Address}); err != nil {
		t.Fatal(err)
	}
	for _, state := range []protocol.WorkUnitState{protocol.WorkUnitActive, protocol.WorkUnitBlocked, protocol.WorkUnitVerified} {
		if _, err = engine.TransitionWorkUnit(protocol.WorkUnitTransitionParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, State: state}); err != nil {
			t.Fatal(err)
		}
		transitionEvents := len(journal.events)
		if _, err = engine.TransitionWorkUnit(protocol.WorkUnitTransitionParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, State: state}); err != nil {
			t.Fatal(err)
		}
		if len(journal.events) != transitionEvents {
			t.Fatalf("idempotent %s transition appended an event", state)
		}
		objective := string(state)
		beforeEvents := len(journal.events)
		updated, err := engine.UpdateWorkUnit(protocol.WorkUnitUpdateParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, Objective: &objective})
		if err != nil {
			t.Fatal(err)
		}
		if updated.Objective != objective {
			t.Fatalf("updated objective = %q", updated.Objective)
		}
		updatedAt := updated.UpdatedAt
		if _, err = engine.UpdateWorkUnit(protocol.WorkUnitUpdateParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, Objective: &objective}); err != nil {
			t.Fatal(err)
		}
		if len(journal.events) != beforeEvents+1 {
			t.Fatalf("no-op update appended an event: %d -> %d", beforeEvents+1, len(journal.events))
		}
		got, _, err := engine.WorkUnit(unit.UUID)
		if err != nil || !got.UpdatedAt.Equal(updatedAt) {
			t.Fatalf("no-op update changed timestamp: %#v, %v", got, err)
		}
		*now = now.Add(time.Second)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := replayed.WorkUnit(unit.UUID)
	if err != nil || got.State != protocol.WorkUnitVerified || got.Objective != string(protocol.WorkUnitVerified) {
		t.Fatalf("replayed work unit = %#v, %v", got, err)
	}
}

func TestWorkUnitChangeAttachmentIsIdempotentAndReplayable(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	other := register(t, engine, "31234567-89ab-4def-8123-456789abcdef")
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, Objective: "ship", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: actor.Address}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: other.Address}); err != nil {
		t.Fatal(err)
	}
	params := protocol.WorkUnitChangeAttachParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, ChangeID: "rlyxyz", Kind: protocol.WorkUnitChangeMaterialized}
	first, err := engine.AttachWorkUnitChange(params)
	if err != nil {
		t.Fatal(err)
	}
	before := len(journal.events)
	second, err := engine.AttachWorkUnitChange(params)
	if err != nil || first != second || len(journal.events) != before {
		t.Fatalf("attachment not idempotent: %#v %#v events=%d", first, second, len(journal.events))
	}
	if _, err = engine.AttachWorkUnitChange(protocol.WorkUnitChangeAttachParams{WorkUnitUUID: unit.UUID, Actor: other.Address, ChangeID: params.ChangeID, Kind: params.Kind}); err == nil {
		t.Fatal("different actor reused JJ change relation")
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	changes, err := replayed.WorkUnitChanges(unit.UUID)
	if err != nil || len(changes) != 1 || changes[0].ChangeID != "rlyxyz" {
		t.Fatalf("replayed changes = %#v, %v", changes, err)
	}
	data, err := json.Marshal(protocol.WorkUnitChangeAttachedEvent{Relation: protocol.WorkUnitChange{WorkUnitUUID: unit.UUID, ChangeID: params.ChangeID, Kind: params.Kind, Actor: other.Address, AttachedAt: first.AttachedAt}})
	if err != nil {
		t.Fatal(err)
	}
	conflict := protocol.Event{Version: protocol.Version, Sequence: uint64(len(journal.events) + 1), Type: "work_unit.change_attached", At: first.AttachedAt, Data: data}
	conflictingEvents := append(append([]protocol.Event(nil), journal.events...), conflict)
	if _, err = New(&memoryJournal{}, conflictingEvents, Options{Now: time.Now}); err == nil {
		t.Fatal("replay accepted JJ change relation reuse by another actor")
	}
}

func TestWorkUnitJournalReplayAndEqualParticipants(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	other := register(t, engine, "11234567-89ab-4def-8123-456789abcdef")
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, Objective: "ship it", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: actor.Address}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: other.Address}); err != nil {
		t.Fatal(err)
	}
	if _, err = engine.TransitionWorkUnit(protocol.WorkUnitTransitionParams{WorkUnitUUID: unit.UUID, Actor: other.Address, State: protocol.WorkUnitActive}); err != nil {
		t.Fatal(err)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	got, participants, err := replayed.WorkUnit(unit.UUID)
	if err != nil || got.State != protocol.WorkUnitActive || len(participants) != 2 {
		t.Fatalf("replayed work unit = %#v participants=%#v err=%v", got, participants, err)
	}
}
