package state

import (
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

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
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return time.Now() }})
	if err != nil {
		t.Fatal(err)
	}
	got, participants, err := replayed.WorkUnit(unit.UUID)
	if err != nil || got.State != protocol.WorkUnitActive || len(participants) != 2 {
		t.Fatalf("replayed work unit = %#v participants=%#v err=%v", got, participants, err)
	}
}
