package state

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestDirectionCreateIsNormalizedIdempotentAndReplayable(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	creator := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	input := protocol.Direction{
		UUID:      "53e7929c-193b-4440-b309-a4eb2be8bf9c",
		Objective: "coordinate the project",
		CreatedBy: creator.Address,
		State:     protocol.DirectionActive,
	}

	created, err := engine.CreateDirection(input)
	if err != nil {
		t.Fatal(err)
	}
	if created.State != protocol.DirectionDraft || created.CreatedAt.IsZero() || !created.UpdatedAt.Equal(created.CreatedAt) {
		t.Fatalf("direction was not normalized: %#v", created)
	}
	events := len(journal.events)
	duplicate, err := engine.CreateDirection(input)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate != created || len(journal.events) != events {
		t.Fatalf("duplicate create was not idempotent: %#v %#v, events %d", created, duplicate, len(journal.events))
	}

	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Direction(created.UUID)
	if err != nil || got != created {
		t.Fatalf("replayed direction = %#v, want %#v (err %v)", got, created, err)
	}
}

func TestDirectionCreateRequiresCanonicalUUIDObjectiveAndCreator(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	creator := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	base := protocol.Direction{UUID: "53e7929c-193b-4440-b309-a4eb2be8bf9c", Objective: "objective", CreatedBy: creator.Address}
	for name, direction := range map[string]protocol.Direction{
		"uuid":      {UUID: "not-a-uuid", Objective: base.Objective, CreatedBy: base.CreatedBy},
		"objective": {UUID: base.UUID, CreatedBy: base.CreatedBy},
		"creator":   {UUID: base.UUID, Objective: base.Objective, CreatedBy: "missing"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := engine.CreateDirection(direction); err == nil {
				t.Fatal("invalid direction was accepted")
			}
		})
	}
}

func TestDirectionCreatorMustBeCanonicalUUIDOnReplay(t *testing.T) {
	_, _, now := newTestEngine(t)
	direction := protocol.Direction{UUID: "63e7929c-193b-4440-b309-a4eb2be8bf9c", Objective: "objective", CreatedBy: "not-a-uuid", State: protocol.DirectionDraft, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
	data, err := json.Marshal(protocol.DirectionCreatedEvent{Direction: direction})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = New(&memoryJournal{}, []protocol.Event{{Version: protocol.Version, Sequence: 1, Type: "direction.created", Data: data}}, Options{Now: func() time.Time { return *now }}); err == nil {
		t.Fatal("replay accepted invalid direction creator")
	}
}

func TestDirectionTransitionLifecycleIsReplaySafe(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	actor := register(t, engine, "11234567-89ab-4def-8123-456789abcdef")
	direction, err := engine.CreateDirection(protocol.Direction{UUID: "12345678-89ab-4def-8123-456789abcdef", Objective: "objective", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	transitionDirectionThroughLifecycle(t, engine, now, actor.Address, direction.UUID)
	if _, err := engine.TransitionDirection(protocol.DirectionTransitionParams{DirectionUUID: direction.UUID, Actor: actor.Address, State: protocol.DirectionCompleted}); err == nil {
		t.Fatal("terminal same-state transition was accepted")
	}
	if _, err := engine.TransitionDirection(protocol.DirectionTransitionParams{DirectionUUID: direction.UUID, Actor: "21234567-89ab-4def-8123-456789abcdef", State: protocol.DirectionAbandoned}); err == nil {
		t.Fatal("unregistered actor was accepted")
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Direction(direction.UUID)
	if err != nil || got.State != protocol.DirectionCompleted || got.CompletedAt == nil || !got.CompletedAt.Equal(got.UpdatedAt) {
		t.Fatalf("replayed direction = %#v, want completed with matching completion timestamp (err %v)", got, err)
	}
}

func transitionDirectionThroughLifecycle(t *testing.T, engine *Engine, now *time.Time, actor, direction string) {
	for _, state := range []protocol.DirectionState{protocol.DirectionActive, protocol.DirectionPaused, protocol.DirectionActive, protocol.DirectionConverging, protocol.DirectionVerified, protocol.DirectionCompleted} {
		*now = now.Add(time.Second)
		got, err := engine.TransitionDirection(protocol.DirectionTransitionParams{DirectionUUID: direction, Actor: actor, State: state})
		if err != nil {
			t.Fatalf("transition to %s: %v", state, err)
		}
		if got.State != state || !got.UpdatedAt.Equal(*now) {
			t.Fatalf("transition to %s = %#v", state, got)
		}
	}
}

func TestDirectionTransitionRejectsIllegalAndStaleReplay(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	actor := register(t, engine, "31234567-89ab-4def-8123-456789abcdef")
	direction, err := engine.CreateDirection(protocol.Direction{UUID: "32345678-89ab-4def-8123-456789abcdef", Objective: "objective", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	at := now.UTC().Add(time.Second)
	if _, err := engine.TransitionDirection(protocol.DirectionTransitionParams{DirectionUUID: direction.UUID, Actor: actor.Address, State: protocol.DirectionPaused}); err == nil {
		t.Fatal("illegal draft transition was accepted")
	}
	data, err := json.Marshal(protocol.DirectionTransitionEvent{DirectionUUID: direction.UUID, Actor: actor.Address, From: protocol.DirectionActive, To: protocol.DirectionPaused, At: at})
	if err != nil {
		t.Fatal(err)
	}
	bad := append(append([]protocol.Event(nil), journal.events...), protocol.Event{Version: protocol.Version, Sequence: uint64(len(journal.events) + 1), Type: "direction.transitioned", Data: data})
	if _, err := New(&memoryJournal{}, bad, Options{Now: func() time.Time { return *now }}); err == nil {
		t.Fatal("replay accepted a transition with a stale prior state")
	}
}

func TestWorkUnitDirectionLinkRequiresExistingCanonicalDirection(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	creator := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	direction, err := engine.CreateDirection(protocol.Direction{UUID: "73e7929c-193b-4440-b309-a4eb2be8bf9c", Objective: "objective", CreatedBy: creator.Address})
	if err != nil {
		t.Fatal(err)
	}
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "83e7929c-193b-4440-b309-a4eb2be8bf9c", RepositoryUUID: creator.RepositoryUUID, WorkspaceUUID: creator.WorkspaceUUID, Objective: "work", CreatedBy: creator.Address, DirectionUUID: direction.UUID})
	if err != nil || unit.DirectionUUID != direction.UUID {
		t.Fatalf("valid direction link failed: %#v, %v", unit, err)
	}
	if _, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "93e7929c-193b-4440-b309-a4eb2be8bf9c", RepositoryUUID: creator.RepositoryUUID, WorkspaceUUID: creator.WorkspaceUUID, Objective: "work", CreatedBy: creator.Address, DirectionUUID: "not-a-uuid"}); err == nil {
		t.Fatal("invalid direction UUID accepted")
	}
	if _, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "a3e7929c-193b-4440-b309-a4eb2be8bf9c", RepositoryUUID: creator.RepositoryUUID, WorkspaceUUID: creator.WorkspaceUUID, Objective: "work", CreatedBy: creator.Address, DirectionUUID: "b3e7929c-193b-4440-b309-a4eb2be8bf9c"}); err == nil {
		t.Fatal("unknown direction accepted")
	}
	if _, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }}); err != nil {
		t.Fatalf("valid linked work unit failed replay: %v", err)
	}
}
