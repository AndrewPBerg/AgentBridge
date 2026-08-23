//nolint:cyclop // package average includes out-of-scope production functions.
package provenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestWorkUnitProjectionNormalizesParticipantsAndReplay(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	}()
	unit := protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "objective", State: protocol.WorkUnitProposed, CreatedBy: "01234567-89ab-4def-8123-456789abcdef", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := db.Project(event(t, 1, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
		t.Fatal(err)
	}
	member := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, JoinedAt: time.Now().UTC(), ParticipationState: "active"}
	if err := db.Project(event(t, 2, "work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, Result: member})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 2, "work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, Result: member})); err != nil {
		t.Fatal(err)
	}
	got, err := db.WorkUnit(unit.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if got.UUID != unit.UUID || len(got.Participants) != 1 || got.Participants[0].Actor != unit.CreatedBy {
		t.Fatalf("work unit = %#v", got)
	}
	var length int
	if err := db.db.QueryRowContext(context.Background(), `SELECT length(work_unit_uuid) FROM work_units`).Scan(&length); err != nil {
		t.Fatal(err)
	}
	if length != 16 {
		t.Fatalf("work unit UUID length = %d", length)
	}
}

func TestWorkUnitChangeProjectionUsesBLOBAndIsReplaySafe(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	at := time.Now().UTC()
	unit := protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "objective", State: protocol.WorkUnitProposed, CreatedBy: "01234567-89ab-4def-8123-456789abcdef", CreatedAt: at, UpdatedAt: at}
	actor := protocol.Actor{Address: unit.CreatedBy, SessionUUID: unit.CreatedBy, Harness: "pi", CWD: "/repo", State: "active", StartedAt: at, HeartbeatAt: at, RepositoryUUID: unit.RepositoryUUID, RepositoryRoot: "/repo", WorkspaceUUID: unit.WorkspaceUUID, WorkspaceRoot: "/repo", WorkspaceKind: "git"}
	member := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, JoinedAt: at, ParticipationState: "active"}
	other := "31234567-89ab-4def-8123-456789abcdef"
	otherMember := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: other, JoinedAt: at, ParticipationState: "active"}
	for sequence, value := range []struct {
		kind string
		data any
	}{
		{"actor.upserted", actor},
		{"work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit}},
		{"work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, Result: member}},
		{"work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: other, Result: otherMember}},
		{"work_unit.change_attached", protocol.WorkUnitChangeAttachedEvent{Relation: protocol.WorkUnitChange{WorkUnitUUID: unit.UUID, ChangeID: "rlyxyz", Kind: protocol.WorkUnitChangeWorking, Actor: unit.CreatedBy, AttachedAt: at}}},
	} {
		if err := db.Project(event(t, uint64(sequence+1), value.kind, value.data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Project(event(t, 5, "work_unit.change_attached", protocol.WorkUnitChangeAttachedEvent{Relation: protocol.WorkUnitChange{WorkUnitUUID: unit.UUID, ChangeID: "rlyxyz", Kind: protocol.WorkUnitChangeWorking, Actor: unit.CreatedBy, AttachedAt: at}})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 6, "work_unit.change_attached", protocol.WorkUnitChangeAttachedEvent{Relation: protocol.WorkUnitChange{WorkUnitUUID: unit.UUID, ChangeID: "rlyxyz", Kind: protocol.WorkUnitChangeWorking, Actor: other, AttachedAt: at}})); err == nil {
		t.Fatal("projection accepted JJ change relation reuse by another actor")
	}
	got, err := db.WorkUnit(unit.UUID)
	if err != nil || len(got.Changes) != 1 || got.Changes[0].ChangeID != "rlyxyz" {
		t.Fatalf("work unit changes = %#v, %v", got.Changes, err)
	}
	var relationType, actorType string
	if err := db.db.QueryRowContext(context.Background(), `SELECT typeof(work_unit_uuid), typeof(actor_uuid) FROM work_unit_changes`).Scan(&relationType, &actorType); err != nil {
		t.Fatal(err)
	}
	if relationType != "blob" || actorType != "blob" {
		t.Fatalf("relation types = %s/%s", relationType, actorType)
	}
}

func TestWorkUnitProjectionRejectsStaleAndUnauthorizedMutations(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	}()
	at := time.Now().UTC()
	unit := protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "one", State: protocol.WorkUnitProposed, CreatedBy: "01234567-89ab-4def-8123-456789abcdef", CreatedAt: at, UpdatedAt: at}
	actor := protocol.Actor{Address: unit.CreatedBy, SessionUUID: unit.CreatedBy, Harness: "pi", CWD: "/repo", State: "active", StartedAt: at, HeartbeatAt: at, RepositoryUUID: unit.RepositoryUUID, RepositoryRoot: "/repo", WorkspaceUUID: unit.WorkspaceUUID, WorkspaceRoot: "/repo", WorkspaceKind: "git"}
	if err := db.Project(event(t, 1, "actor.upserted", actor)); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 2, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
		t.Fatal(err)
	}
	member := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, JoinedAt: at, ParticipationState: "active"}
	if err := db.Project(event(t, 3, "work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, Result: member})); err != nil {
		t.Fatal(err)
	}
	updated := unit
	updated.Objective = "two"
	updated.UpdatedAt = at.Add(time.Second)
	if err := db.Project(event(t, 4, "work_unit.updated", protocol.WorkUnitUpdatedEvent{UUID: unit.UUID, Actor: unit.CreatedBy, Previous: unit, Result: updated})); err != nil {
		t.Fatal(err)
	}
	stale := updated
	stale.Objective = "stale"
	if err := db.Project(event(t, 5, "work_unit.updated", protocol.WorkUnitUpdatedEvent{UUID: unit.UUID, Actor: unit.CreatedBy, Previous: unit, Result: stale})); err == nil {
		t.Fatal("stale work unit update was projected")
	}
	unauthorized := updated
	unauthorized.Objective = "unauthorized"
	if err := db.Project(event(t, 5, "work_unit.updated", protocol.WorkUnitUpdatedEvent{UUID: unit.UUID, Actor: "31234567-89ab-4def-8123-456789abcdef", Previous: updated, Result: unauthorized})); err == nil {
		t.Fatal("nonparticipant work unit update was projected")
	}
	invalidTransition := protocol.WorkUnitTransitionEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, From: protocol.WorkUnitBlocked, To: protocol.WorkUnitVerified, At: at.Add(2 * time.Second)}
	if err := db.Project(event(t, 5, "work_unit.transitioned", invalidTransition)); err == nil {
		t.Fatal("transition with stale from state was projected")
	}
}
