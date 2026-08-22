package provenance

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestWorkUnitActorLeaveProjectsAndReplays(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	at := time.Now().UTC()
	unit := protocol.WorkUnit{
		UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef",
		WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "leave", State: protocol.WorkUnitProposed,
		CreatedBy: "01234567-89ab-4def-8123-456789abcdef", CreatedAt: at, UpdatedAt: at,
	}
	if err := database.Project(event(t, 1, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
		t.Fatal(err)
	}
	active := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, JoinedAt: at, ParticipationState: "active"}
	if err := database.Project(event(t, 2, "work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, At: at, Result: active})); err != nil {
		t.Fatal(err)
	}
	leftAt := at.Add(time.Second)
	left := active
	left.LeftAt = &leftAt
	left.ParticipationState = "left"
	leave := protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, At: leftAt, Previous: &active, Result: left}
	if err := database.Project(event(t, 3, "work_unit.actor_left", leave)); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 3, "work_unit.actor_left", leave)); err != nil {
		t.Fatalf("idempotent leave replay failed: %v", err)
	}
	got, err := database.WorkUnit(unit.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Participants) != 1 || got.Participants[0].ParticipationState != "left" || got.Participants[0].LeftAt == nil || !got.Participants[0].LeftAt.Equal(leftAt) {
		t.Fatalf("projected participant = %#v", got.Participants)
	}
	conflict := leave
	conflict.Result.ParticipationState = "active"
	if err := database.Project(event(t, 4, "work_unit.actor_left", conflict)); err == nil {
		t.Fatal("conflicting leave event was accepted")
	}
	unknown := leave
	unknown.Actor = "31234567-89ab-4def-8123-456789abcdef"
	unknown.Result.Actor = unknown.Actor
	unknown.Previous = nil
	if err := database.Project(event(t, 4, "work_unit.actor_left", unknown)); err == nil {
		t.Fatal("leave for unknown membership was accepted")
	}
}
