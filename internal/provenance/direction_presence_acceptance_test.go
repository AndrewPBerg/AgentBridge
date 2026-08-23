package provenance

import (
	"context"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestStaleHeartbeatDirectionParticipantIsNotLive(t *testing.T) {
	db := openLeaseAcceptanceDB(t)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	actor := actorUUID("direction-stale-actor")
	direction := actorUUID("direction-stale")
	unit := actorUUID("unit-stale")
	repo := actorUUID("repo-stale")
	workspace := actorUUID("workspace-stale")
	if err := db.Project(event(t, 1, "actor.upserted", protocol.Actor{Address: actor, SessionUUID: actor, Harness: "test", CWD: "/repo", State: "active", Generation: 1, RepositoryUUID: repo, WorkspaceUUID: workspace, StartedAt: now, HeartbeatAt: now})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 2, "direction.created", protocol.DirectionCreatedEvent{Direction: protocol.Direction{UUID: direction, Objective: "stale", State: protocol.DirectionActive, CreatedBy: actor, CreatedAt: now, UpdatedAt: now}})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 3, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: protocol.WorkUnit{UUID: unit, DirectionUUID: direction, RepositoryUUID: repo, WorkspaceUUID: workspace, Objective: "unit", State: protocol.WorkUnitActive, CreatedBy: actor, CreatedAt: now, UpdatedAt: now}})); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `INSERT INTO work_unit_actors(work_unit_uuid,actor_uuid,joined_at,participation_state) VALUES(?,?,?,'active')`, uuidBlob(unit), uuidBlob(actor), now.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET heartbeat_at=? WHERE session_uuid=?`, now.Add(-time.Minute).Format(time.RFC3339Nano), uuidBlob(actor)); err != nil {
		t.Fatal(err)
	}
	status, err := db.DirectionStatus(direction)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Participants) != 1 || status.Participants[0].Live {
		t.Fatalf("stale participant = %#v, want Live=false", status.Participants)
	}
}
