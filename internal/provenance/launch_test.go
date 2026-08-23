package provenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // The projection acceptance test checks all UUID relations together.
func TestLaunchProjectionStoresNormalizedUUIDRelationsAsBlobs(t *testing.T) {
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
	unit := protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "objective", State: protocol.WorkUnitProposed, CreatedBy: "01234567-89ab-4def-8123-456789abcdef", CreatedAt: at, UpdatedAt: at}
	if err := db.Project(event(t, 1, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
		t.Fatal(err)
	}
	launch := protocol.Launch{UUID: "31234567-89ab-4def-8123-456789abcdef", ParentActors: []string{"01234567-89ab-4def-8123-456789abcdef", "11234567-89ab-4def-8123-456789abcdef"}, CreatedAt: at}
	if err := db.Project(event(t, 2, "launch.created", protocol.LaunchCreatedEvent{Launch: launch})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 3, "launch.child_attached", protocol.LaunchChildAttachedEvent{LaunchUUID: launch.UUID, ChildActor: "41234567-89ab-4def-8123-456789abcdef", At: at.Add(time.Second)})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 4, "launch.work_unit_attached", protocol.LaunchWorkUnitAttachedEvent{LaunchUUID: launch.UUID, WorkUnitUUID: unit.UUID, At: at.Add(2 * time.Second)})); err != nil {
		t.Fatal(err)
	}
	var launchLength, childLength, workUnitLength, parentLength, parents int
	if err := db.db.QueryRowContext(context.Background(), `SELECT length(launch_uuid), length(child_actor_uuid), length(work_unit_uuid) FROM launches`).Scan(&launchLength, &childLength, &workUnitLength); err != nil {
		t.Fatal(err)
	}
	if err := db.db.QueryRowContext(context.Background(), `SELECT length(actor_uuid), count(*) FROM launch_parent_actors`).Scan(&parentLength, &parents); err != nil {
		t.Fatal(err)
	}
	if launchLength != 16 || childLength != 16 || workUnitLength != 16 || parentLength != 16 || parents != 2 {
		t.Fatalf("launch UUID relation lengths/count = %d, %d, %d, %d, %d", launchLength, childLength, workUnitLength, parentLength, parents)
	}
}

func TestLaunchTerminationProjectsFailureReason(t *testing.T) {
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
	launch := protocol.Launch{UUID: "31234567-89ab-4def-8123-456789abcdef", CreatedAt: at}
	if err := db.Project(event(t, 1, "launch.created", protocol.LaunchCreatedEvent{Launch: launch})); err != nil {
		t.Fatal(err)
	}
	if err := db.Project(event(t, 2, "launch.terminated", protocol.LaunchTerminatedEvent{LaunchUUID: launch.UUID, Reason: "pi unavailable", At: at.Add(time.Second)})); err != nil {
		t.Fatal(err)
	}
	var reason string
	if err := db.db.QueryRowContext(context.Background(), `SELECT termination_reason FROM launches WHERE launch_uuid=?`, uuidBlob(launch.UUID)).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason != "pi unavailable" {
		t.Fatalf("termination reason = %q", reason)
	}
}
