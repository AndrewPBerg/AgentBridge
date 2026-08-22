package provenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestDirectionTransitionProjectsReplaySafely(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	at := time.Unix(100, 0).UTC()
	direction := protocol.Direction{UUID: "53e7929c-193b-4440-b309-a4eb2be8bf9c", Objective: "Coordinate", State: protocol.DirectionActive, CreatedBy: "11111111-1111-4111-8111-111111111111", CreatedAt: at, UpdatedAt: at}
	if err := database.Project(event(t, 1, "direction.created", protocol.DirectionCreatedEvent{Direction: direction})); err != nil {
		t.Fatal(err)
	}
	transition := protocol.DirectionTransitionEvent{DirectionUUID: direction.UUID, Actor: direction.CreatedBy, From: protocol.DirectionActive, To: protocol.DirectionConverging, At: at.Add(time.Second)}
	transitionEvent := event(t, 2, "direction.transitioned", transition)
	if err := database.Project(transitionEvent); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(transitionEvent); err != nil {
		t.Fatal(err)
	}
	got, err := database.Direction(direction.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != protocol.DirectionConverging || !got.UpdatedAt.Equal(transition.At) {
		t.Fatalf("direction after transition = %#v", got)
	}
}

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestDirectionProjectionAndQuery(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()

	direction := protocol.Direction{
		UUID:            "53e7929c-193b-4440-b309-a4eb2be8bf9c",
		Objective:       "Coordinate the implementation",
		SuccessCriteria: "The projection is queryable",
		Constraints:     "No direction actors yet",
		Context:         "provenance",
		State:           protocol.DirectionActive,
		CreatedBy:       "11111111-1111-4111-8111-111111111111",
		CreatedAt:       time.Unix(100, 0).UTC(),
		UpdatedAt:       time.Unix(100, 0).UTC(),
	}
	e := event(t, 1, "direction.created", protocol.DirectionCreatedEvent{Direction: direction})
	if err := database.Project(e); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(e); err != nil {
		t.Fatal(err)
	}

	var directionType, creatorType string
	if err := database.db.QueryRowContext(context.Background(), `SELECT type FROM pragma_table_info('directions') WHERE name = 'direction_uuid'`).Scan(&directionType); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRowContext(context.Background(), `SELECT type FROM pragma_table_info('directions') WHERE name = 'created_by'`).Scan(&creatorType); err != nil {
		t.Fatal(err)
	}
	if directionType != "BLOB" || creatorType != "BLOB" {
		t.Fatalf("directions UUID columns = %q, %q; want BLOB, BLOB", directionType, creatorType)
	}

	got, err := database.Direction(direction.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if got.UUID != direction.UUID || got.CreatedBy != direction.CreatedBy || got.Objective != direction.Objective || got.State != direction.State {
		t.Fatalf("queried direction = %#v, want %#v", got, direction)
	}
	if _, err := database.Direction("22222222-2222-4222-8222-222222222222"); err == nil {
		t.Fatal("unknown direction query succeeded")
	}
}

//nolint:cyclop // end-to-end test keeps projection, scope fixtures, and rollup assertions together.
func TestDirectionStatusRollsUpAttachedWorkUnitsDeterministically(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	at := time.Unix(100, 0).UTC()
	direction := protocol.Direction{UUID: "53e7929c-193b-4440-b309-a4eb2be8bf9c", Objective: "Coordinate", State: protocol.DirectionActive, CreatedBy: "11111111-1111-4111-8111-111111111111", CreatedAt: at, UpdatedAt: at}
	if err := database.Project(event(t, 1, "direction.created", protocol.DirectionCreatedEvent{Direction: direction})); err != nil {
		t.Fatal(err)
	}
	units := []protocol.WorkUnit{
		{UUID: "21234567-89ab-4def-8123-456789abcdef", DirectionUUID: direction.UUID, RepositoryUUID: "31234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "41234567-89ab-4def-8123-456789abcdef", Objective: "later repository", State: protocol.WorkUnitProposed, CreatedBy: direction.CreatedBy, CreatedAt: at, UpdatedAt: at},
		{UUID: "51234567-89ab-4def-8123-456789abcdef", DirectionUUID: direction.UUID, RepositoryUUID: "01234567-89ab-4def-8123-456789abcdef", WorkspaceUUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "earlier repository", State: protocol.WorkUnitVerified, CreatedBy: direction.CreatedBy, CreatedAt: at, UpdatedAt: at},
	}
	for i, unit := range units {
		if err := database.Project(event(t, uint64(i+2), "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.db.ExecContext(context.Background(), `INSERT INTO repositories(id, root, kind, updated_sequence) VALUES (?, ?, ?, ?), (?, ?, ?, ?)`,
		uuidBlob(units[0].RepositoryUUID), "/project/child", "git", 4,
		uuidBlob(units[1].RepositoryUUID), "/project", "directory", 4); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(context.Background(), `INSERT INTO workspaces(id, repository_uuid, root, kind, updated_sequence) VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)`,
		uuidBlob(units[0].WorkspaceUUID), uuidBlob(units[0].RepositoryUUID), "/project/child", "git-worktree", 4,
		uuidBlob(units[1].WorkspaceUUID), uuidBlob(units[1].RepositoryUUID), "/project", "directory", 4); err != nil {
		t.Fatal(err)
	}
	status, err := database.DirectionStatus(direction.UUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.WorkUnits) != 2 || status.WorkUnits[0].Objective != "earlier repository" || status.WorkUnits[1].Objective != "later repository" {
		t.Fatalf("work unit order = %#v", status.WorkUnits)
	}
	if got := status.WorkUnits[0]; got.RepositoryRoot != "/project" || got.WorkspaceRoot != "/project" || got.WorkspaceKind != "directory" {
		t.Fatalf("parent-directory scope = %#v", got)
	}
	if got := status.WorkUnits[1]; got.RepositoryRoot != "/project/child" || got.WorkspaceRoot != "/project/child" || got.WorkspaceKind != "git-worktree" {
		t.Fatalf("child Git scope = %#v", got)
	}
	if status.Readiness.Ready || len(status.Readiness.BlockingReasons) != 1 {
		t.Fatalf("readiness = %#v", status.Readiness)
	}
}
