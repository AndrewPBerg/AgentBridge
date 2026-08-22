package provenance

import (
	"path/filepath"
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointProjectionRejectsInvalidWorkUnitUUID(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	checkpoint := protocol.CheckpointRequest{ID: "checkpoint:invalid", Actor: "actor", RepositoryUUID: "repo", WorkspaceUUID: "workspace", WorkUnitUUID: "not-a-uuid", CheckpointKind: "settled"}
	if err := database.Project(event(t, 1, "checkpoint.requested", checkpoint)); err == nil {
		t.Fatal("invalid work unit UUID was accepted")
	}
}

func TestCheckpointProjectionStoresDeclarationAndSupportsWorkUnitQueries(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	checkpoint := protocol.CheckpointRequest{
		ID: "checkpoint:one", Actor: "01234567-89ab-cdef-0123-456789abcdef", DeclaredBy: "human",
		SessionGeneration: 4, RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222",
		WorkUnitUUID: "33333333-3333-5333-8333-333333333333", CheckpointKind: "settled", JournalStart: 10, JournalEnd: 12,
	}
	if err := database.Project(event(t, 12, "checkpoint.requested", checkpoint)); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 12, "checkpoint.requested", checkpoint)); err != nil {
		t.Fatal(err)
	}

	checkpoints, err := database.ListCheckpoints(checkpoint.WorkUnitUUID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 {
		t.Fatalf("checkpoints = %#v", checkpoints)
	}
	got := checkpoints[0]
	if got.ID != checkpoint.ID || got.Actor != checkpoint.Actor || got.DeclaredBy != "human" || got.WorkUnitUUID != checkpoint.WorkUnitUUID || got.JournalStart != 10 || got.JournalEnd != 12 {
		t.Fatalf("checkpoint = %#v", got)
	}
	if status, err := database.Status(); err != nil || status.Checkpoints != 1 {
		t.Fatalf("status = %#v, err = %v", status, err)
	}
}
