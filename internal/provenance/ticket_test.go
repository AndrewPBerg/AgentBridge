package provenance

import (
	"path/filepath"
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointTicketProjectionIsImmutable(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	base := protocol.CheckpointRequest{
		ID: "ticket-checkpoint", Actor: "01234567-89ab-4def-8123-456789abcdef",
		RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222",
		CheckpointKind: "settled", Tickets: protocol.Tickets(`[{"key":"AB-1"}]`),
	}
	first := event(t, 1, "checkpoint.requested", base)
	if err := db.Project(first); err != nil {
		t.Fatal(err)
	}
	got, err := db.Checkpoint(base.ID)
	if err != nil || got.Tickets != base.Tickets {
		t.Fatalf("checkpoint tickets = %s, err %v", got.Tickets, err)
	}
	changed := base
	changed.Tickets = protocol.Tickets(`[{"key":"AB-2"}]`)
	if err := db.Project(event(t, 2, "checkpoint.requested", changed)); err == nil {
		t.Fatal("changed checkpoint tickets were projected over immutable record")
	}
	got, err = db.Checkpoint(base.ID)
	if err != nil || got.Tickets != base.Tickets {
		t.Fatalf("checkpoint tickets changed after conflict = %s, err %v", got.Tickets, err)
	}
}
