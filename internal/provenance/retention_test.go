package provenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // Acceptance test keeps retention setup and all boundary assertions together.
func TestPruneExternalChangesBeforeRemovesOnlyExpiredFilesystemEvidence(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "retention.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	repository := actorUUID("retention-repository")
	workspace := actorUUID("retention-workspace")
	unknown := actorUUID("retention-unknown")
	change := func(id string, at time.Time) protocol.ExternalChange {
		path := "/repo/file.txt"
		return protocol.ExternalChange{
			ID: id, Actor: unknown, UnknownActor: unknown, RepositoryUUID: repository, WorkspaceUUID: workspace,
			IntervalStartedAt: at, IntervalEndedAt: at, ContinuityState: "current", ChangeKind: "modified", Path: path,
			Before:           &protocol.FileSnapshot{Path: path, Exists: true, Kind: "file", SHA256: "before"},
			After:            &protocol.FileSnapshot{Path: path, Exists: true, Kind: "file", SHA256: "after"},
			RelatedIntentIDs: []string{"intent"},
		}
	}
	oldID, currentID := actorUUID("retention-old"), actorUUID("retention-current")
	if err := database.Project(event(t, 1, "external_change.observed", change(oldID, now.Add(-15*24*time.Hour)))); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 2, "external_change.observed", change(currentID, now.Add(-13*24*time.Hour)))); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 3, "retained.audit", map[string]string{"keep": "true"})); err != nil {
		t.Fatal(err)
	}

	result, err := database.PruneExternalChangesBefore(context.Background(), now.Add(-14*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if result.ExternalChanges != 1 || result.RawEvents != 1 {
		t.Fatalf("retention result = %#v", result)
	}
	for table, want := range map[string]int{"external_changes": 1, "external_change_paths": 1, "external_change_intents": 1, "events": 2} {
		var count int
		if err := database.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Errorf("%s count = %d, want %d", table, count, want)
		}
	}
	var retained []byte
	if err := database.db.QueryRowContext(context.Background(), `SELECT external_change_uuid FROM external_changes`).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if uuidString(retained) != currentID {
		t.Fatalf("retained external change = %s, want %s", uuidString(retained), currentID)
	}
	second, err := database.PruneExternalChangesBefore(context.Background(), now.Add(-14*24*time.Hour))
	if err != nil || second.ExternalChanges != 0 || second.RawEvents != 0 {
		t.Fatalf("idempotent retention = %#v, %v", second, err)
	}
}
