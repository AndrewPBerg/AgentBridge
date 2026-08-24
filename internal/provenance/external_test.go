package provenance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // one integration test validates projection, relations, BLOBs, and continuity.
func TestExternalChangeProjectionUsesNormalizedUUIDRelations(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "external.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	repository := "11111111-1111-4111-8111-111111111111"
	workspace := "22222222-2222-4222-8222-222222222222"
	unknown := "33333333-3333-5333-8333-333333333333"
	id := "44444444-4444-4444-8444-444444444444"
	at := time.Now().UTC()
	change := protocol.ExternalChange{
		ID: id, Actor: unknown, UnknownActor: unknown, RepositoryUUID: repository, WorkspaceUUID: workspace,
		IntervalStartedAt: at, IntervalEndedAt: at.Add(time.Millisecond), ContinuityState: "current",
		ChangeKind: "modified", Path: "/repo/file.txt", WatchmanClock: "c:1:2",
		Before:           &protocol.FileSnapshot{Path: "/repo/file.txt", Exists: true, Kind: "file", SHA256: "before"},
		After:            &protocol.FileSnapshot{Path: "/repo/file.txt", Exists: true, Kind: "file", SHA256: "after"},
		RelatedIntentIDs: []string{"intent-1"},
	}
	if err := database.Project(event(t, 1, "external_change.observed", change)); err != nil {
		t.Fatal(err)
	}
	status := protocol.WatchContinuity{RepositoryUUID: repository, WorkspaceUUID: workspace, At: at, WatchmanClock: "c:1:2"}
	if err := database.Project(event(t, 2, "watch.continuity_restored", status)); err != nil {
		t.Fatal(err)
	}
	records, err := database.ListExternalChanges(workspace, 10)
	if err != nil || len(records) != 1 {
		t.Fatalf("external records = %#v, %v", records, err)
	}
	if records[0].ID != id || records[0].UnknownActor != unknown || records[0].Path != change.Path || len(records[0].RelatedIntentIDs) != 1 {
		t.Fatalf("external record = %#v", records[0])
	}
	var idLength, repositoryLength, workspaceLength, actorLength int
	if err := database.db.QueryRowContext(context.Background(), `SELECT length(external_change_uuid),length(repository_uuid),length(workspace_uuid),length(unknown_actor_uuid) FROM external_changes`).Scan(&idLength, &repositoryLength, &workspaceLength, &actorLength); err != nil {
		t.Fatal(err)
	}
	if idLength != 16 || repositoryLength != 16 || workspaceLength != 16 || actorLength != 16 {
		t.Fatalf("UUID lengths = %d/%d/%d/%d", idLength, repositoryLength, workspaceLength, actorLength)
	}
	var rawEventBytes, externalDataBytes int
	if err := database.db.QueryRowContext(context.Background(), `SELECT length(e.data), length(x.data) FROM events e JOIN external_changes x ON x.event_sequence=e.sequence WHERE e.sequence=1`).Scan(&rawEventBytes, &externalDataBytes); err != nil {
		t.Fatal(err)
	}
	if rawEventBytes != 0 || externalDataBytes != 0 {
		t.Fatalf("external payload duplicated in projection: events=%d external=%d", rawEventBytes, externalDataBytes)
	}
	continuity, err := database.WorkspaceContinuity(workspace)
	if err != nil || continuity.State != "restored" {
		t.Fatalf("continuity = %#v, %v", continuity, err)
	}
}
