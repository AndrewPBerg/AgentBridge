package provenance

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointProjectionRejectsInvalidUUIDs(t *testing.T) {
	base := protocol.CheckpointRequest{
		ID: "invalid", Actor: "01234567-89ab-4def-8123-456789abcdef",
		RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222",
		CheckpointKind: "settled",
	}
	for name, mutate := range map[string]func(*protocol.CheckpointRequest){
		"actor":      func(value *protocol.CheckpointRequest) { value.Actor = "actor" },
		"repository": func(value *protocol.CheckpointRequest) { value.RepositoryUUID = "repo" },
		"workspace":  func(value *protocol.CheckpointRequest) { value.WorkspaceUUID = "workspace" },
		"work unit":  func(value *protocol.CheckpointRequest) { value.WorkUnitUUID = "not-a-uuid" },
	} {
		t.Run(name, func(t *testing.T) {
			database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			checkpoint := base
			mutate(&checkpoint)
			if err := database.Project(event(t, 1, "checkpoint.requested", checkpoint)); err == nil {
				t.Fatalf("invalid checkpoint %s UUID was accepted", name)
			}
		})
	}
}

func TestCheckpointProjectionAcceptsFirstMultiResultRangeAndReplay(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "checkpoint-range.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	actor := "01234567-89ab-4def-8123-456789abcdef"
	repo := "11111111-1111-5111-8111-111111111111"
	workspace := "22222222-2222-5222-8222-222222222222"
	zero := 0
	events := []protocol.Event{
		event(t, 1, "actor.upserted", protocol.Actor{Address: actor, RepositoryUUID: repo, WorkspaceUUID: workspace, Generation: 1, State: "active", StartedAt: time.Now(), HeartbeatAt: time.Now()}),
		event(t, 2, "test.result", protocol.TestResult{ID: "range-result-1", Actor: actor, Command: "go test", CWD: "/repo", ExitCode: &zero, RepositoryUUID: repo, WorkspaceUUID: workspace}),
		event(t, 3, "test.result", protocol.TestResult{ID: "range-result-2", Actor: actor, Command: "go test", CWD: "/repo", ExitCode: &zero, RepositoryUUID: repo, WorkspaceUUID: workspace}),
		event(t, 4, "checkpoint.requested", protocol.CheckpointRequest{ID: "range-checkpoint", Actor: actor, SessionGeneration: 1, RepositoryUUID: repo, WorkspaceUUID: workspace, CheckpointKind: "settled", JournalStart: 2, JournalEnd: 3, TestResultIDs: []string{"range-result-1", "range-result-2"}}),
	}
	if err := database.ProjectAll(events); err != nil {
		t.Fatal(err)
	}
	if err := database.ProjectAll(events); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := database.Checkpoint("range-checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.JournalStart != 2 || checkpoint.JournalEnd != 3 || len(checkpoint.TestResultIDs) != 2 {
		t.Fatalf("projected checkpoint = %#v", checkpoint)
	}
}

func TestCheckpointProjectionStoresDeclarationAndSupportsWorkUnitQueries(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	checkpoint := protocol.CheckpointRequest{
		ID: "one", Actor: "01234567-89ab-4def-8123-456789abcdef", DeclaredBy: "human",
		SessionGeneration: 4, RepositoryUUID: "11111111-1111-5111-8111-111111111111", WorkspaceUUID: "22222222-2222-5222-8222-222222222222",
		WorkUnitUUID: "33333333-3333-5333-8333-333333333333", CheckpointKind: "settled", JournalStart: 10, JournalEnd: 12,
		BoundaryEventID: "boundary-1", BoundaryType: "explicit", TurnID: "turn-1", CompactionEventID: "compact-1",
		MutationIDs: []string{"mutation-1"}, MessageIDs: []string{"message-1"}, CollisionIDs: []string{"collision-1"}, TestResultIDs: []string{"test-1"},
		Metadata: map[string]string{"summary": "ready"}, Git: &protocol.GitContext{Head: "head-1"}, JJ: &protocol.JJContext{ChangeID: "change-1"},
	}
	at := time.Now().UTC()
	unit := protocol.WorkUnit{UUID: checkpoint.WorkUnitUUID, RepositoryUUID: checkpoint.RepositoryUUID, WorkspaceUUID: checkpoint.WorkspaceUUID, Objective: "objective", State: protocol.WorkUnitProposed, CreatedBy: checkpoint.Actor, CreatedAt: at, UpdatedAt: at}
	if err := database.Project(event(t, 11, "work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit})); err != nil {
		t.Fatal(err)
	}
	member := protocol.WorkUnitActor{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, JoinedAt: at, ParticipationState: "active"}
	if err := database.Project(event(t, 12, "work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: unit.UUID, Actor: unit.CreatedBy, Result: member})); err != nil {
		t.Fatal(err)
	}
	actorBlob := uuidBlob(checkpoint.Actor)
	if _, err := database.db.Exec(`INSERT INTO mutations(id, actor, session_generation, tool_call_id, tool, operation, cwd, workspace_key, paths_json, relative_paths_json, started_at, completed_at, before_json, after_json, updated_sequence, repository_uuid, workspace_uuid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, "mutation-1", actorBlob, 1, "call", "tool", "op", "/", "workspace", "[]", "[]", at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), "[]", "[]", 10, uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO messages(id, kind, from_actor, to_actor, body, global_sequence, sender_sequence, recipient_sequence, created_at, data, event_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, "message-1", "note", actorBlob, actorBlob, "body", 1, 1, 1, at.Format(time.RFC3339Nano), "{}", 11); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO collisions(id, path, state, created_at, updated_at, data, updated_sequence) VALUES (?, ?, ?, ?, ?, ?, ?)`, []byte("collision-1"), "/", "open", at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), "{}", 12); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO collision_actors(collision_id, ordinal, session_uuid) VALUES (?, ?, ?)`, []byte("collision-1"), 0, actorBlob); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.Exec(`INSERT INTO test_results(id, actor, session_generation, command, cwd, completed_at, started_at, output_truncated, repository_uuid, workspace_uuid, data, event_sequence) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, "test-1", actorBlob, 1, "go test", "/", at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), 0, uuidBlob(checkpoint.RepositoryUUID), uuidBlob(checkpoint.WorkspaceUUID), "{}", 10); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 13, "checkpoint.requested", checkpoint)); err != nil {
		t.Fatal(err)
	}
	if err := database.Project(event(t, 13, "checkpoint.requested", checkpoint)); err != nil {
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
	if got.ID != checkpoint.ID || got.Actor != checkpoint.Actor || got.DeclaredBy != "human" || got.WorkUnitUUID != checkpoint.WorkUnitUUID || got.JournalStart != 10 || got.JournalEnd != 12 ||
		got.BoundaryEventID != checkpoint.BoundaryEventID || got.TurnID != checkpoint.TurnID || got.CompactionEventID != checkpoint.CompactionEventID ||
		got.Git == nil || got.Git.Head != checkpoint.Git.Head || got.JJ == nil || got.JJ.ChangeID != checkpoint.JJ.ChangeID ||
		len(got.MutationIDs) != 1 || got.MutationIDs[0] != "mutation-1" || len(got.MessageIDs) != 1 || len(got.CollisionIDs) != 1 || len(got.TestResultIDs) != 1 || got.Metadata["summary"] != "ready" {
		t.Fatalf("checkpoint = %#v", got)
	}
	if status, err := database.Status(); err != nil || status.Checkpoints != 1 {
		t.Fatalf("status = %#v, err = %v", status, err)
	}
	var evidenceCount, metadataCount int
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM checkpoint_evidence WHERE checkpoint_id = ?`, checkpoint.ID).Scan(&evidenceCount); err != nil {
		t.Fatal(err)
	}
	if err := database.db.QueryRow(`SELECT COUNT(*) FROM checkpoint_metadata WHERE checkpoint_id = ?`, checkpoint.ID).Scan(&metadataCount); err != nil {
		t.Fatal(err)
	}
	if evidenceCount != 4 || metadataCount != 1 {
		t.Fatalf("relational checkpoint projection = evidence %d, metadata %d", evidenceCount, metadataCount)
	}

	turnIndex := 9
	conflictingCheckpoint := checkpoint
	conflictingCheckpoint.TurnIndex = &turnIndex
	if err := database.Project(event(t, 14, "checkpoint.requested", conflictingCheckpoint)); err == nil {
		t.Fatal("conflicting checkpoint payload was projected")
	}

	transaction, err := database.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	conflictingEvidence := checkpoint
	conflictingEvidence.MutationIDs = []string{"different-mutation"}
	if err := projectCheckpointEvidence(transaction, conflictingEvidence); err == nil {
		transaction.Rollback()
		t.Fatal("conflicting checkpoint evidence was projected")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}

	transaction, err = database.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	conflictingMetadata := checkpoint
	conflictingMetadata.Metadata = map[string]string{"summary": "different"}
	if err := projectCheckpointMetadata(transaction, conflictingMetadata); err == nil {
		transaction.Rollback()
		t.Fatal("conflicting checkpoint metadata was projected")
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatal(err)
	}
}
