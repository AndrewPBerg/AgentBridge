package provenance

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func event(t *testing.T, sequence uint64, eventType string, data any) protocol.Event {
	t.Helper()
	encoded, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Event{Version: 1, Sequence: sequence, Type: eventType, At: time.Unix(int64(sequence), 0).UTC(), Data: encoded}
}

func actorUUID(label string) string {
	d := sha256.Sum256([]byte(label))
	d[6] = (d[6] & 0x0f) | 0x40
	d[8] = (d[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", d[0:4], d[4:6], d[6:8], d[8:10], d[10:16])
}

type recordingAppender struct {
	events []protocol.Event
}

type blockingAppender struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingAppender) Append(protocol.Event) error {
	close(b.entered)
	<-b.release
	return nil
}

//nolint:gocritic // protocol.Appender requires a value event.
func (r *recordingAppender) Append(event protocol.Event) error {
	r.events = append(r.events, event)
	return nil
}

func TestUUIDProjectionUsesBinaryColumnsAndNormalizedCollisionActors(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	checks := map[string]string{"repositories": "id", "workspaces": "id", "actors": "session_uuid", "mutations": "actor", "session_events": "actor", "collisions": "id", "collision_actors": "collision_id", "messages": "from_actor", "test_results": "actor", "checkpoint_requests": "actor", "checkpoint_repository": "repository_uuid", "checkpoint_workspace": "workspace_uuid", "checkpoint_work_unit": "work_unit_uuid"}
	for table, column := range checks {
		lookupTable := table
		if strings.HasPrefix(table, "checkpoint_") {
			lookupTable = "checkpoint_requests"
		}
		var kind string
		if err := database.db.QueryRowContext(context.Background(), `SELECT type FROM pragma_table_info(?) WHERE name = ?`, lookupTable, column).Scan(&kind); err != nil {
			t.Fatalf("%s.%s: %v", table, column, err)
		}
		if kind != "BLOB" {
			t.Errorf("%s.%s has type %q, want BLOB", table, column, kind)
		}
	}
	var actorsJSON int
	if err := database.db.QueryRowContext(context.Background(), `SELECT count(*) FROM pragma_table_info('collisions') WHERE name = 'actors_json'`).Scan(&actorsJSON); err != nil {
		t.Fatal(err)
	}
	if actorsJSON != 0 {
		t.Fatal("collisions still has denormalized actors_json")
	}
}

func TestQueryWaitCannotMissConcurrentDurableAppendTail(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	primary := &blockingAppender{entered: make(chan struct{}), release: make(chan struct{})}
	projector := NewProjectingAppender(primary, database)
	defer projector.Close()
	actor := protocol.Actor{Address: actorUUID("race"), Harness: "pi", SessionUUID: actorUUID("race"), CWD: "/repo", State: "active", StartedAt: time.Now(), HeartbeatAt: time.Now()}
	projectionEvent := event(t, 1, "actor.upserted", actor)
	appendDone := make(chan error, 1)
	go func() { appendDone <- projector.Append(projectionEvent) }()
	<-primary.entered

	waitDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { waitDone <- projector.WaitForCurrent(ctx) }()
	select {
	case err := <-waitDone:
		t.Fatalf("query wait returned before durable append was published: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(primary.release)
	if err := <-appendDone; err != nil {
		t.Fatal(err)
	}
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	status, err := database.Status()
	if err != nil || status.ProjectedSequence != 1 {
		t.Fatalf("durable append was not visible after wait: %#v, %v", status, err)
	}
}

func TestAsyncProjectorBackpressuresWithoutSequenceGaps(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	primary := &recordingAppender{}
	projector := newProjectingAppender(primary, database, 1)
	for sequence := uint64(1); sequence <= 100; sequence++ {
		actor := protocol.Actor{
			Address: actorUUID("actor"), Harness: "pi", SessionUUID: actorUUID("actor"), CWD: "/repo", State: "active",
			StartedAt: time.Now(), HeartbeatAt: time.Now(),
		}
		if err := projector.Append(event(t, sequence, "actor.upserted", actor)); err != nil {
			t.Fatal(err)
		}
	}
	projector.Close()
	status, err := database.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Events != 100 || status.ProjectedSequence != 100 {
		t.Fatalf("projection gap: %#v", status)
	}
	health := projector.Health()
	if health.JournalSequence != 100 || health.ProjectedSequence != 100 || health.Lag != 0 {
		t.Fatalf("misleading projection health: %#v", health)
	}
}

func TestAsyncProjectorWaitsForReadYourWrites(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	projector := NewProjectingAppender(&recordingAppender{}, database)
	defer projector.Close()
	actor := protocol.Actor{Address: actorUUID("wait"), Harness: "pi", SessionUUID: actorUUID("wait"), CWD: "/repo", State: "active", StartedAt: time.Now(), HeartbeatAt: time.Now()}
	if err := projector.Append(event(t, 1, "actor.upserted", actor)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := projector.WaitForCurrent(ctx); err != nil {
		t.Fatal(err)
	}
	health := projector.Health()
	if health.Lag != 0 || health.JournalSequence != 1 || health.ProjectedSequence != 1 {
		t.Fatalf("projection not caught up: %#v", health)
	}
}

func TestProjectedActorIdentityIsCanonicalAndFixedWidth(t *testing.T) {
	valid := "01234567-89ab-4def-8123-456789abcdef"
	for _, input := range []string{valid, "pi:" + valid} {
		got, ok := normalizeProjectedActorUUID(input)
		if !ok || got != valid {
			t.Fatalf("normalizeProjectedActorUUID(%q) = %q, %v", input, got, ok)
		}
	}
	for _, input := range []string{"actor", "pi:actor", "pi:" + "00000000-0000-0000-8000-000000000000"} {
		if got, ok := normalizeProjectedActorUUID(input); ok || got != "" {
			t.Fatalf("normalizeProjectedActorUUID(%q) unexpectedly accepted %q", input, got)
		}
	}
}

func TestAsyncProjectorFlushesOnClose(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	primary := &recordingAppender{}
	projector := NewProjectingAppender(primary, database)
	actor := protocol.Actor{Address: actorUUID("async"), Harness: "pi", SessionUUID: actorUUID("async"), CWD: "/repo", State: "active", StartedAt: time.Now(), HeartbeatAt: time.Now()}
	if err := projector.Append(event(t, 1, "actor.upserted", actor)); err != nil {
		t.Fatal(err)
	}
	projector.Close()
	if len(primary.events) != 1 {
		t.Fatalf("primary events = %d", len(primary.events))
	}
	timeline, err := database.Timeline(actorUUID("async"), "", 10)
	if err != nil || len(timeline) != 1 {
		t.Fatalf("async projection was not flushed: %#v, %v", timeline, err)
	}
}

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestProjectionSchemaUpgradeResetsReadModelForJournalBackfill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-bridge.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	actor := protocol.Actor{Address: actorUUID("migrate"), Harness: "pi", SessionUUID: actorUUID("migrate"), CWD: "/repo", State: "active", StartedAt: now, HeartbeatAt: now}
	projectionEvent := event(t, 1, "actor.upserted", actor)
	if err := database.Project(projectionEvent); err != nil {
		t.Fatal(err)
	}
	if _, err := database.db.ExecContext(context.Background(), `UPDATE projection_meta SET version = ?`, projectionSchemaVersion-1); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	status, err := database.Status()
	if err != nil || status.Events != 0 {
		t.Fatalf("old projection was not reset: %#v, %v", status, err)
	}
	if err := database.Project(projectionEvent); err != nil {
		t.Fatal(err)
	}
	status, err = database.Status()
	if err != nil || status.Events != 1 || status.Actors != 1 {
		t.Fatalf("journal backfill did not rebuild projection: %#v, %v", status, err)
	}
}

func TestCanonicalActorAddressCannotBeShadowedByAlias(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	now := time.Now()
	canonicalActor := protocol.Actor{Address: actorUUID("real"), Harness: "pi", SessionUUID: actorUUID("real"), CWD: "/repo", State: "active", StartedAt: now, HeartbeatAt: now}
	shadow := protocol.Actor{Address: actorUUID("other"), Harness: "pi", SessionUUID: actorUUID("other"), Alias: "real", CWD: "/repo", State: "active", StartedAt: now, HeartbeatAt: now}
	if err := database.ProjectAll([]protocol.Event{
		event(t, 1, "actor.upserted", canonicalActor),
		event(t, 2, "actor.upserted", shadow),
	}); err != nil {
		t.Fatal(err)
	}
	resolved, err := database.ResolveActor(actorUUID("real"))
	if err != nil || resolved != actorUUID("real") {
		t.Fatalf("canonical address resolved to %q, %v", resolved, err)
	}
}

func TestPersistedAliasRequiresScopeWhenAmbiguous(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	now := time.Now()
	left := protocol.Actor{
		Address: actorUUID("left"), Harness: "pi", SessionUUID: actorUUID("left"), Alias: "builder", CWD: "/repo", State: "active",
		RepositoryUUID: "1", RepositoryRoot: "/repo", WorkspaceUUID: "left", WorkspaceRoot: "/repo", WorkspaceKind: "git-worktree",
		StartedAt: now, HeartbeatAt: now,
	}
	right := protocol.Actor{
		Address: actorUUID("right"), Harness: "pi", SessionUUID: actorUUID("right"), Alias: "builder", CWD: "/repo-right", State: "active",
		RepositoryUUID: "1", RepositoryRoot: "/repo", WorkspaceUUID: "right", WorkspaceRoot: "/repo-right", WorkspaceKind: "git-worktree",
		StartedAt: now, HeartbeatAt: now,
	}
	if err := database.ProjectAll([]protocol.Event{
		event(t, 1, "actor.upserted", left), event(t, 2, "actor.upserted", right),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.ResolveActor("@builder"); err == nil {
		t.Fatal("ambiguous unscoped alias unexpectedly resolved")
	}
	resolved, err := database.ResolveActorScoped("@builder", "", left.WorkspaceUUID)
	if err != nil || resolved != left.Address {
		t.Fatalf("workspace-scoped alias = %q, %v", resolved, err)
	}
	if _, err := database.AgentSummary("@builder", 10); err == nil {
		t.Fatal("unscoped agent summary unexpectedly accepted ambiguous alias")
	}
	summary, err := database.AgentSummary("@builder", 10, ActorScope{WorkspaceUUID: left.WorkspaceUUID})
	if err != nil || summary.Actor != left.Address {
		t.Fatalf("scoped agent summary = %#v, %v", summary, err)
	}
}

func TestMutationOrderingUsesStableSequenceTieBreaker(t *testing.T) {
	database, err := Open(filepath.Join(t.TempDir(), "agent-bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	now := time.Now().UTC()
	actor := protocol.Actor{Address: actorUUID("order"), Harness: "pi", SessionUUID: actorUUID("order"), CWD: "/repo", State: "active", StartedAt: now, HeartbeatAt: now}
	intent := func(id string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor.Address, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo", StartedAt: now, ExpiresAt: now.Add(time.Minute)}
	}
	if err := database.ProjectAll([]protocol.Event{
		event(t, 1, "actor.upserted", actor), event(t, 2, "intent.started", intent("first")), event(t, 3, "intent.started", intent("second")),
	}); err != nil {
		t.Fatal(err)
	}
	mutations, err := database.ListMutations(MutationFilter{Actor: actor.Address, Limit: 2})
	if err != nil || len(mutations) != 2 || mutations[0].ID != "second" || mutations[1].ID != "first" {
		t.Fatalf("unstable mutation order: %#v, %v", mutations, err)
	}
}

func TestTursoProjectionAndQueries(t *testing.T) {
	database, actor, path := setupTursoProjectionFixture(t)
	defer func() {
		if err := database.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	assertTursoProjectionQueries(t, database, &actor, path)
}

func setupTursoProjectionFixture(t *testing.T) (*DB, protocol.Actor, string) {
	path := filepath.Join(t.TempDir(), ".agent-bridge", "agent-bridge.db")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 21, 4, 0, 0, 0, time.UTC)
	actor := protocol.Actor{
		Address: actorUUID("session"), Harness: "pi", SessionUUID: actorUUID("session"), Alias: "walkie", CWD: "/repo", State: "active",
		RepositoryUUID: "test", RepositoryRoot: "/repo", WorkspaceUUID: "test", WorkspaceRoot: "/repo", WorkspaceKind: "git-jj-workspace",
		Generation: 7, StartedAt: now, HeartbeatAt: now,
		Git: &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: "/repo", Branch: "main", Head: "abcdef"},
		JJ:  &protocol.JJContext{WorkspaceRoot: "/repo", ChangeID: "qpvuntsm"},
	}
	started := protocol.Intent{
		ID: "intent-1", Actor: actor.Address, SessionGeneration: 7, TurnID: "session:7:turn:3", TurnIndex: func() *int { value := 3; return &value }(),
		ToolCallID: "tool-1", Tool: "edit", Operation: "edit",
		Paths: []string{"/repo/schema.ts"}, RelativePaths: []string{"schema.ts"}, CWD: "/repo",
		RepositoryUUID: actor.RepositoryUUID, RepositoryRoot: actor.RepositoryRoot, WorkspaceUUID: actor.WorkspaceUUID,
		WorkspaceRoot: actor.WorkspaceRoot, WorkspaceKind: actor.WorkspaceKind, WorkspaceKey: "/repo", Git: actor.Git, JJ: actor.JJ,
		Context:   protocol.IntentContext{AssistantExcerpt: "Updating the schema"},
		Before:    []protocol.FileSnapshot{{Path: "/repo/schema.ts", Exists: true, Kind: "file", SHA256: "before", Size: 10}},
		StartedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	}
	completed := started
	success := true
	completed.Success = &success
	completedAt := now.Add(time.Second)
	completed.CompletedAt = &completedAt
	completed.After = []protocol.FileSnapshot{{Path: "/repo/schema.ts", Exists: true, Kind: "file", SHA256: "after", Size: 11}}
	completed.GitAfter = actor.Git
	completed.JJAfter = actor.JJ
	sessionEvent := protocol.SessionEvent{
		ID: "session-event-1", Actor: actor.Address, SessionGeneration: 7, Type: "session.compacted", At: now.Add(2 * time.Second),
		Summary: "Compacted schema work", Data: json.RawMessage(`{"reason":"threshold"}`),
	}

	collision := protocol.Collision{
		ID: "collision-1", Path: "/repo/schema.ts", State: protocol.CollisionResolved,
		Actors: [2]string{"other", actor.Address}, CreatedAt: now, UpdatedAt: now.Add(3 * time.Second), Resolution: "walkie owned schema",
	}
	events := []protocol.Event{
		event(t, 1, "actor.upserted", actor),
		event(t, 2, "intent.started", started),
		event(t, 3, "intent.completed", completed),
		event(t, 4, "session.event", sessionEvent),
		event(t, 5, "collision.upserted", collision),
		event(t, 6, "collision.transitioned", protocol.CollisionTransitionEvent{
			CollisionID: collision.ID, Actor: actor.Address, From: protocol.CollisionResolved, To: protocol.CollisionResolved,
			Resolution: "walkie owns schema", At: now.Add(4 * time.Second),
		}),
	}
	if err := database.ProjectAll(events); err != nil {
		t.Fatal(err)
	}
	if err := database.ProjectAll(events); err != nil {
		t.Fatalf("idempotent replay failed: %v", err)
	}

	return database, actor, path
}

func assertTursoProjectionQueries(t *testing.T, database *DB, actor *protocol.Actor, path string) {
	assertTursoStatusAndScopes(t, database, actor)
	assertTursoMutationQueries(t, database, actor)
	assertTursoActorQueries(t, database)
	assertTursoFileMode(t, path)
}

func assertTursoStatusAndScopes(t *testing.T, database *DB, actor *protocol.Actor) {
	status, err := database.Status()
	if err != nil {
		t.Fatal(err)
	}
	if status.Events != 6 || status.Actors != 1 || status.Repositories != 1 || status.Workspaces != 1 || status.Mutations != 1 || status.SessionEvents != 1 || status.Collisions != 1 {
		t.Fatalf("status = %#v", status)
	}
	assertTursoScopes(t, database, actor)
}

func assertTursoScopes(t *testing.T, database *DB, actor *protocol.Actor) {
	scopes, err := database.Scopes()
	if err != nil || len(scopes.Repositories) != 1 || len(scopes.Workspaces) != 1 || scopes.Workspaces[0].ID != actor.WorkspaceUUID {
		t.Fatalf("scopes = %#v, %v", scopes, err)
	}
}

func assertTursoMutationQueries(t *testing.T, database *DB, actor *protocol.Actor) {
	mutations, err := database.ListMutations(MutationFilter{Actor: "@walkie", Path: "/repo/schema.ts"})
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) != 1 || mutations[0].Success == nil || !*mutations[0].Success || mutations[0].TurnID != "session:7:turn:3" {
		t.Fatalf("mutations = %#v", mutations)
	}
	if string(mutations[0].Before) == "" || string(mutations[0].After) == "" {
		t.Fatalf("missing snapshots: %#v", mutations[0])
	}

	assertTursoMutationExplanation(t, database, actor)
}

func assertTursoMutationExplanation(t *testing.T, database *DB, actor *protocol.Actor) {
	explained, err := database.Mutation("intent-1")
	if err != nil || explained.AssistantExcerpt != "Updating the schema" {
		t.Fatalf("explain = %#v, %v", explained, err)
	}
	timeline, err := database.Timeline("@walkie", "", 10)
	if err != nil || len(timeline) != 5 {
		t.Fatalf("timeline = %#v, %v", timeline, err)
	}
	sessionEvents, err := database.SessionEvents("@walkie", 10)
	if err != nil || len(sessionEvents) != 1 || sessionEvents[0].Summary != "Compacted schema work" {
		t.Fatalf("session events = %#v, %v", sessionEvents, err)
	}

	assertTursoMutationExplanations(t, database, actor)
}

func assertTursoMutationExplanations(t *testing.T, database *DB, actor *protocol.Actor) {
	who, err := database.WhoChanged("/repo/schema.ts", 10)
	if err != nil || len(who.Mutations) != 1 || len(who.Collisions) != 1 {
		t.Fatalf("who changed = %#v, %v", who, err)
	}
	if who.Collisions[0].State != string(protocol.CollisionResolved) || who.Collisions[0].Resolution != "walkie owns schema" || who.Collisions[0].ResolvedBy != actor.Address {
		t.Fatalf("projected collision transition = %#v", who.Collisions[0])
	}
	why, err := database.Why("intent-1", 10)
	if err != nil || why.Mutation.TurnID != "session:7:turn:3" || len(why.Collisions) != 1 {
		t.Fatalf("why = %#v, %v", why, err)
	}
}

func assertTursoActorQueries(t *testing.T, database *DB) {
	agent, err := database.AgentSummary("@walkie", 10)
	if err != nil || len(agent.Mutations) != 1 || len(agent.SessionEvents) != 1 {
		t.Fatalf("agent summary = %#v, %v", agent, err)
	}
	since, err := database.SinceCompaction("@walkie", 10)
	if err != nil || since.Compaction == nil || since.Compaction.Summary != "Compacted schema work" {
		t.Fatalf("since compaction = %#v, %v", since, err)
	}
}

func assertTursoFileMode(t *testing.T, path string) {
	files, err := filepath.Glob(path + "*")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		info, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o600 {
			t.Fatalf("database file %s mode = %o", file, info.Mode().Perm())
		}
	}
}
