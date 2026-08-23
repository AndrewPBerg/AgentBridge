package provenance

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func openLeaseAcceptanceDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	return db
}

func seedLeaseActor(t *testing.T, db *DB, actor string, generation uint64, at time.Time) {
	t.Helper()
	_, err := db.db.ExecContext(context.Background(), `INSERT INTO actors(session_uuid,harness,cwd,state,generation,started_at,heartbeat_at,capabilities_json,updated_sequence) VALUES(?,?,?,?,?,?,?,?,?)`,
		uuidBlob(actor), "test", "/repo", "active", generation, at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), "{}", 1)
	if err != nil {
		t.Fatal(err)
	}
}

func seedLeaseIntent(t *testing.T, db *DB, actor, id, tool, repo, workspace string, at time.Time) {
	t.Helper()
	_, err := db.db.ExecContext(context.Background(), `INSERT INTO mutations(id,actor,session_generation,tool_call_id,tool,operation,cwd,repository_uuid,workspace_uuid,workspace_key,paths_json,relative_paths_json,before_json,after_json,started_at,updated_sequence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, uuidBlob(actor), 1, tool, "edit", "edit", "/repo", uuidBlob(repo), uuidBlob(workspace), "/repo", "[]", "[]", "{}", "{}", at.Format(time.RFC3339Nano), 1)
	if err != nil {
		t.Fatal(err)
	}
}

//nolint:unparam // all acceptance requests use generation one by design.
func leaseRequest(actor, lease, token, intent, tool, repo, workspace string, generation uint64, paths []string, now time.Time) *protocol.MutationLeaseRequest {
	return &protocol.MutationLeaseRequest{LeaseUUID: lease, FencingToken: token, ActorUUID: actor, Generation: generation, RepositoryUUID: repo, WorkspaceUUID: workspace, IntentID: intent, ToolCallID: tool, Paths: paths, Now: now}
}

//nolint:gocritic,unparam // fixture keeps acceptance setup readable and generation is intentionally fixed.
func leaseFixture(t *testing.T) (*DB, time.Time, string, string, string, string, string) {
	t.Helper()
	db := openLeaseAcceptanceDB(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a, b := actorUUID("lease-a"), actorUUID("lease-b")
	repo, workspace := actorUUID("repo"), actorUUID("workspace")
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseActor(t, db, b, 1, at)
	seedLeaseIntent(t, db, a, "intent-a", "tool-a", repo, workspace, at)
	seedLeaseIntent(t, db, b, "intent-b", "tool-b", repo, workspace, at)
	return db, at, a, b, repo, workspace, "intent-a"
}

func TestMutationLeaseExactPathGrantWaitAndSQLConflictRegression(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	first := leaseRequest(a, actorUUID("lease-1"), actorUUID("token-1"), intent, "tool-a", repo, workspace, 1, []string{"/repo/a"}, at)
	got, err := db.AcquireMutationLease(context.Background(), first)
	if err != nil || got.Decision != protocol.LeaseGrant {
		t.Fatalf("first acquire = %#v, %v", got, err)
	}
	second := leaseRequest(b, actorUUID("lease-2"), actorUUID("token-2"), "intent-b", "tool-b", repo, workspace, 1, []string{"/repo/a"}, at)
	got, err = db.AcquireMutationLease(context.Background(), second)
	if err != nil || got.Decision != protocol.LeaseWait || len(got.Conflicts) != 1 {
		t.Fatalf("conflicting acquire = %#v, %v", got, err)
	}
	third := second
	third.Paths = []string{"/repo/ab"}
	got, err = db.AcquireMutationLease(context.Background(), third)
	if err != nil || got.Decision != protocol.LeaseGrant {
		t.Fatalf("prefix-like path incorrectly conflicted = %#v, %v", got, err)
	}
}

//nolint:cyclop // end-to-end lineage acceptance scenario.
func TestMutationLeaseTakeoverLineageRootDepthAndOneActiveLeaf(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	rootReq := leaseRequest(a, actorUUID("lineage-a"), actorUUID("lineage-ta"), intent, "tool-a", repo, workspace, 1, []string{"/repo/a"}, at)
	root, err := db.AcquireMutationLease(context.Background(), rootReq)
	if err != nil {
		t.Fatal(err)
	}
	previous := root.Lease
	for i, actor := range []string{b, a, b} {
		succ := actorUUID("lineage-succ-" + string(rune('0'+i)))
		result, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: previous.LeaseUUID, LeaseUUID: succ, FencingToken: actorUUID("lineage-token-" + string(rune('0'+i))), RequesterActorUUID: actor, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: "handoff", Now: at.Add(time.Duration(i+1) * time.Second)})
		if err != nil {
			t.Fatal(err)
		}
		previous = &result.Lease
	}
	ancestry, err := db.MutationLeaseAncestry(context.Background(), previous.LeaseUUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ancestry.Leases) != 4 || ancestry.Leases[0].TakeoverDepth != 0 || ancestry.Leases[3].TakeoverDepth != 3 {
		t.Fatalf("ancestry = %#v", ancestry.Leases)
	}
	if ancestry.Leases[3].RootLeaseUUID != root.Lease.LeaseUUID {
		t.Fatalf("root = %q, want %q", ancestry.Leases[3].RootLeaseUUID, root.Lease.LeaseUUID)
	}
	var active int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM mutation_leases WHERE state='active'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("active leaves = %d, want 1", active)
	}
	if _, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: root.Lease.LeaseUUID, LeaseUUID: actorUUID("stale-ancestor"), FencingToken: actorUUID("stale-token"), RequesterActorUUID: a, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: "stale", Now: at.Add(10 * time.Second)}); err == nil {
		t.Fatal("stale ancestor takeover was accepted")
	}
	released, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: root.Lease.LeaseUUID, FencingToken: root.Lease.FencingToken, ActorUUID: a, Generation: 1})
	if err != nil || released {
		t.Fatalf("stale ancestor token release = %v, %v", released, err)
	}
}

func TestMutationLeaseConcurrentTakeoverExactlyOneWins(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("race-root"), actorUUID("race-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/a"}, at))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i, actor := range []string{a, b} {
		wg.Add(1)
		go func(i int, actor string) {
			defer wg.Done()
			_, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: root.Lease.LeaseUUID, LeaseUUID: actorUUID("race-succ-" + string(rune('0'+i))), FencingToken: actorUUID("race-succ-token-" + string(rune('0'+i))), RequesterActorUUID: actor, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: "race", Now: at.Add(time.Second)})
			results <- err
		}(i, actor)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("takeover wins = %d, want exactly one", wins)
	}
}

//nolint:cyclop // end-to-end lifecycle acceptance scenario.
func TestMutationLeaseValidationExpiryRenewalAndActorLiveness(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	for _, source := range []string{"operator", ""} {
		_, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: actorUUID("missing"), LeaseUUID: actorUUID("x"), FencingToken: actorUUID("y"), RequesterActorUUID: b, RequesterGeneration: 1, AcquisitionSource: source, Reason: "ok"})
		if err == nil {
			t.Fatalf("invalid source %q accepted", source)
		}
	}
	for _, reason := range []string{"", string(make([]byte, 1001))} {
		_, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: actorUUID("missing"), LeaseUUID: actorUUID("x"), FencingToken: actorUUID("y"), RequesterActorUUID: b, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: reason})
		if err == nil {
			t.Fatalf("invalid reason length %d accepted", len(reason))
		}
	}
	_, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("ttl"), actorUUID("ttl-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/ttl"}, at))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET heartbeat_at=? WHERE session_uuid=?`, at.Add(3*time.Minute).Format(time.RFC3339Nano), uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	got, err := db.AcquireMutationLease(context.Background(), leaseRequest(b, actorUUID("after-ttl"), actorUUID("after-ttl-token"), "intent-b", "tool-b", repo, workspace, 1, []string{"/repo/ttl"}, at.Add(3*time.Minute)))
	if err != nil || got.Decision != protocol.LeaseGrant {
		t.Fatalf("expired lease = %#v, %v", got, err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET heartbeat_at=? WHERE session_uuid=?`, at.Add(29*time.Minute).Format(time.RFC3339Nano), uuidBlob(a)); err != nil {
		t.Fatal(err)
	}
	blocked, err := db.RenewMutationLease(*leaseRequest(a, actorUUID("ttl"), actorUUID("ttl-token"), intent, "tool-a", repo, workspace, 1, nil, at.Add(31*time.Minute)))
	if err != nil || blocked.Decision != protocol.LeaseBlock {
		t.Fatalf("hard deadline renewal = %#v, %v", blocked, err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET state='dead' WHERE session_uuid=?`, uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	dead, err := db.AcquireMutationLease(context.Background(), leaseRequest(b, actorUUID("dead"), actorUUID("dead-token"), "intent-b", "tool-b", repo, workspace, 1, []string{"/repo/dead"}, at))
	if err != nil || dead.Decision != protocol.LeaseBlock {
		t.Fatalf("dead actor acquire = %#v, %v", dead, err)
	}
}

func TestMutationLeaseUUIDBLOBPersistenceAndCanonicalRebuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	a, repo, workspace := actorUUID("blob-a"), actorUUID("blob-repo"), actorUUID("blob-workspace")
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseIntent(t, db, a, "blob-intent", "blob-tool", repo, workspace, at)
	if _, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("blob-lease"), actorUUID("blob-token"), "blob-intent", "blob-tool", repo, workspace, 1, []string{"/repo/blob"}, at)); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"mutation_leases", "mutation_lease_paths", "mutation_lease_audit"} {
		var typ string
		if err := db.db.QueryRowContext(context.Background(), `SELECT type FROM pragma_table_info(?) WHERE name LIKE '%uuid%' LIMIT 1`, table).Scan(&typ); err != nil {
			t.Fatal(err)
		}
		if typ != "BLOB" {
			t.Fatalf("%s UUID column type = %s", table, typ)
		}
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE projection_meta SET version=?`, projectionSchemaVersion-1); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	var length int
	if err := db.db.QueryRowContext(context.Background(), `SELECT length(lease_uuid) FROM mutation_leases`).Scan(&length); err != nil {
		t.Fatal(err)
	}
	if length != 16 {
		t.Fatalf("persisted lease length = %d", length)
	}
}

// These two tests intentionally document currently absent invariants. Keep them
// failing until lease lifecycle mutations are journal-native and takeover sends
// a durable mailbox message rather than returning an in-memory notification.
func TestMutationLeaseLifecycleIsJournalNative(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	if _, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("journal-lease"), actorUUID("journal-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/journal"}, at)); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM events WHERE type LIKE 'lease.%'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("lease acquire was not recorded as a journal-native lifecycle event")
	}
}

func TestOldHolderTakeoverNotificationIsDurableMailboxMessage(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("mail-root"), actorUUID("mail-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/mail"}, at))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.TakeoverMutationLease(context.Background(), protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: root.Lease.LeaseUUID, LeaseUUID: actorUUID("mail-successor"), FencingToken: actorUUID("mail-successor-token"), RequesterActorUUID: b, RequesterGeneration: 1, AcquisitionSource: "human", Reason: "handoff", Now: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM messages WHERE to_actor=?`, uuidBlob(a)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Fatal("old-holder takeover notification is not a durable mailbox message")
	}
}
