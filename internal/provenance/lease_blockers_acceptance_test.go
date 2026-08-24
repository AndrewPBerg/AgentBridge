package provenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
	"github.com/AndrewPBerg/agent-bridge/internal/store"
)

// leaseTestAppender is deliberately small: it lets acceptance tests fail the
// authoritative append without changing the production journal implementation.
type leaseTestAppender struct {
	mu     sync.Mutex
	next   uint64
	fail   bool
	events []protocol.Event
}

func (a *leaseTestAppender) Append(event protocol.Event) error { //nolint:gocritic // protocol.Appender requires value semantics.
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return errors.New("simulated lost journal append")
	}
	a.events = append(a.events, event)
	return nil
}

func (a *leaseTestAppender) AppendNext(event protocol.Event) (protocol.Event, error) { //nolint:gocritic // protocol.Appender requires value semantics.
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fail {
		return protocol.Event{}, errors.New("simulated lost journal append")
	}
	a.next++
	event.Sequence = a.next
	a.events = append(a.events, event)
	return event, nil
}

func takeoverRequest(pred, succ, token, actor string, at time.Time) protocol.MutationLeaseTakeoverRequest {
	return protocol.MutationLeaseTakeoverRequest{
		PredecessorLeaseUUID: pred, LeaseUUID: succ, FencingToken: token,
		RequesterActorUUID: actor, RequesterGeneration: 1,
		AcquisitionSource: "agent", Reason: "acceptance handoff", Now: at,
	}
}

func TestAcquireAppendFailureLeavesNoLeaseProjection(t *testing.T) {
	db, at, actor, _, repo, workspace, intent := leaseFixture(t)
	appender := &leaseTestAppender{fail: true}
	db.SetLeaseAppender(appender, 0)
	leaseUUID := actorUUID("failed-acquire")
	_, err := db.AcquireMutationLease(context.Background(), leaseRequest(actor, leaseUUID, actorUUID("failed-acquire-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/failed-acquire"}, at))
	if err == nil {
		t.Fatal("acquire unexpectedly succeeded with a failed journal append")
	}
	var leases int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(leaseUUID)).Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatalf("lease projection rows after failed append = %d", leases)
	}
}

func TestTakeoverAppendFailureLeavesPredecessorActiveAndNoSuccessor(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("append-root"), actorUUID("append-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/append"}, at))
	if err != nil {
		t.Fatal(err)
	}
	appender := &leaseTestAppender{}
	db.SetLeaseAppender(appender, 0)
	appender.mu.Lock()
	appender.fail = true
	appender.mu.Unlock()
	successor := actorUUID("append-successor")
	if _, err := db.TakeoverMutationLease(context.Background(), takeoverRequest(root.Lease.LeaseUUID, successor, actorUUID("append-successor-token"), b, at.Add(time.Second))); err == nil {
		t.Fatal("takeover unexpectedly succeeded with a failed journal append")
	}
	var leaseState string
	if err := db.db.QueryRowContext(context.Background(), `SELECT state FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(root.Lease.LeaseUUID)).Scan(&leaseState); err != nil {
		t.Fatal(err)
	}
	if leaseState != string(protocol.LeaseActive) {
		t.Fatalf("predecessor state = %q, want active", leaseState)
	}
	var successors int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(successor)).Scan(&successors); err != nil {
		t.Fatal(err)
	}
	if successors != 0 {
		t.Fatalf("successor rows after failed append = %d", successors)
	}
}

//nolint:cyclop,gocognit,funlen // End-to-end restart coverage intentionally combines projection and engine replay.
func TestOneTakeoverEventRebuildsLineageAndCanonicalMessageAndEnginePollsAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db, err := OpenProjection(path)
	if err != nil {
		t.Fatal(err)
	}
	appender := &leaseTestAppender{}
	db.SetLeaseAppender(appender, 0)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a, b, repo, workspace := actorUUID("restart-a"), actorUUID("restart-b"), actorUUID("restart-repo"), actorUUID("restart-workspace")
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseActor(t, db, b, 1, at)
	seedLeaseIntent(t, db, a, "restart-intent", "restart-tool", repo, workspace, at)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("restart-root"), actorUUID("restart-root-token"), "restart-intent", "restart-tool", repo, workspace, 1, []string{"/repo/restart"}, at))
	if err != nil {
		t.Fatal(err)
	}
	appender.mu.Lock()
	rootEvent := appender.events[len(appender.events)-1]
	appender.mu.Unlock()
	if err := db.Project(rootEvent); err != nil {
		t.Fatal(err)
	}
	successor := actorUUID("restart-successor")
	if _, err := db.TakeoverMutationLease(context.Background(), takeoverRequest(root.Lease.LeaseUUID, successor, actorUUID("restart-successor-token"), b, at.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	appender.mu.Lock()
	events := append([]protocol.Event(nil), appender.events...)
	appender.mu.Unlock()
	var takeovers int
	for _, event := range events {
		if event.Type == "lease.takeover" {
			takeovers++
		}
	}
	if takeovers != 1 {
		t.Fatalf("takeover journal events = %d, want one", takeovers)
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	rebuilt, err := OpenProjection(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Errorf("close rebuilt database: %v", err)
		}
	})
	replayEvents := make([]protocol.Event, 0, 2+len(events))
	replayEvents = append(replayEvents,

		event(t, 1, "actor.upserted", protocol.Actor{Address: a, SessionUUID: a, Harness: "test", CWD: "/repo", State: "active", RepositoryUUID: repo, RepositoryRoot: "/repo", WorkspaceUUID: workspace, WorkspaceRoot: "/repo", WorkspaceKind: "git", StartedAt: at, HeartbeatAt: at}),
		event(t, 2, "actor.upserted", protocol.Actor{Address: b, SessionUUID: b, Harness: "test", CWD: "/repo", State: "active", RepositoryUUID: repo, RepositoryRoot: "/repo", WorkspaceUUID: workspace, WorkspaceRoot: "/repo", WorkspaceKind: "git", StartedAt: at, HeartbeatAt: at}),
	)
	for _, lifecycle := range events {
		lifecycle.Sequence += 2
		replayEvents = append(replayEvents, lifecycle)
	}
	if err := rebuilt.ProjectAll(replayEvents); err != nil {
		t.Fatal(err)
	}
	ancestry, err := rebuilt.MutationLeaseAncestry(context.Background(), successor)
	if err != nil || len(ancestry.Leases) != 2 || ancestry.Leases[0].State != protocol.LeaseSuperseded || ancestry.Leases[1].State != protocol.LeaseActive {
		t.Fatalf("rebuilt lineage = %#v, err=%v", ancestry.Leases, err)
	}
	var messages int
	if err := rebuilt.db.QueryRowContext(context.Background(), `SELECT count(*) FROM messages WHERE id LIKE 'lease-takeover:%' AND to_actor=?`, uuidBlob(a)).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 1 {
		t.Fatalf("rebuilt canonical takeover messages = %d, want one", messages)
	}
	restarted, err := state.New(appender, replayEvents, state.Options{})
	if err != nil {
		t.Fatal(err)
	}
	polled, err := restarted.Poll(a, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(polled) != 1 || polled[0].To != a || !strings.Contains(polled[0].Body, successor) {
		t.Fatalf("restarted holder mailbox = %#v", polled)
	}
}

func TestAcquireRetryIsIdempotentButConflictingLeaseReuseIsRejected(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	request := leaseRequest(a, actorUUID("retry-lease"), actorUUID("retry-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/retry"}, at)
	first, err := db.AcquireMutationLease(context.Background(), request)
	if err != nil || first.Decision != protocol.LeaseGrant {
		t.Fatalf("first acquire = %#v, %v", first, err)
	}
	retry, err := db.AcquireMutationLease(context.Background(), request)
	if err != nil || retry.Decision != protocol.LeaseGrant || retry.Lease == nil || retry.Lease.FencingToken != request.FencingToken {
		t.Errorf("identical retry = %#v, %v", retry, err)
	}
	conflict := request
	conflict.FencingToken = actorUUID("retry-conflicting-token")
	if got, err := db.AcquireMutationLease(context.Background(), conflict); err == nil && got.Decision == protocol.LeaseGrant {
		t.Fatalf("conflicting lease reuse granted: %#v", got)
	}
}

//nolint:cyclop // Replay coverage intentionally combines journal reconstruction and expiry cleanup.
func TestExpiredOrReplacedLeaseCannotResurrectAsActiveOnReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a, b, repo, workspace := actorUUID("replay-a"), actorUUID("replay-b"), actorUUID("replay-repo"), actorUUID("replay-workspace")
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseActor(t, db, b, 2, at)
	seedLeaseIntent(t, db, a, "replay-intent", "replay-tool", repo, workspace, at)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("replay-root"), actorUUID("replay-root-token"), "replay-intent", "replay-tool", repo, workspace, 1, []string{"/repo/replay"}, at))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.AcquireMutationLease(context.Background(), leaseRequest(b, actorUUID("replay-cleanup"), actorUUID("replay-cleanup-token"), "replay-intent", "replay-tool", repo, workspace, 2, []string{"/repo/replay"}, at.Add(3*time.Minute))); err != nil {
		// The request is intentionally blocked by the missing generation-2 intent;
		// cleanup still runs before that validation.
		t.Logf("expected cleanup request rejection: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	journal, events, err := store.Open(path + ".events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	rebuilt, err := OpenProjection(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Errorf("close rebuilt database: %v", err)
		}
	})
	if err := rebuilt.ProjectAll(events); err != nil {
		t.Fatal(err)
	}
	var active int
	if err := rebuilt.db.QueryRowContext(context.Background(), `SELECT count(*) FROM mutation_leases WHERE lease_uuid=? AND state='active'`, uuidBlob(root.Lease.LeaseUUID)).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("replayed expired lease is active: %d", active)
	}
}

func TestTakeoverTTLIsCappedAtRootHardDeadline(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("ttl-root"), actorUUID("ttl-root-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/ttl-cap"}, at))
	if err != nil {
		t.Fatal(err)
	}
	db.SetLeaseAppender(&leaseTestAppender{}, 0)
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET heartbeat_at=? WHERE session_uuid=?`, at.Add(29*time.Minute).Format(time.RFC3339Nano), uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	result, err := db.TakeoverMutationLease(context.Background(), takeoverRequest(root.Lease.LeaseUUID, actorUUID("ttl-successor"), actorUUID("ttl-successor-token"), b, at.Add(29*time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	if result.Lease.ExpiresAt.After(root.Lease.HardDeadline) {
		t.Fatalf("successor expiry %s exceeds root hard deadline %s", result.Lease.ExpiresAt, root.Lease.HardDeadline)
	}
}

func TestDeadAndStaleRequesterCannotTakeOver(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("liveness-root"), actorUUID("liveness-root-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/liveness"}, at))
	if err != nil {
		t.Fatal(err)
	}
	db.SetLeaseAppender(&leaseTestAppender{}, 0)
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET state='dead' WHERE session_uuid=?`, uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TakeoverMutationLease(context.Background(), takeoverRequest(root.Lease.LeaseUUID, actorUUID("dead-successor"), actorUUID("dead-token"), b, at.Add(time.Second))); err == nil {
		t.Fatal("dead requester takeover was accepted")
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET state='active', heartbeat_at=? WHERE session_uuid=?`, at.Add(-time.Minute).Format(time.RFC3339Nano), uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.TakeoverMutationLease(context.Background(), takeoverRequest(root.Lease.LeaseUUID, actorUUID("stale-successor"), actorUUID("stale-token"), b, at.Add(time.Second))); err == nil {
		t.Fatal("stale requester takeover was accepted")
	}
}

//nolint:cyclop,gocognit // Schema introspection intentionally checks all lease tables and constraints.
func TestCanonicalLeaseSchemaForeignKeysAndChecksAreIntrospected(t *testing.T) {
	db := openLeaseAcceptanceDB(t)
	ctx := context.Background()
	for _, table := range []string{"mutation_leases", "mutation_lease_paths", "mutation_lease_audit"} {
		var sqlText string
		if err := db.db.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&sqlText); err != nil {
			t.Fatal(err)
		}
		if table == "mutation_leases" && !strings.Contains(strings.ToUpper(sqlText), "CHECK (STATE IN") {
			t.Errorf("mutation_leases missing state CHECK constraint: %s", sqlText)
		}
		if table == "mutation_lease_audit" && !strings.Contains(strings.ToUpper(sqlText), "CHECK (ACQUISITION_SOURCE IN") {
			t.Errorf("mutation_lease_audit missing acquisition_source CHECK constraint: %s", sqlText)
		}
		rows, err := db.db.QueryContext(ctx, `SELECT "table" FROM pragma_foreign_key_list(?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		var parents []string
		for rows.Next() {
			var parent string
			if err := rows.Scan(&parent); err != nil {
				t.Fatal(err)
			}
			parents = append(parents, parent)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil { //nolint:sqlclosecheck // rows are closed immediately after checking iteration errors.
			t.Fatal(err)
		}
		if table == "mutation_leases" && !containsString(parents, "mutation_leases") {
			t.Errorf("mutation_leases missing self foreign keys: %v", parents)
		}
		if table != "mutation_leases" && !containsString(parents, "mutation_leases") {
			t.Errorf("%s missing mutation_leases foreign key: %v", table, parents)
		}
	}
}

func containsString(values []string, want string) bool {
	return slices.Contains(values, want)
}
