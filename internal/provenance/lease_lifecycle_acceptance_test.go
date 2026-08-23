package provenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestReleaseAppendFailureReturnsErrorAndPreservesActiveLease(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	appender := &leaseTestAppender{}
	db.SetLeaseAppender(appender, 0)
	req := leaseRequest(a, actorUUID("release-failure"), actorUUID("release-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/release-failure"}, at)
	lease, err := db.AcquireMutationLease(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	appender.mu.Lock()
	appender.fail = true
	appender.mu.Unlock()
	released, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: lease.Lease.LeaseUUID, FencingToken: lease.Lease.FencingToken, ActorUUID: a, Generation: 1})
	if err == nil || released {
		t.Fatalf("release = %v, %v; want append error", released, err)
	}
	var state string
	if err := db.db.QueryRowContext(context.Background(), `SELECT state FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(lease.Lease.LeaseUUID)).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != protocol.LeaseActive {
		t.Fatalf("state = %q, want active", state)
	}
}

func TestReleaseCommitFailureReturnsError(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	req := leaseRequest(a, actorUUID("commit-failure"), actorUUID("commit-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/commit-failure"}, at)
	got, err := db.AcquireMutationLease(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Closing the underlying handle makes the transaction commit path fail;
	// this specifically guards against the release API swallowing commit errors.
	if err := db.db.Close(); err != nil {
		t.Fatal(err)
	}
	released, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: got.Lease.LeaseUUID, FencingToken: got.Lease.FencingToken, ActorUUID: a, Generation: 1})
	if err == nil || released {
		t.Fatalf("release after close = %v, %v; want DB error", released, err)
	}
}

//nolint:cyclop // Rebuild acceptance intentionally checks each durable timestamp boundary.
func TestRebuiltLeaseRetainsTerminalAt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db, err := OpenProjection(path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetLeaseAppender(&leaseTestAppender{}, 0)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a := actorUUID("terminal-a")
	repo := actorUUID("terminal-repo")
	workspace := actorUUID("terminal-workspace")
	intent := "terminal-intent"
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseIntent(t, db, a, intent, "terminal-tool", repo, workspace, at)
	req := leaseRequest(a, actorUUID("terminal-lease"), actorUUID("terminal-token"), intent, "terminal-tool", repo, workspace, 1, []string{"/repo/terminal"}, at)
	got, err := db.AcquireMutationLease(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	releasedAt := at.Add(7 * time.Second)
	if _, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: got.Lease.LeaseUUID, FencingToken: got.Lease.FencingToken, ActorUUID: a, Generation: 1, Now: releasedAt}); err != nil {
		t.Fatal(err)
	}
	var terminal, expiry string
	if err := db.db.QueryRowContext(context.Background(), `SELECT terminal_at, expires_at FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(got.Lease.LeaseUUID)).Scan(&terminal, &expiry); err != nil {
		t.Fatal(err)
	}
	if terminal != releasedAt.Format(time.RFC3339Nano) || expiry != terminal {
		t.Fatalf("terminal/expiry = %q/%q, want %q", terminal, expiry, releasedAt.Format(time.RFC3339Nano))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
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
	var rebuiltTerminal, rebuiltExpiry string
	if err := rebuilt.db.QueryRowContext(context.Background(), `SELECT terminal_at, expires_at FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(got.Lease.LeaseUUID)).Scan(&rebuiltTerminal, &rebuiltExpiry); err != nil {
		t.Fatal(err)
	}
	if rebuiltTerminal == "" || rebuiltExpiry != expiry {
		t.Fatalf("rebuilt terminal/expiry = %q/%q, want terminal and expiry %q", rebuiltTerminal, rebuiltExpiry, expiry)
	}
}

//nolint:cyclop,gocognit // End-to-end cancellation acceptance covers fencing, projection, and rebuild.
func TestWaitingReleaseCancelsAndRebuildsWithoutBlockers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bridge.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close cancellation database: %v", err)
		}
	})
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	a, b := actorUUID("cancel-a"), actorUUID("cancel-b")
	repo, workspace := actorUUID("cancel-repo"), actorUUID("cancel-workspace")
	seedLeaseActor(t, db, a, 1, at)
	seedLeaseActor(t, db, b, 1, at)
	seedLeaseIntent(t, db, a, "cancel-a-intent", "cancel-a-tool", repo, workspace, at)
	seedLeaseIntent(t, db, b, "cancel-b-intent", "cancel-b-tool", repo, workspace, at)
	blocker, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("cancel-blocker"), actorUUID("cancel-blocker-token"), "cancel-a-intent", "cancel-a-tool", repo, workspace, 1, []string{"/repo/cancel"}, at))
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := db.AcquireMutationLease(context.Background(), leaseRequest(b, actorUUID("cancel-waiting"), actorUUID("cancel-waiting-token"), "cancel-b-intent", "cancel-b-tool", repo, workspace, 1, []string{"/repo/cancel"}, at.Add(time.Second)))
	if err != nil || waiting.Lease == nil || waiting.Decision != protocol.LeaseWait {
		t.Fatalf("waiting acquire = %#v, %v", waiting, err)
	}
	stale, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: waiting.Lease.LeaseUUID, FencingToken: waiting.Lease.FencingToken, ActorUUID: a, Generation: 1, Now: at.Add(2 * time.Second)})
	if err != nil || stale {
		t.Fatalf("stale actor waiting release = %v, %v", stale, err)
	}
	stale, err = db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: waiting.Lease.LeaseUUID, FencingToken: actorUUID("cancel-wrong-token"), ActorUUID: b, Generation: 1, Now: at.Add(2 * time.Second)})
	if err != nil || stale {
		t.Fatalf("stale token waiting release = %v, %v", stale, err)
	}
	releasedAt := at.Add(3 * time.Second)
	canceled, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: waiting.Lease.LeaseUUID, FencingToken: waiting.Lease.FencingToken, ActorUUID: b, Generation: 1, Now: releasedAt})
	if err != nil || !canceled {
		t.Fatalf("waiting release = %v, %v", canceled, err)
	}
	var state, terminal, eventAt string
	if err := db.db.QueryRowContext(context.Background(), `SELECT state,terminal_at FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(waiting.Lease.LeaseUUID)).Scan(&state, &terminal); err != nil {
		t.Fatal(err)
	}
	if state != protocol.LeaseCancelled || terminal != releasedAt.Format(time.RFC3339Nano) {
		t.Fatalf("canceled lease = %q/%q", state, terminal)
	}
	var blockers int
	if err := db.db.QueryRowContext(context.Background(), `SELECT count(*) FROM mutation_lease_blockers WHERE waiting_lease_uuid=?`, uuidBlob(waiting.Lease.LeaseUUID)).Scan(&blockers); err != nil {
		t.Fatal(err)
	}
	if blockers != 0 {
		t.Fatalf("normalized blockers = %d, want 0", blockers)
	}
	if err := db.db.QueryRowContext(context.Background(), `SELECT at FROM events WHERE type='lease.canceled' ORDER BY sequence DESC LIMIT 1`).Scan(&eventAt); err != nil {
		t.Fatal(err)
	}
	if eventAt != releasedAt.Format(time.RFC3339Nano) {
		t.Fatalf("canceled event at = %q, want %q", eventAt, releasedAt.Format(time.RFC3339Nano))
	}
	if _, err := db.ListMutationLeases(context.Background(), b, workspace); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	rebuilt, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rebuilt.Close(); err != nil {
			t.Errorf("close rebuilt cancellation database: %v", err)
		}
	})
	leases, err := rebuilt.ListMutationLeases(context.Background(), b, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if len(leases) != 0 || blocker.Lease == nil {
		t.Fatalf("reconnected active/waiting leases = %#v", leases)
	}
}

//nolint:cyclop // Near-deadline acceptance keeps blocker release and promotion together.
func TestWaitingPromotionCapsExpiryAtExistingHardDeadline(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	root, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("near-root"), actorUUID("near-root-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/near"}, at))
	if err != nil {
		t.Fatal(err)
	}
	waitingRequest := leaseRequest(b, actorUUID("near-waiting"), actorUUID("near-waiting-token"), "intent-b", "tool-b", repo, workspace, 1, []string{"/repo/near"}, at.Add(time.Second))
	waiting, err := db.AcquireMutationLease(context.Background(), waitingRequest)
	if err != nil || waiting.Lease == nil {
		t.Fatalf("near-deadline wait = %#v, %v", waiting, err)
	}
	hard := at.Add(90 * time.Second)
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET heartbeat_at=? WHERE session_uuid=?`, at.Add(30*time.Second).Format(time.RFC3339Nano), uuidBlob(b)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutations SET session_generation=1 WHERE id=?`, "intent-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutation_leases SET hard_deadline=? WHERE lease_uuid=?`, hard.Format(time.RFC3339Nano), uuidBlob(waiting.Lease.LeaseUUID)); err != nil {
		t.Fatal(err)
	}
	waitingRequest.Now = at.Add(30 * time.Second)
	if released, err := db.ReleaseMutationLease(context.Background(), &protocol.MutationLeaseReleaseRequest{LeaseUUID: root.Lease.LeaseUUID, FencingToken: root.Lease.FencingToken, ActorUUID: a, Generation: 1, Now: waitingRequest.Now}); err != nil || !released {
		t.Fatalf("near-deadline blocker release = %v, %v", released, err)
	}
	promoted, err := db.AcquireMutationLease(context.Background(), waitingRequest)
	if err != nil || promoted.Decision != protocol.LeaseGrant || promoted.Lease == nil {
		t.Fatalf("near-deadline promotion = %#v, %v", promoted, err)
	}
	if !promoted.Lease.ExpiresAt.Equal(hard) {
		t.Fatalf("promoted expiry = %s, want %s", promoted.Lease.ExpiresAt, hard)
	}
	_ = root
}

func TestImmediateGenerationReplacementReacquiresWithoutWait(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET generation=2 WHERE session_uuid=?`, uuidBlob(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutations SET session_generation=2 WHERE id=?`, intent); err != nil {
		t.Fatal(err)
	}
	first := leaseRequest(a, actorUUID("generation-old"), actorUUID("generation-old-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/generation"}, at)
	// Restore the old generation long enough to create its lease, then replace it.
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET generation=1 WHERE session_uuid=?`, uuidBlob(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutations SET session_generation=1 WHERE id=?`, intent); err != nil {
		t.Fatal(err)
	}
	old, err := db.AcquireMutationLease(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE actors SET generation=2, heartbeat_at=? WHERE session_uuid=?`, at.Format(time.RFC3339Nano), uuidBlob(a)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutations SET session_generation=2 WHERE id=?`, intent); err != nil {
		t.Fatal(err)
	}
	next := leaseRequest(a, actorUUID("generation-new"), actorUUID("generation-new-token"), intent, "tool-a", repo, workspace, 2, []string{"/repo/generation"}, at.Add(time.Second))
	got, err := db.AcquireMutationLease(context.Background(), next)
	if err != nil || got.Decision != protocol.LeaseGrant {
		t.Fatalf("replacement acquire = %#v, %v", got, err)
	}
	if old.Lease == nil {
		t.Fatal("old lease missing")
	}
}

func TestLoadLeaseCorruptionAndDBErrorsPropagate(t *testing.T) {
	db, at, a, _, repo, workspace, intent := leaseFixture(t)
	req := leaseRequest(a, actorUUID("corrupt"), actorUUID("corrupt-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/corrupt"}, at)
	got, err := db.AcquireMutationLease(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.ExecContext(context.Background(), `UPDATE mutation_leases SET expires_at='not-a-time' WHERE lease_uuid=?`, uuidBlob(got.Lease.LeaseUUID)); err != nil {
		t.Fatal(err)
	}
	id, err := parseLeaseUUID(got.Lease.LeaseUUID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loadLease(context.Background(), db.db, id); err == nil {
		t.Fatal("corrupt lease timestamp was swallowed")
	}
	if err := db.db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RenewMutationLease(*req); err == nil {
		t.Fatal("closed DB error was swallowed")
	}
}

func TestWaitingAcquireIsDurableAndNormalized(t *testing.T) {
	db, at, a, b, repo, workspace, intent := leaseFixture(t)
	first, err := db.AcquireMutationLease(context.Background(), leaseRequest(a, actorUUID("wait-blocker"), actorUUID("wait-blocker-token"), intent, "tool-a", repo, workspace, 1, []string{"/repo/wait"}, at))
	if err != nil {
		t.Fatal(err)
	}
	got, err := db.AcquireMutationLease(context.Background(), leaseRequest(b, actorUUID("wait-claim"), actorUUID("wait-token"), "intent-b", "tool-b", repo, workspace, 1, []string{"/repo/wait"}, at.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if got.Decision != protocol.LeaseWait {
		t.Fatalf("decision = %#v, want wait", got)
	}
	var state string
	if err := db.db.QueryRowContext(context.Background(), `SELECT state FROM mutation_leases WHERE lease_uuid=?`, uuidBlob(actorUUID("wait-claim"))).Scan(&state); err != nil {
		t.Fatalf("waiting claim not durable: %v", err)
	}
	if state != protocol.LeaseWaiting {
		t.Fatalf("waiting state = %q", state)
	}
	if first.Lease == nil {
		t.Fatal("blocker missing")
	}
}
