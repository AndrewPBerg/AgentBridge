package provenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
	"github.com/AndrewPBerg/agent-bridge/internal/store"
)

type leaseJournalFixtureData struct {
	dir      string
	journal  *store.Journal
	db       *DB
	appender *ProjectingAppender
	engine   *state.Engine
	actors   []protocol.Actor
	now      time.Time
}

// leaseJournalFixture creates actors and intents through Engine, so the only
// source of projection rows is the shared journal.
func leaseJournalFixture(t *testing.T) *leaseJournalFixtureData {
	t.Helper()
	dir := t.TempDir()
	journal, events, err := store.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := OpenProjection(filepath.Join(dir, "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	appender := NewProjectingAppender(journal, db)
	engine, err := state.New(appender, events, state.Options{})
	if err != nil {
		t.Fatal(err)
	}
	db.SetLeaseAppender(engine, 0)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	repo, workspace := actorUUID("journal-repo"), actorUUID("journal-workspace")
	actors := make([]protocol.Actor, 3)
	for i, label := range []string{"journal-a", "journal-b", "journal-c"} {
		actors[i] = protocol.Actor{Address: actorUUID(label), SessionUUID: actorUUID(label), Harness: "test", CWD: "/repo", State: "active", RepositoryUUID: repo, WorkspaceUUID: workspace, Generation: 1, StartedAt: now, HeartbeatAt: now, Addressable: true}
		actors[i], err = engine.Register(actors[i])
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := range actors {
		actor := &actors[i]
		intent := protocol.Intent{ID: "journal-intent-" + string(rune('a'+i)), Actor: actor.Address, SessionGeneration: 1, ToolCallID: "journal-tool-" + string(rune('a'+i)), Tool: "edit", Operation: "edit", Paths: []string{"/repo/lease-" + string(rune('a'+i))}, CWD: "/repo", RepositoryUUID: repo, WorkspaceUUID: workspace, WorkspaceKey: "/repo", StartedAt: now, ExpiresAt: now.Add(time.Hour)}
		if _, err := engine.BeginIntent(intent); err != nil {
			t.Fatal(err)
		}
	}
	if err := appender.WaitForCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		appender.Close()
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
		if err := journal.Close(); err != nil {
			t.Errorf("close journal: %v", err)
		}
	})
	return &leaseJournalFixtureData{dir: dir, journal: journal, db: db, appender: appender, engine: engine, actors: actors, now: now}
}

func journalLeaseRequest(actor *protocol.Actor, lease, token, intent, tool, repo, workspace string, now time.Time) *protocol.MutationLeaseRequest {
	return &protocol.MutationLeaseRequest{LeaseUUID: lease, FencingToken: token, ActorUUID: actor.SessionUUID, Generation: 1, RepositoryUUID: repo, WorkspaceUUID: workspace, IntentID: intent, ToolCallID: tool, Paths: []string{"/repo/shared"}, Now: now}
}

func TestLeaseAdmissionUsesProjectedNestedRepositoryIntentScope(t *testing.T) {
	fixture := leaseJournalFixture(t)
	actor := protocol.Actor{Address: actorUUID("nested-actor"), SessionUUID: actorUUID("nested-actor"), Harness: "test", CWD: "/work", State: "active", Generation: 1, StartedAt: fixture.now, HeartbeatAt: fixture.now, Addressable: true}
	actor, err := fixture.engine.Register(actor)
	if err != nil {
		t.Fatal(err)
	}
	git := &protocol.GitContext{RepoRoot: "/work/nested", WorktreeRoot: "/work/nested", GitDir: "/work/nested/.git", CommonDir: "/work/nested/.git"}
	intent := protocol.Intent{ID: "nested-intent", Actor: actor.Address, SessionGeneration: 1, ToolCallID: "nested-tool", Tool: "edit", Operation: "edit", Paths: []string{"/work/nested/file.ts"}, CWD: "/work", Git: git, StartedAt: fixture.now, ExpiresAt: fixture.now.Add(time.Hour)}
	if _, err := fixture.engine.BeginIntent(intent); err != nil {
		t.Fatal(err)
	}
	if err := fixture.appender.WaitForCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := &protocol.MutationLeaseRequest{LeaseUUID: actorUUID("nested-lease"), FencingToken: actorUUID("nested-token"), ActorUUID: actor.Address, Generation: 1, RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, IntentID: intent.ID, ToolCallID: intent.ToolCallID, Paths: intent.Paths, Now: fixture.now}
	result, err := fixture.db.AcquireMutationLease(context.Background(), request)
	if err != nil || result.Decision != protocol.LeaseGrant {
		t.Fatalf("nested admission = %#v, %v", result, err)
	}
	if result.Lease.RepositoryUUID == actor.RepositoryUUID || result.Lease.WorkspaceUUID == actor.WorkspaceUUID {
		t.Fatalf("nested lease kept parent actor scope: %#v", result.Lease)
	}
}

//nolint:gocognit,cyclop // Intentional end-to-end concurrency coverage has high control-flow complexity.
func TestLeaseJournalSharedAppenderConcurrentEngineAndLifecycleSequences(t *testing.T) {
	fixture := leaseJournalFixture(t)
	journal, db, appender, engine, actors, now := fixture.journal, fixture.db, fixture.appender, fixture.engine, fixture.actors, fixture.now
	repo, workspace := actors[0].RepositoryUUID, actors[0].WorkspaceUUID
	rootReq := journalLeaseRequest(&actors[0], actorUUID("journal-root"), actorUUID("journal-root-token"), "journal-intent-a", "journal-tool-a", repo, workspace, now)
	root, err := db.AcquireMutationLease(context.Background(), rootReq)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 20 {
			if _, err := engine.RecordSessionEvent(protocol.SessionEvent{ID: "ordinary-" + string(rune('a'+i)), Actor: actors[0].Address, SessionGeneration: 1, Type: "ordinary"}); err != nil {
				errs <- err
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := range 3 {
			req := rootReq
			req.Now = now.Add(time.Duration(i+1) * time.Second)
			if _, err := db.RenewMutationLeaseContext(ctx, *req); err != nil {
				errs <- err
			}
		}
		b, err := db.TakeoverMutationLease(ctx, protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: root.Lease.LeaseUUID, LeaseUUID: actorUUID("journal-b"), FencingToken: actorUUID("journal-b-token"), RequesterActorUUID: actors[1].SessionUUID, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: "handoff", Now: now.Add(10 * time.Second)})
		if err != nil {
			errs <- err
			return
		}
		c, err := db.TakeoverMutationLease(ctx, protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: b.Lease.LeaseUUID, LeaseUUID: actorUUID("journal-c"), FencingToken: actorUUID("journal-c-token"), RequesterActorUUID: actors[2].SessionUUID, RequesterGeneration: 1, AcquisitionSource: "human", Reason: "handoff", Now: now.Add(11 * time.Second)})
		if err != nil {
			errs <- err
			return
		}
		_, err = db.ReleaseMutationLease(ctx, &protocol.MutationLeaseReleaseRequest{LeaseUUID: c.Lease.LeaseUUID, FencingToken: c.Lease.FencingToken, ActorUUID: actors[2].SessionUUID, Generation: 1})
		if err != nil {
			errs <- err
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("interleaved operation: %v", err)
	}
	if err := appender.WaitForCurrent(ctx); err != nil {
		t.Errorf("projection wait: %v", err)
	}
	verifyJournalRestart(t, journal, db)
}

func verifyJournalRestart(t *testing.T, journal *store.Journal, db *DB) {
	t.Helper()
	reopened, events, err := store.Open(journal.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened journal: %v", err)
		}
	}()
	seen := make(map[uint64]bool, len(events))
	for i, event := range events {
		if event.Sequence != uint64(i+1) || seen[event.Sequence] {
			t.Fatalf("journal sequence[%d] = %d (duplicate or gap)", i, event.Sequence)
		}
		seen[event.Sequence] = true
	}
	if len(events) < 1 {
		t.Fatal("journal is empty")
	}
	if _, err := state.New(journal, events, state.Options{}); err != nil {
		t.Fatalf("engine restart: %v", err)
	}
	restarted, err := OpenProjection(db.Path())
	if err != nil {
		t.Fatalf("projection restart: %v", err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatalf("close restarted projection: %v", err)
	}
}

//nolint:gocognit,cyclop // Intentional end-to-end rebuild coverage has high control-flow complexity.
func TestLeaseJournalDestructiveProjectionRebuildPreservesLineagePathsAuditAndMailbox(t *testing.T) {
	fixture := leaseJournalFixture(t)
	dir, journal, db, appender, actors, now := fixture.dir, fixture.journal, fixture.db, fixture.appender, fixture.actors, fixture.now
	repo, workspace := actors[0].RepositoryUUID, actors[0].WorkspaceUUID
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	root, err := db.AcquireMutationLease(ctx, journalLeaseRequest(&actors[0], actorUUID("rebuild-a"), actorUUID("rebuild-a-token"), "journal-intent-a", "journal-tool-a", repo, workspace, now))
	if err != nil {
		t.Fatal(err)
	}
	b, err := db.TakeoverMutationLease(ctx, protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: root.Lease.LeaseUUID, LeaseUUID: actorUUID("rebuild-b"), FencingToken: actorUUID("rebuild-b-token"), RequesterActorUUID: actors[1].SessionUUID, RequesterGeneration: 1, AcquisitionSource: "agent", Reason: "handoff", Now: now.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.TakeoverMutationLease(ctx, protocol.MutationLeaseTakeoverRequest{PredecessorLeaseUUID: b.Lease.LeaseUUID, LeaseUUID: actorUUID("rebuild-c"), FencingToken: actorUUID("rebuild-c-token"), RequesterActorUUID: actors[2].SessionUUID, RequesterGeneration: 1, AcquisitionSource: "human", Reason: "handoff", Now: now.Add(2 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReleaseMutationLease(ctx, &protocol.MutationLeaseReleaseRequest{LeaseUUID: c.Lease.LeaseUUID, FencingToken: c.Lease.FencingToken, ActorUUID: actors[2].SessionUUID, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := appender.WaitForCurrent(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(filepath.Join(dir, "bridge.db") + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	freshJournal, events, err := store.Open(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := freshJournal.Close(); err != nil {
			t.Errorf("close fresh journal: %v", err)
		}
	}()
	rebuilt, err := OpenProjection(filepath.Join(dir, "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rebuilt.Close(); err != nil {
			t.Errorf("close rebuilt projection: %v", err)
		}
	}()
	if err := rebuilt.ProjectAll(events); err != nil {
		t.Fatal(err)
	}
	ancestry, err := rebuilt.MutationLeaseAncestry(context.Background(), c.Lease.LeaseUUID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ancestry.Leases) != 3 || ancestry.Leases[0].LeaseUUID != root.Lease.LeaseUUID || ancestry.Leases[1].LeaseUUID != b.Lease.LeaseUUID || ancestry.Leases[2].LeaseUUID != c.Lease.LeaseUUID {
		t.Fatalf("rebuilt lineage = %#v", ancestry.Leases)
	}
	if ancestry.Leases[0].State != protocol.LeaseSuperseded || ancestry.Leases[1].State != protocol.LeaseSuperseded || ancestry.Leases[2].State != protocol.LeaseReleased {
		t.Fatalf("rebuilt states = %#v", ancestry.Leases)
	}
	var paths, audit, mailbox int
	if err := rebuilt.db.QueryRowContext(ctx, `SELECT count(*) FROM mutation_lease_paths WHERE lease_uuid=?`, uuidBlob(c.Lease.LeaseUUID)).Scan(&paths); err != nil {
		t.Fatal(err)
	}
	if err := rebuilt.db.QueryRowContext(ctx, `SELECT count(*) FROM mutation_lease_audit`).Scan(&audit); err != nil {
		t.Fatal(err)
	}
	if err := rebuilt.db.QueryRowContext(ctx, `SELECT count(*) FROM messages WHERE to_actor=?`, uuidBlob(actors[0].SessionUUID)).Scan(&mailbox); err != nil {
		t.Fatal(err)
	}
	if paths != 1 || audit != 2 || mailbox != 1 {
		t.Fatalf("rebuilt paths/audit/mailbox = %d/%d/%d", paths, audit, mailbox)
	}
}

type failOnceAppender struct {
	sync.Mutex
	failed bool
	calls  int
}

func (f *failOnceAppender) Append(protocol.Event) error {
	f.Lock()
	defer f.Unlock()
	f.calls++
	if !f.failed {
		f.failed = true
		return errors.New("poison")
	}
	return nil
}

func TestFailedPoisonedLeaseJournalAppendCannotReuseSequence(t *testing.T) {
	db, err := OpenProjection(filepath.Join(t.TempDir(), "bridge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	}()
	primary := &failOnceAppender{}
	appender := NewProjectingAppender(primary, db)
	defer appender.Close()
	if _, err := appender.AppendNext(protocol.Event{Version: protocol.Version, Type: "ordinary"}); err == nil {
		t.Fatal("poisoned append unexpectedly succeeded")
	}
	if _, err := appender.AppendNext(protocol.Event{Version: protocol.Version, Type: "ordinary"}); err == nil {
		t.Fatal("failed append allowed sequence reuse")
	}
	primary.Lock()
	defer primary.Unlock()
	if primary.calls != 1 {
		t.Fatalf("primary append calls = %d, want 1", primary.calls)
	}
}
