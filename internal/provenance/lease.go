package provenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

const actorHeartbeatFreshness = 60 * time.Second

// appendLeaseEvent publishes through the authoritative append-only journal.
// The SQL transaction is deliberately not used to allocate sequences. The
// journal append and SQLite projection have an unavoidable boundary: a durable
// journal event may be ahead of its projection after a crash. Callers fail
// closed and startup replay reconciles that state from events.jsonl.
func (d *DB) appendLeaseEvent(at time.Time, eventType string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	d.leaseMu.Lock()
	defer d.leaseMu.Unlock()
	if d.leaseAppender == nil {
		return errors.New("lease authority is not configured")
	}
	event := protocol.Event{Version: protocol.Version, Type: eventType, At: at.UTC(), Data: data}
	if _, err := d.leaseAppender.AppendNext(event); err != nil {
		return fmt.Errorf("append lease lifecycle event: %w", err)
	}
	return nil
}

func (d *DB) appendLeaseLifecycle(action string, lease *protocol.MutationLease, at time.Time, reason string) error {
	event := protocol.MutationLeaseLifecycleEvent{Lease: *lease, Action: action, Reason: reason}
	return d.appendLeaseEvent(at, "lease."+action, event)
}

func ensureLeaseSchema(ctx context.Context, d *DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS mutation_leases (lease_uuid BLOB PRIMARY KEY, fencing_token BLOB NOT NULL, actor_uuid BLOB NOT NULL, generation INTEGER NOT NULL, repository_uuid BLOB NOT NULL, workspace_uuid BLOB NOT NULL, intent_id TEXT NOT NULL, tool_call_id TEXT NOT NULL, granted_at TEXT NOT NULL, renewed_at TEXT NOT NULL, expires_at TEXT NOT NULL, hard_deadline TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('active','waiting','released','expired','canceled','superseded','revoked')), predecessor_lease_uuid BLOB REFERENCES mutation_leases(lease_uuid), root_lease_uuid BLOB NOT NULL, takeover_depth INTEGER NOT NULL DEFAULT 0, superseded_by_lease_uuid BLOB, terminal_at TEXT) STRICT`,
		`CREATE TABLE IF NOT EXISTS mutation_lease_audit (id INTEGER PRIMARY KEY, predecessor_lease_uuid BLOB NOT NULL REFERENCES mutation_leases(lease_uuid), successor_lease_uuid BLOB NOT NULL REFERENCES mutation_leases(lease_uuid), requester_actor_uuid BLOB NOT NULL, requester_generation INTEGER NOT NULL, acquisition_source TEXT NOT NULL CHECK(acquisition_source IN ('agent','human')), reason TEXT NOT NULL, work_unit_uuid BLOB, collision_id BLOB, requested_at TEXT NOT NULL, notification_body TEXT NOT NULL) STRICT`,
		`CREATE TABLE IF NOT EXISTS mutation_lease_paths (lease_uuid BLOB NOT NULL REFERENCES mutation_leases(lease_uuid) ON DELETE CASCADE, path TEXT NOT NULL, PRIMARY KEY(lease_uuid,path)) STRICT`,
		`CREATE TABLE IF NOT EXISTS mutation_lease_blockers (waiting_lease_uuid BLOB NOT NULL REFERENCES mutation_leases(lease_uuid) ON DELETE CASCADE, blocking_lease_uuid BLOB NOT NULL REFERENCES mutation_leases(lease_uuid), blocking_root_lease_uuid BLOB NOT NULL, collision_id BLOB, PRIMARY KEY(waiting_lease_uuid,blocking_lease_uuid)) STRICT`,
		`CREATE INDEX IF NOT EXISTS mutation_lease_blockers_blocking ON mutation_lease_blockers(blocking_lease_uuid)`,
		`CREATE INDEX IF NOT EXISTS mutation_lease_paths_path ON mutation_lease_paths(path)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS mutation_leases_one_active_leaf ON mutation_leases(root_lease_uuid) WHERE state='active'`,
	} {
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize mutation lease schema: %w", err)
		}
	}
	// Upgrade databases created by the in-progress lease worker without ever
	// rewriting lease identities.
	for _, column := range []string{"predecessor_lease_uuid BLOB", "root_lease_uuid BLOB", "takeover_depth INTEGER NOT NULL DEFAULT 0", "superseded_by_lease_uuid BLOB", "terminal_at TEXT"} {
		name := strings.Fields(column)[0]
		var present string
		err := d.db.QueryRowContext(ctx, `SELECT name FROM pragma_table_info('mutation_leases') WHERE name=?`, name).Scan(&present)
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := d.db.ExecContext(ctx, `ALTER TABLE mutation_leases ADD COLUMN `+column); err != nil {
				return fmt.Errorf("upgrade mutation lease schema: %w", err)
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func parseLeaseUUID(value string) ([]byte, error) {
	if err := protocol.ValidateUUID(value); err != nil {
		return nil, err
	}
	return uuidBlob(value), nil
}

type preparedLeaseRequest struct {
	result                protocol.MutationLeaseResult
	leaseID, token, actor []byte
	repo, workspace       []byte
	paths                 []string
	now                   time.Time
}

func prepareLeaseRequest(request *protocol.MutationLeaseRequest) (preparedLeaseRequest, error) {
	input := preparedLeaseRequest{}
	values := []struct {
		value string
		dest  *[]byte
		name  string
	}{{request.LeaseUUID, &input.leaseID, "lease_uuid"}, {request.FencingToken, &input.token, "fencing_token"}, {request.ActorUUID, &input.actor, "actor_uuid"}, {request.RepositoryUUID, &input.repo, "repository_uuid"}, {request.WorkspaceUUID, &input.workspace, "workspace_uuid"}}
	for _, value := range values {
		parsed, err := parseLeaseUUID(value.value)
		if err != nil {
			input.result = protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "invalid " + value.name}
			return input, err
		}
		*value.dest = parsed
	}
	if request.IntentID == "" || request.ToolCallID == "" || len(request.Paths) == 0 {
		input.result = protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "intent, tool call, and paths are required"}
		return input, errors.New("intent, tool call, and paths are required")
	}
	input.paths = uniquePaths(request.Paths)
	if len(input.paths) == 0 {
		input.result = protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "paths are required"}
		return input, errors.New("paths are required")
	}
	input.now = request.Now
	if input.now.IsZero() {
		input.now = time.Now().UTC()
	}
	input.now = input.now.UTC()
	return input, nil
}

func findExistingLease(ctx context.Context, db leaseQueryer, id []byte, request *protocol.MutationLeaseRequest) (*protocol.MutationLease, protocol.MutationLeaseResult, error) {
	candidate, err := loadLease(ctx, db, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, protocol.MutationLeaseResult{}, nil
	}
	if err != nil {
		return nil, protocol.MutationLeaseResult{}, err
	}
	if candidate.State == protocol.LeaseActive && sameLeaseRequest(&candidate, request) {
		return &candidate, protocol.MutationLeaseResult{Decision: protocol.LeaseGrant, Lease: &candidate}, nil
	}
	if candidate.State != protocol.LeaseWaiting {
		return nil, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "lease_uuid is already used"}, errors.New("conflicting lease_uuid reuse")
	}
	return &candidate, protocol.MutationLeaseResult{}, nil
}

// AcquireMutationLease atomically admits an exact-path edit/write lease.
//
//nolint:cyclop // Admission coordinates validation, conflict selection, and one transaction.
func (d *DB) AcquireMutationLease(ctx context.Context, request *protocol.MutationLeaseRequest) (protocol.MutationLeaseResult, error) {
	d.leaseCommandMu.Lock()
	defer d.leaseCommandMu.Unlock()
	d.leaseAdmissionMu.Lock()
	defer d.leaseAdmissionMu.Unlock()
	input, err := prepareLeaseRequest(request)
	if err != nil {
		return input.result, err
	}
	if err := d.journalLeaseCleanup(ctx, input.now, input.actor, request.Generation); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	d.projectionMu.Lock()
	writeLocked := true
	if err := ensureLeaseSchema(ctx, d); err != nil {
		d.projectionMu.Unlock()
		writeLocked = false
		return protocol.MutationLeaseResult{}, err
	}
	defer func() {
		if writeLocked {
			d.projectionMu.Unlock()
		}
	}()
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	defer rollbackLeaseTx(tx)
	if result, blocked := validateLeaseAdmission(ctx, tx, &input, request); blocked {
		return result, nil
	}
	existing, result, err := findExistingLease(ctx, tx, input.leaseID, request)
	if err != nil || result.Lease != nil {
		return result, err
	}
	conflicts, err := findLeaseConflicts(ctx, tx, input.workspace, input.paths, input.now)
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	if leaseConflict(conflicts, request.LeaseUUID) {
		result, published, err := d.admitWaitingLease(ctx, tx, &input, request, existing, conflicts)
		if err != nil || !published {
			return result, err
		}
		rollbackLeaseTx(tx)
		d.projectionMu.Unlock()
		writeLocked = false
		if err := d.waitLeaseProjection(ctx); err != nil {
			return protocol.MutationLeaseResult{}, err
		}
		return result, nil
	}
	lease, err := d.admitActiveLease(&input, request, existing)
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	rollbackLeaseTx(tx)
	d.projectionMu.Unlock()
	writeLocked = false
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	return protocol.MutationLeaseResult{Decision: protocol.LeaseGrant, Lease: &lease}, nil
}

func (d *DB) admitActiveLease(input *preparedLeaseRequest, request *protocol.MutationLeaseRequest, existing *protocol.MutationLease) (protocol.MutationLease, error) {
	lease := activeLeaseSnapshot(input, request, existing)
	reason := "exact-path grant"
	if existing != nil {
		reason = "waiting contender promoted"
	}
	if err := d.appendLeaseLifecycle("acquired", &lease, input.now, reason); err != nil {
		return protocol.MutationLease{}, err
	}
	return lease, nil
}

func validateLeaseAdmission(ctx context.Context, tx *sql.Tx, input *preparedLeaseRequest, request *protocol.MutationLeaseRequest) (protocol.MutationLeaseResult, bool) {
	var state, heartbeat string
	if err := tx.QueryRowContext(ctx, `SELECT state, heartbeat_at FROM actors WHERE session_uuid=? AND generation=?`, input.actor, request.Generation).Scan(&state, &heartbeat); err != nil {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "actor generation is not registered"}, true
	}
	heartbeatAt, err := parseLeaseTime(heartbeat)
	if err != nil || state == "dead" || input.now.Sub(heartbeatAt) > actorHeartbeatFreshness {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "actor heartbeat is stale"}, true
	}
	var intentTool, intentEnd string
	var repository, workspace []byte
	if err := tx.QueryRowContext(ctx, `SELECT tool_call_id, COALESCE(completed_at,''), repository_uuid, workspace_uuid FROM mutations WHERE id=? AND actor=? AND session_generation=? AND tool_call_id=?`, request.IntentID, input.actor, request.Generation, request.ToolCallID).Scan(&intentTool, &intentEnd, &repository, &workspace); err != nil || intentTool != request.ToolCallID || intentEnd != "" || len(repository) != 16 || len(workspace) != 16 {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "mutation intent is missing or already completed"}, true
	}
	// Scope is daemon-owned intent identity. The actor may be rooted above the
	// repository containing the edited file, so client actor scope is not
	// authoritative for an individual mutation.
	input.repo, input.workspace = repository, workspace
	request.RepositoryUUID, request.WorkspaceUUID = uuidString(repository), uuidString(workspace)
	return protocol.MutationLeaseResult{}, false
}

func leaseConflict(conflicts []protocol.MutationLease, leaseUUID string) bool {
	return len(conflicts) > 0 && conflicts[0].LeaseUUID != leaseUUID
}

func findLeaseConflicts(ctx context.Context, tx *sql.Tx, workspace []byte, paths []string, now time.Time) ([]protocol.MutationLease, error) {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(paths)), ",")
	args := make([]any, 0, 2+len(paths))
	args = append(args, workspace, now.Format(time.RFC3339Nano))
	for _, path := range paths {
		args = append(args, path)
	}
	rows, err := tx.QueryContext(ctx, `SELECT DISTINCT l.lease_uuid,l.fencing_token,l.actor_uuid,l.generation,l.repository_uuid,l.workspace_uuid,l.intent_id,l.tool_call_id,l.granted_at,l.renewed_at,l.expires_at,l.hard_deadline,l.state,COALESCE(l.root_lease_uuid,l.lease_uuid) FROM mutation_leases l JOIN mutation_lease_paths p ON p.lease_uuid=l.lease_uuid WHERE l.state='active' AND l.workspace_uuid=? AND l.expires_at>? AND p.path IN (`+placeholders+`) ORDER BY l.root_lease_uuid, l.lease_uuid`, args...)
	if err != nil {
		return nil, err
	}
	return scanLeases(ctx, rows, tx)
}

func (d *DB) admitWaitingLease(ctx context.Context, tx *sql.Tx, input *preparedLeaseRequest, request *protocol.MutationLeaseRequest, existing *protocol.MutationLease, conflicts []protocol.MutationLease) (protocol.MutationLeaseResult, bool, error) {
	if existing != nil {
		existing.Paths = input.paths
		return protocol.MutationLeaseResult{Decision: protocol.LeaseWait, Lease: existing, Conflicts: conflicts, Reason: "exact path is leased"}, false, nil
	}
	lease, err := waitingLeaseSnapshot(ctx, tx, input, request, &conflicts[0])
	if err != nil {
		return protocol.MutationLeaseResult{}, false, err
	}
	if err := d.appendLeaseLifecycle("waiting", &lease, input.now, "exact path is leased"); err != nil {
		return protocol.MutationLeaseResult{}, false, err
	}
	return protocol.MutationLeaseResult{Decision: protocol.LeaseWait, Lease: &lease, Conflicts: conflicts, Reason: "exact path is leased"}, true, nil
}

func waitingLeaseSnapshot(ctx context.Context, tx *sql.Tx, input *preparedLeaseRequest, request *protocol.MutationLeaseRequest, blocker *protocol.MutationLease) (protocol.MutationLease, error) {
	granted, hard := input.now, input.now.Add(30*time.Minute)
	expires := input.now.Add(2 * time.Minute)
	if expires.After(hard) {
		expires = hard
	}
	lease := protocol.MutationLease{LeaseUUID: request.LeaseUUID, FencingToken: request.FencingToken, ActorUUID: request.ActorUUID, Generation: request.Generation, RepositoryUUID: uuidString(input.repo), WorkspaceUUID: uuidString(input.workspace), IntentID: request.IntentID, ToolCallID: request.ToolCallID, Paths: input.paths, GrantedAt: granted, RenewedAt: granted, ExpiresAt: expires, HardDeadline: hard, State: protocol.LeaseWaiting, RootLeaseUUID: request.LeaseUUID, BlockingLeaseUUID: blocker.LeaseUUID, BlockingRootLeaseUUID: blocker.RootLeaseUUID}
	var collision []byte
	if err := tx.QueryRowContext(ctx, `SELECT id FROM collisions WHERE state IN ('active','open') AND path=? ORDER BY id LIMIT 1`, input.paths[0]).Scan(&collision); err == nil && len(collision) == 16 {
		lease.CollisionID = uuidString(collision)
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return protocol.MutationLease{}, err
	}
	return lease, nil
}

func activeLeaseSnapshot(input *preparedLeaseRequest, request *protocol.MutationLeaseRequest, existing *protocol.MutationLease) protocol.MutationLease {
	granted, hard := input.now, input.now.Add(30*time.Minute)
	expires := input.now.Add(2 * time.Minute)
	if existing != nil && expires.After(existing.HardDeadline) {
		expires = existing.HardDeadline
	}
	lease := protocol.MutationLease{LeaseUUID: request.LeaseUUID, FencingToken: request.FencingToken, ActorUUID: request.ActorUUID, Generation: request.Generation, RepositoryUUID: uuidString(input.repo), WorkspaceUUID: uuidString(input.workspace), IntentID: request.IntentID, ToolCallID: request.ToolCallID, Paths: input.paths, GrantedAt: granted, RenewedAt: granted, ExpiresAt: expires, HardDeadline: hard, State: protocol.LeaseActive, RootLeaseUUID: request.LeaseUUID}
	if existing != nil {
		lease = *existing
		lease.Paths, lease.State, lease.GrantedAt, lease.RenewedAt, lease.ExpiresAt = input.paths, protocol.LeaseActive, granted, granted, expires
		lease.TerminalAt, lease.BlockingLeaseUUID, lease.BlockingRootLeaseUUID = nil, "", ""
	}
	return lease
}

func sameLeaseRequest(existing *protocol.MutationLease, request *protocol.MutationLeaseRequest) bool {
	return existing.LeaseUUID == request.LeaseUUID && existing.FencingToken == request.FencingToken && existing.ActorUUID == request.ActorUUID && existing.Generation == request.Generation && existing.RepositoryUUID == request.RepositoryUUID && existing.WorkspaceUUID == request.WorkspaceUUID && existing.IntentID == request.IntentID && existing.ToolCallID == request.ToolCallID && strings.Join(existing.Paths, "\x00") == strings.Join(uniquePaths(request.Paths), "\x00")
}

func (d *DB) journalLeaseCleanup(ctx context.Context, now time.Time, actor []byte, generation uint64) error {
	rows, err := d.db.QueryContext(ctx, `SELECT lease_uuid FROM mutation_leases WHERE state IN ('active','waiting') AND (expires_at<=? OR hard_deadline<=? OR (actor_uuid=? AND generation<>?))`, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), actor, generation)
	if err != nil {
		return err
	}
	defer closeLeaseRows(rows)
	var ids [][]byte
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, id := range ids {
		lease, err := loadLease(ctx, d.db, id)
		if err != nil {
			return err
		}
		terminal := now
		lease.TerminalAt = &terminal
		if lease.ActorUUID == uuidString(actor) && lease.Generation != generation && now.Sub(lease.RenewedAt) <= 24*time.Hour {
			lease.State = protocol.LeaseReleased
		} else {
			lease.State = protocol.LeaseExpired
		}
		if err := d.appendLeaseLifecycle(strings.TrimPrefix(lease.State, "active"), &lease, now, "deterministic lease cleanup"); err != nil {
			return err
		}
	}
	return nil
}

func parseLeaseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}

func uniquePaths(paths []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

//nolint:cyclop,gocognit,sqlclosecheck // row hydration performs UUID and timestamp conversion.
func scanLeases(ctx context.Context, rows *sql.Rows, tx *sql.Tx) ([]protocol.MutationLease, error) {
	defer closeLeaseRows(rows)
	var out []protocol.MutationLease
	for rows.Next() {
		var id, tok, act, rep, ws []byte
		var gen uint64
		var l protocol.MutationLease
		var g, r, e, h, state string
		var root []byte
		if err := rows.Scan(&id, &tok, &act, &gen, &rep, &ws, &l.IntentID, &l.ToolCallID, &g, &r, &e, &h, &state, &root); err != nil {
			return nil, err
		}
		l.LeaseUUID = uuidString(id)
		l.FencingToken = uuidString(tok)
		l.ActorUUID = uuidString(act)
		l.Generation = gen
		l.RepositoryUUID = uuidString(rep)
		l.WorkspaceUUID = uuidString(ws)
		var err error
		if l.GrantedAt, err = parseLeaseTime(g); err != nil {
			return nil, err
		}
		if l.RenewedAt, err = parseLeaseTime(r); err != nil {
			return nil, err
		}
		if l.ExpiresAt, err = parseLeaseTime(e); err != nil {
			return nil, err
		}
		if l.HardDeadline, err = parseLeaseTime(h); err != nil {
			return nil, err
		}
		l.State = state
		l.RootLeaseUUID = uuidString(root)
		pRows, err := tx.QueryContext(ctx, `SELECT path FROM mutation_lease_paths WHERE lease_uuid=? ORDER BY path`, id)
		if err != nil {
			return nil, err
		}
		for pRows.Next() {
			var p string
			if err := pRows.Scan(&p); err != nil {
				if closeErr := pRows.Close(); closeErr != nil {
					return nil, fmt.Errorf("scan lease path: %w; close: %w", err, closeErr)
				}
				return nil, err
			}
			l.Paths = append(l.Paths, p)
		}
		if err := pRows.Err(); err != nil {
			return nil, err
		}
		if err := pRows.Close(); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// TakeoverMutationLease explicitly creates a successor lease. The predecessor
// row is never rewritten with the successor's holder or fencing token.
//
//nolint:cyclop,gocognit,funlen,gocritic // one transaction preserves lineage and mailbox ordering.
func (d *DB) TakeoverMutationLease(ctx context.Context, req protocol.MutationLeaseTakeoverRequest) (protocol.MutationLeaseTakeoverResult, error) {
	d.leaseCommandMu.Lock()
	defer d.leaseCommandMu.Unlock()
	d.leaseTakeoverMu.Lock()
	defer d.leaseTakeoverMu.Unlock()
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	d.projectionMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			d.projectionMu.Unlock()
		}
	}()
	if err := ensureLeaseSchema(ctx, d); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	pred, err := parseLeaseUUID(req.PredecessorLeaseUUID)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	if _, err = parseLeaseUUID(req.LeaseUUID); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	if _, err = parseLeaseUUID(req.FencingToken); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	requester, err := parseLeaseUUID(req.RequesterActorUUID)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	if req.AcquisitionSource != "agent" && req.AcquisitionSource != "human" {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("acquisition_source must be agent or human")
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" || len(reason) > 1000 {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("reason must contain 1 to 1000 characters")
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	var old protocol.MutationLease
	var id, token, actor, repo, ws, predecessor, root []byte
	var granted, renewed, expires, hard, state string
	var depth uint64
	if err = d.db.QueryRowContext(ctx, `SELECT lease_uuid,fencing_token,actor_uuid,generation,repository_uuid,workspace_uuid,intent_id,tool_call_id,granted_at,renewed_at,expires_at,hard_deadline,state,predecessor_lease_uuid,COALESCE(root_lease_uuid,lease_uuid),takeover_depth FROM mutation_leases WHERE lease_uuid=?`, pred).Scan(&id, &token, &actor, &old.Generation, &repo, &ws, &old.IntentID, &old.ToolCallID, &granted, &renewed, &expires, &hard, &state, &predecessor, &root, &depth); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("predecessor lease not found")
	}
	if state != protocol.LeaseActive {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("predecessor lease is not active")
	}
	var requesterState, heartbeat string
	if err = d.db.QueryRowContext(ctx, `SELECT state,heartbeat_at FROM actors WHERE session_uuid=? AND generation=?`, requester, req.RequesterGeneration).Scan(&requesterState, &heartbeat); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("requester actor generation is not registered")
	}
	hb, err := parseLeaseTime(heartbeat)
	if err != nil || requesterState == "dead" || now.Sub(hb) > actorHeartbeatFreshness {
		return protocol.MutationLeaseTakeoverResult{}, errors.New("requester heartbeat is stale")
	}
	old.LeaseUUID, old.FencingToken, old.ActorUUID = uuidString(id), uuidString(token), uuidString(actor)
	old.RepositoryUUID, old.WorkspaceUUID = uuidString(repo), uuidString(ws)
	old.State = protocol.LeaseSuperseded
	old.PredecessorLeaseUUID = uuidString(predecessor)
	old.RootLeaseUUID = uuidString(root)
	old.TakeoverDepth = depth
	old.SupersededByLeaseUUID = req.LeaseUUID
	old.GrantedAt, err = parseLeaseTime(granted)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	old.RenewedAt, err = parseLeaseTime(renewed)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	old.ExpiresAt, err = parseLeaseTime(expires)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	old.HardDeadline, err = parseLeaseTime(hard)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	rows, err := d.db.QueryContext(ctx, `SELECT path FROM mutation_lease_paths WHERE lease_uuid=? ORDER BY path`, pred)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	defer closeLeaseRows(rows)
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return protocol.MutationLeaseTakeoverResult{}, err
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	hardDeadline, err := parseLeaseTime(hard)
	if err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	expiresAt := now.Add(2 * time.Minute)
	if expiresAt.After(hardDeadline) {
		expiresAt = hardDeadline
	}
	successor := protocol.MutationLease{LeaseUUID: req.LeaseUUID, FencingToken: req.FencingToken, ActorUUID: req.RequesterActorUUID, Generation: req.RequesterGeneration, RepositoryUUID: old.RepositoryUUID, WorkspaceUUID: old.WorkspaceUUID, IntentID: old.IntentID, ToolCallID: old.ToolCallID, Paths: paths, GrantedAt: now, RenewedAt: now, ExpiresAt: expiresAt, HardDeadline: hardDeadline, State: protocol.LeaseActive, PredecessorLeaseUUID: req.PredecessorLeaseUUID, RootLeaseUUID: old.RootLeaseUUID, TakeoverDepth: old.TakeoverDepth + 1}
	body := fmt.Sprintf("Explicit lease takeover: lease %s superseded by %s. Source=%s. Reason: %s", req.PredecessorLeaseUUID, req.LeaseUUID, req.AcquisitionSource, reason)
	message := protocol.Message{ID: "lease-takeover:" + req.LeaseUUID, Kind: "lease.takeover", From: req.RequesterActorUUID, To: old.ActorUUID, Body: body, CreatedAt: now}
	if err := d.canonicalLeaseMessage(ctx, &message); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	event := protocol.MutationLeaseLifecycleEvent{Lease: successor, Action: "takeover", Reason: reason, PredecessorLease: &old, SuccessorLease: &successor, Message: &message, PreviousHolderUUID: old.ActorUUID, NotificationBody: body, RequesterActorUUID: req.RequesterActorUUID, RequesterGeneration: req.RequesterGeneration, AcquisitionSource: req.AcquisitionSource, WorkUnitUUID: req.WorkUnitUUID, CollisionID: req.CollisionID}
	if err := d.appendLeaseEvent(now, "lease.takeover", event); err != nil {
		d.projectionMu.Unlock()
		writeLocked = false
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	d.projectionMu.Unlock()
	writeLocked = false
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseTakeoverResult{}, err
	}
	return protocol.MutationLeaseTakeoverResult{Lease: successor, PreviousHolderUUID: old.ActorUUID, NotificationBody: body}, nil
}

// MutationLeaseAncestry returns the complete predecessor-to-successor lineage.
func (d *DB) canonicalLeaseMessage(ctx context.Context, message *protocol.Message) error {
	if message.GlobalSequence != 0 {
		return nil
	}
	if err := d.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(global_sequence),0)+1 FROM messages`).Scan(&message.GlobalSequence); err != nil {
		return err
	}
	if err := d.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(sender_sequence),0)+1 FROM messages WHERE from_actor=?`, uuidBlob(message.From)).Scan(&message.SenderSequence); err != nil {
		return err
	}
	if err := d.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(recipient_sequence),0)+1 FROM messages WHERE to_actor=?`, uuidBlob(message.To)).Scan(&message.RecipientSequence); err != nil {
		return err
	}
	return nil
}

// MutationLeaseAncestry returns the complete predecessor-to-successor lineage.
func (d *DB) MutationLeaseAncestry(ctx context.Context, leaseUUID string) (protocol.MutationLeaseAncestryResult, error) {
	current, err := parseLeaseUUID(leaseUUID)
	if err != nil {
		return protocol.MutationLeaseAncestryResult{}, err
	}
	var reverse []protocol.MutationLease
	for depth := 0; depth < 1024 && len(current) > 0; depth++ {
		var l protocol.MutationLease
		var id, token, actor, repo, ws, predecessor, root []byte
		var granted, renewed, expires, hard, state string
		var takeDepth uint64
		err = d.db.QueryRowContext(ctx, `SELECT lease_uuid,fencing_token,actor_uuid,generation,repository_uuid,workspace_uuid,intent_id,tool_call_id,granted_at,renewed_at,expires_at,hard_deadline,state,predecessor_lease_uuid,root_lease_uuid,takeover_depth FROM mutation_leases WHERE lease_uuid=?`, current).Scan(&id, &token, &actor, &l.Generation, &repo, &ws, &l.IntentID, &l.ToolCallID, &granted, &renewed, &expires, &hard, &state, &predecessor, &root, &takeDepth)
		if err != nil {
			return protocol.MutationLeaseAncestryResult{}, err
		}
		l.LeaseUUID, l.FencingToken, l.ActorUUID = uuidString(id), uuidString(token), uuidString(actor)
		l.RepositoryUUID, l.WorkspaceUUID = uuidString(repo), uuidString(ws)
		l.State, l.PredecessorLeaseUUID, l.RootLeaseUUID, l.TakeoverDepth = state, uuidString(predecessor), uuidString(root), takeDepth
		l.GrantedAt, err = parseLeaseTime(granted)
		if err != nil {
			return protocol.MutationLeaseAncestryResult{}, err
		}
		l.RenewedAt, err = parseLeaseTime(renewed)
		if err != nil {
			return protocol.MutationLeaseAncestryResult{}, err
		}
		l.ExpiresAt, err = parseLeaseTime(expires)
		if err != nil {
			return protocol.MutationLeaseAncestryResult{}, err
		}
		l.HardDeadline, err = parseLeaseTime(hard)
		if err != nil {
			return protocol.MutationLeaseAncestryResult{}, err
		}
		reverse = append(reverse, l)
		current = predecessor
	}
	if len(current) != 0 {
		return protocol.MutationLeaseAncestryResult{}, errors.New("lease ancestry exceeds maximum depth")
	}
	result := protocol.MutationLeaseAncestryResult{Leases: make([]protocol.MutationLease, len(reverse))}
	for i := range reverse {
		result.Leases[len(reverse)-1-i] = reverse[i]
	}
	return result, nil
}

// ReleaseMutationLease releases a lease using its fencing credentials.
//
//nolint:cyclop // Fencing, projection, and journal admission are one atomic lifecycle operation.
//nolint:gocritic // Protocol request values intentionally remain value-shaped at the API boundary.
func (d *DB) ReleaseMutationLease(ctx context.Context, req *protocol.MutationLeaseReleaseRequest) (bool, error) {
	d.leaseCommandMu.Lock()
	defer d.leaseCommandMu.Unlock()
	if err := d.waitLeaseProjection(ctx); err != nil {
		return false, err
	}
	d.projectionMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			d.projectionMu.Unlock()
		}
	}()
	id, err := parseLeaseUUID(req.LeaseUUID)
	if err != nil {
		return false, err
	}
	token, err := parseLeaseUUID(req.FencingToken)
	if err != nil {
		return false, err
	}
	actor, err := parseLeaseUUID(req.ActorUUID)
	if err != nil {
		return false, err
	}
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer rollbackLeaseTx(tx)
	lease, err := loadLease(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if lease.FencingToken != uuidString(token) || lease.ActorUUID != uuidString(actor) || lease.Generation != req.Generation || (lease.State != protocol.LeaseActive && lease.State != protocol.LeaseWaiting) {
		return false, nil
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	state := protocol.LeaseReleased
	action := "released"
	if lease.State == protocol.LeaseWaiting {
		state = protocol.LeaseCancelled
		action = "canceled"
	}
	terminal := now
	lease.State = state
	lease.ExpiresAt = terminal
	lease.TerminalAt = &terminal
	lease.BlockingLeaseUUID, lease.BlockingRootLeaseUUID, lease.CollisionID = "", "", ""
	if err := d.appendLeaseLifecycle(action, &lease, now, "holder release"); err != nil {
		return false, err
	}
	rollbackLeaseTx(tx)
	d.projectionMu.Unlock()
	writeLocked = false
	if err := d.waitLeaseProjection(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func closeLeaseRows(rows *sql.Rows) {
	if err := rows.Close(); err != nil {
		_ = err
	}
}

func (d *DB) waitLeaseProjection(ctx context.Context) error {
	if waiter, ok := d.leaseAppender.(interface{ WaitForCurrent(context.Context) error }); ok {
		return waiter.WaitForCurrent(ctx)
	}
	return nil
}

func rollbackLeaseTx(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		_ = err
	}
}
