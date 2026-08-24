package provenance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

// RenewMutationLease accepts both the legacy request-only form and the
// context-aware form while callers migrate to RenewMutationLeaseContext.
func (d *DB) RenewMutationLease(value any, requests ...protocol.MutationLeaseRequest) (protocol.MutationLeaseResult, error) {
	if ctx, ok := value.(context.Context); ok && len(requests) == 1 {
		return d.RenewMutationLeaseContext(ctx, requests[0])
	}
	if req, ok := value.(protocol.MutationLeaseRequest); ok && len(requests) == 0 {
		return d.RenewMutationLeaseContext(context.Background(), req)
	}
	return protocol.MutationLeaseResult{}, errors.New("invalid lease renewal arguments")
}

// RenewMutationLeaseContext extends a lease only while its original actor
// generation, heartbeat, intent, and fencing token are still valid.
//
//nolint:cyclop,gocritic // renewal validates several independent lease invariants in one transaction.
func (d *DB) RenewMutationLeaseContext(ctx context.Context, req protocol.MutationLeaseRequest) (protocol.MutationLeaseResult, error) {
	d.leaseCommandMu.Lock()
	defer d.leaseCommandMu.Unlock()
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	d.projectionMu.Lock()
	writeLocked := true
	defer func() {
		if writeLocked {
			d.projectionMu.Unlock()
		}
	}()
	id, token, actor, err := leaseCredentials(&req)
	if err != nil {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "invalid lease credentials"}, err
	}
	now := req.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	defer rollbackLeaseTx(tx)
	lease, result, blocked, err := loadRenewalLease(ctx, tx, id, token, actor, req.Generation, now)
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	if blocked {
		return result, nil
	}
	expires := now.Add(2 * time.Minute)
	if expires.After(lease.HardDeadline) {
		expires = lease.HardDeadline
	}
	if !expires.After(now) {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "hard deadline reached"}, nil
	}
	lease.RenewedAt, lease.ExpiresAt = now, expires
	if err := d.appendLeaseLifecycle("renewed", &lease, now, "lease renewal"); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	rollbackLeaseTx(tx)
	d.projectionMu.Unlock()
	writeLocked = false
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	return protocol.MutationLeaseResult{Decision: protocol.LeaseGrant, Lease: &lease, Reason: "renewed"}, nil
}

func leaseCredentials(req *protocol.MutationLeaseRequest) (id, token, actor []byte, err error) {
	id, err = parseLeaseUUID(req.LeaseUUID)
	if err != nil {
		return nil, nil, nil, err
	}
	token, err = parseLeaseUUID(req.FencingToken)
	if err != nil {
		return nil, nil, nil, err
	}
	actor, err = parseLeaseUUID(req.ActorUUID)
	if err != nil {
		return nil, nil, nil, err
	}
	return id, token, actor, nil
}

//nolint:cyclop,nilerr // Renewal converts stale identity and liveness observations into fenced protocol blocks.
func loadRenewalLease(ctx context.Context, tx *sql.Tx, id, token, actor []byte, generation uint64, now time.Time) (protocol.MutationLease, protocol.MutationLeaseResult, bool, error) {
	lease, err := loadLease(ctx, tx, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return lease, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "stale fencing token or actor generation"}, true, nil
		}
		return lease, protocol.MutationLeaseResult{}, false, err
	}
	if lease.FencingToken != uuidString(token) || lease.ActorUUID != uuidString(actor) || lease.Generation != generation || lease.State != protocol.LeaseActive {
		return lease, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "stale fencing token or actor generation"}, true, nil
	}
	var actorState, heartbeat string
	if err := tx.QueryRowContext(ctx, `SELECT state,heartbeat_at FROM actors WHERE session_uuid=? AND generation=?`, actor, generation).Scan(&actorState, &heartbeat); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return lease, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "stale fencing token or actor generation"}, true, nil
		}
		return lease, protocol.MutationLeaseResult{}, false, err
	}
	heartbeatAt, err := parseLeaseTime(heartbeat)
	if err != nil || actorState == "dead" || now.Sub(heartbeatAt) > actorHeartbeatFreshness {
		return lease, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "actor heartbeat is stale"}, true, nil
	}
	var intentEnd string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(completed_at,'') FROM mutations WHERE id=? AND actor=? AND session_generation=? AND tool_call_id=?`, lease.IntentID, actor, lease.Generation, lease.ToolCallID).Scan(&intentEnd); err != nil || intentEnd != "" {
		return lease, protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "mutation intent is missing or already completed"}, true, nil
	}
	return lease, protocol.MutationLeaseResult{}, false, nil
}

type leaseQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

//nolint:cyclop // Complete lease hydration validates each optional lineage and timestamp field.
func loadLease(ctx context.Context, db leaseQueryer, id []byte) (protocol.MutationLease, error) {
	var lease protocol.MutationLease
	var leaseID, leaseToken, leaseActor, repo, workspace, predecessor, root, successor []byte
	var granted, renewed, expires, hard, terminal string
	if err := db.QueryRowContext(ctx, `SELECT lease_uuid,fencing_token,actor_uuid,generation,repository_uuid,workspace_uuid,intent_id,tool_call_id,granted_at,renewed_at,expires_at,hard_deadline,state,COALESCE(predecessor_lease_uuid,X''),COALESCE(root_lease_uuid,lease_uuid),takeover_depth,COALESCE(superseded_by_lease_uuid,X''),COALESCE(terminal_at,'') FROM mutation_leases WHERE lease_uuid=?`, id).Scan(&leaseID, &leaseToken, &leaseActor, &lease.Generation, &repo, &workspace, &lease.IntentID, &lease.ToolCallID, &granted, &renewed, &expires, &hard, &lease.State, &predecessor, &root, &lease.TakeoverDepth, &successor, &terminal); err != nil {
		return protocol.MutationLease{}, fmt.Errorf("read lease: %w", err)
	}
	lease.LeaseUUID, lease.FencingToken, lease.ActorUUID = uuidString(leaseID), uuidString(leaseToken), uuidString(leaseActor)
	lease.RepositoryUUID, lease.WorkspaceUUID = uuidString(repo), uuidString(workspace)
	if len(predecessor) == 16 {
		lease.PredecessorLeaseUUID = uuidString(predecessor)
	}
	if len(root) == 16 {
		lease.RootLeaseUUID = uuidString(root)
	}
	if len(successor) == 16 {
		lease.SupersededByLeaseUUID = uuidString(successor)
	}
	var err error
	if lease.GrantedAt, err = parseLeaseTime(granted); err != nil {
		return lease, err
	}
	if lease.RenewedAt, err = parseLeaseTime(renewed); err != nil {
		return lease, err
	}
	if lease.ExpiresAt, err = parseLeaseTime(expires); err != nil {
		return lease, err
	}
	if lease.HardDeadline, err = parseLeaseTime(hard); err != nil {
		return lease, err
	}
	if terminal != "" {
		parsed, err := parseLeaseTime(terminal)
		if err != nil {
			return lease, err
		}
		lease.TerminalAt = &parsed
	}
	lease.Paths, err = loadLeasePaths(ctx, db, id)
	if err != nil {
		return lease, err
	}
	var blocking, blockingRoot, collision []byte
	if err := db.QueryRowContext(ctx, `SELECT blocking_lease_uuid,blocking_root_lease_uuid,COALESCE(collision_id,X'') FROM mutation_lease_blockers WHERE waiting_lease_uuid=? ORDER BY blocking_lease_uuid LIMIT 1`, id).Scan(&blocking, &blockingRoot, &collision); err == nil {
		lease.BlockingLeaseUUID, lease.BlockingRootLeaseUUID = uuidString(blocking), uuidString(blockingRoot)
		if len(collision) == 16 {
			lease.CollisionID = uuidString(collision)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return lease, err
	}
	return lease, nil
}

func loadLeasePaths(ctx context.Context, db leaseQueryer, id []byte) ([]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT path FROM mutation_lease_paths WHERE lease_uuid=? ORDER BY path`, id)
	if err != nil {
		return nil, fmt.Errorf("read lease paths: %w", err)
	}
	defer closeLeaseRows(rows)
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

// ListMutationLeases returns active and waiting claims for an actor/workspace.
func (d *DB) ListMutationLeases(ctx context.Context, actor, workspace string) ([]protocol.MutationLease, error) {
	actorBlob, err := parseLeaseUUID(actor)
	if err != nil {
		return nil, err
	}
	query := `SELECT lease_uuid FROM mutation_leases WHERE actor_uuid=? AND state IN ('active','waiting')`
	args := []any{actorBlob}
	if workspace != "" {
		workspaceBlob, err := parseLeaseUUID(workspace)
		if err != nil {
			return nil, err
		}
		query += ` AND workspace_uuid=?`
		args = append(args, workspaceBlob)
	}
	rows, err := d.db.QueryContext(ctx, query+` ORDER BY granted_at, lease_uuid`, args...)
	if err != nil {
		return nil, err
	}
	defer closeLeaseRows(rows)
	var result []protocol.MutationLease
	for rows.Next() {
		var id []byte
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		lease, err := loadLease(ctx, d.db, id)
		if err != nil {
			return nil, err
		}
		result = append(result, lease)
	}
	return result, rows.Err()
}
