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
	if result, blocked := renewActorCheck(ctx, tx, id, token, actor, &req, now); blocked {
		return result, nil
	}
	deadlineText, err := leaseDeadline(ctx, tx, id)
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	deadline, err := parseLeaseTime(deadlineText)
	if err != nil {
		return protocol.MutationLeaseResult{}, fmt.Errorf("parse lease hard deadline: %w", err)
	}
	expires := now.Add(2 * time.Minute)
	if expires.After(deadline) {
		expires = deadline
	}
	if !expires.After(now) {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "hard deadline reached"}, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE mutation_leases SET renewed_at=?,expires_at=? WHERE lease_uuid=? AND fencing_token=? AND state='active'`, now.Format(time.RFC3339Nano), expires.Format(time.RFC3339Nano), id, token); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	var repository, workspace []byte
	var grantedText string
	if err := tx.QueryRowContext(ctx, `SELECT repository_uuid,workspace_uuid,granted_at FROM mutation_leases WHERE lease_uuid=?`, id).Scan(&repository, &workspace, &grantedText); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	granted, err := parseLeaseTime(grantedText)
	if err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	lease := protocol.MutationLease{LeaseUUID: req.LeaseUUID, FencingToken: req.FencingToken, ActorUUID: req.ActorUUID, Generation: req.Generation, RepositoryUUID: uuidString(repository), WorkspaceUUID: uuidString(workspace), IntentID: req.IntentID, ToolCallID: req.ToolCallID, GrantedAt: granted, RenewedAt: now, ExpiresAt: expires, HardDeadline: deadline, State: protocol.LeaseActive}
	if err := d.appendLeaseLifecycle("renewed", &lease, now, "lease renewal"); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	d.projectionMu.Unlock()
	writeLocked = false
	if err := d.waitLeaseProjection(ctx); err != nil {
		return protocol.MutationLeaseResult{}, err
	}
	lease, err = loadLease(ctx, d.db, id)
	if err != nil {
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

func renewActorCheck(ctx context.Context, tx *sql.Tx, id, token, actor []byte, req *protocol.MutationLeaseRequest, now time.Time) (protocol.MutationLeaseResult, bool) {
	var state, heartbeat string
	if err := tx.QueryRowContext(ctx, `SELECT a.state,a.heartbeat_at FROM actors a JOIN mutation_leases l ON l.actor_uuid=a.session_uuid AND l.generation=a.generation WHERE l.lease_uuid=? AND l.fencing_token=? AND l.actor_uuid=? AND l.generation=? AND l.state='active'`, id, token, actor, req.Generation).Scan(&state, &heartbeat); err != nil {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "stale fencing token or actor generation"}, true
	}
	hb, err := parseLeaseTime(heartbeat)
	if err != nil || state == "dead" || now.Sub(hb) > actorHeartbeatFreshness {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "actor heartbeat is stale"}, true
	}
	var intentEnd string
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(completed_at,'') FROM mutations WHERE id=? AND actor=? AND session_generation=? AND tool_call_id=?`, req.IntentID, actor, req.Generation, req.ToolCallID).Scan(&intentEnd); err != nil || intentEnd != "" {
		return protocol.MutationLeaseResult{Decision: protocol.LeaseBlock, Reason: "original intent is not active"}, true
	}
	return protocol.MutationLeaseResult{}, false
}

func leaseDeadline(ctx context.Context, tx *sql.Tx, id []byte) (string, error) {
	var deadline string
	if err := tx.QueryRowContext(ctx, `SELECT hard_deadline FROM mutation_leases WHERE lease_uuid=?`, id).Scan(&deadline); err != nil {
		return "", err
	}
	return deadline, nil
}

func loadLease(ctx context.Context, db *sql.DB, id []byte) (protocol.MutationLease, error) {
	var lease protocol.MutationLease
	var leaseID, leaseToken, leaseActor, repo, workspace []byte
	var granted, renewed, expires, hard string
	if err := db.QueryRowContext(ctx, `SELECT lease_uuid,fencing_token,actor_uuid,generation,repository_uuid,workspace_uuid,intent_id,tool_call_id,granted_at,renewed_at,expires_at,hard_deadline,state FROM mutation_leases WHERE lease_uuid=?`, id).Scan(&leaseID, &leaseToken, &leaseActor, &lease.Generation, &repo, &workspace, &lease.IntentID, &lease.ToolCallID, &granted, &renewed, &expires, &hard, &lease.State); err != nil {
		return protocol.MutationLease{}, fmt.Errorf("read renewed lease: %w", err)
	}
	lease.LeaseUUID, lease.FencingToken, lease.ActorUUID = uuidString(leaseID), uuidString(leaseToken), uuidString(leaseActor)
	lease.RepositoryUUID, lease.WorkspaceUUID = uuidString(repo), uuidString(workspace)
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
	lease.Paths, err = loadLeasePaths(ctx, db, id)
	if err != nil {
		return lease, err
	}
	var blocking, root, collision []byte
	if err := db.QueryRowContext(ctx, `SELECT blocking_lease_uuid,blocking_root_lease_uuid,COALESCE(collision_id,X'') FROM mutation_lease_blockers WHERE waiting_lease_uuid=? ORDER BY blocking_lease_uuid LIMIT 1`, id).Scan(&blocking, &root, &collision); err == nil {
		lease.BlockingLeaseUUID, lease.BlockingRootLeaseUUID = uuidString(blocking), uuidString(root)
		if len(collision) == 16 {
			lease.CollisionID = uuidString(collision)
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return lease, err
	}
	return lease, nil
}

func loadLeasePaths(ctx context.Context, db *sql.DB, id []byte) ([]string, error) {
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
	workspaceBlob, err := parseLeaseUUID(workspace)
	if err != nil {
		return nil, err
	}
	rows, err := d.db.QueryContext(ctx, `SELECT lease_uuid FROM mutation_leases WHERE actor_uuid=? AND workspace_uuid=? AND state IN ('active','waiting') ORDER BY granted_at, lease_uuid`, actorBlob, workspaceBlob)
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
