package provenance

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RetentionResult reports rows removed from the rebuildable projection.
type RetentionResult struct {
	ExternalChanges int64
	RawEvents       int64
	Cutoff          time.Time
}

// PruneExternalChangesBefore removes expired high-volume filesystem evidence
// from the Turso projection. The append-only journal remains the recovery
// authority, while current coordination state and checkpoint evidence are
// untouched.
func (d *DB) PruneExternalChangesBefore(ctx context.Context, cutoff time.Time) (RetentionResult, error) {
	result := RetentionResult{Cutoff: cutoff.UTC()}
	d.projectionMu.Lock()
	defer d.projectionMu.Unlock()
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return result, err
	}
	defer rollbackProjection(tx)
	cutoffText := result.Cutoff.Format(time.RFC3339Nano)
	deleted, err := tx.ExecContext(ctx, `DELETE FROM events WHERE sequence IN (SELECT event_sequence FROM external_changes WHERE interval_ended_at < ?)`, cutoffText)
	if err != nil {
		return result, fmt.Errorf("prune raw external-change events: %w", err)
	}
	if result.RawEvents, err = deleted.RowsAffected(); err != nil {
		return result, err
	}
	for _, statement := range []string{
		`DELETE FROM external_change_intents WHERE external_change_uuid IN (SELECT external_change_uuid FROM external_changes WHERE interval_ended_at < ?)`,
		`DELETE FROM external_change_paths WHERE external_change_uuid IN (SELECT external_change_uuid FROM external_changes WHERE interval_ended_at < ?)`,
	} {
		if _, err := tx.ExecContext(ctx, statement, cutoffText); err != nil {
			return result, fmt.Errorf("prune external-change relations: %w", err)
		}
	}
	deleted, err = tx.ExecContext(ctx, `DELETE FROM external_changes WHERE interval_ended_at < ?`, cutoffText)
	if err != nil {
		return result, fmt.Errorf("prune external changes: %w", err)
	}
	if result.ExternalChanges, err = deleted.RowsAffected(); err != nil {
		return result, err
	}
	if err := tx.Commit(); err != nil {
		return result, err
	}
	return result, nil
}
