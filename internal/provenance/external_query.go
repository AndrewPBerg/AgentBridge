package provenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

// ExternalChangeRecord is the normalized read model for an external observation.
type ExternalChangeRecord struct {
	ID                string          `json:"external_change_uuid"`
	RepositoryUUID    string          `json:"repository_uuid"`
	WorkspaceUUID     string          `json:"workspace_uuid"`
	UnknownActor      string          `json:"unknown_actor_uuid"`
	IntervalStartedAt string          `json:"interval_started_at"`
	IntervalEndedAt   string          `json:"interval_ended_at"`
	ContinuityState   string          `json:"continuity_state"`
	ChangeKind        string          `json:"change_kind"`
	Path              string          `json:"path"`
	WatchmanClock     string          `json:"watchman_clock"`
	Before            json.RawMessage `json:"before,omitempty"`
	After             json.RawMessage `json:"after,omitempty"`
	RelatedIntentIDs  []string        `json:"related_intent_ids,omitempty"`
	EventSequence     uint64          `json:"event_sequence"`
}

// ListExternalChanges returns recent external observations in one workspace.
//
//nolint:cyclop,gocognit // row hydration includes normalized path and intent relations.
func (d *DB) ListExternalChanges(workspaceUUID string, limit int) ([]ExternalChangeRecord, error) {
	if protocol.ValidateUUID(workspaceUUID) != nil {
		return nil, errors.New("workspace_uuid: invalid UUID")
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := d.db.QueryContext(context.Background(), `SELECT external_change_uuid,repository_uuid,workspace_uuid,unknown_actor_uuid,interval_started_at,interval_ended_at,continuity_state,change_kind,watchman_clock,COALESCE(before_json,''),COALESCE(after_json,''),event_sequence FROM external_changes WHERE workspace_uuid=? ORDER BY interval_started_at DESC,event_sequence DESC LIMIT ?`, uuidBlob(workspaceUUID), limit)
	if err != nil {
		return nil, err
	}
	defer closeRows(rows)
	result := []ExternalChangeRecord{}
	for rows.Next() {
		var id, repo, workspace, unknown []byte
		var before, after string
		var record ExternalChangeRecord
		if err := rows.Scan(&id, &repo, &workspace, &unknown, &record.IntervalStartedAt, &record.IntervalEndedAt, &record.ContinuityState, &record.ChangeKind, &record.WatchmanClock, &before, &after, &record.EventSequence); err != nil {
			return nil, err
		}
		if before != "" {
			record.Before = json.RawMessage(before)
		}
		if after != "" {
			record.After = json.RawMessage(after)
		}
		record.ID, record.RepositoryUUID, record.WorkspaceUUID, record.UnknownActor = uuidString(id), uuidString(repo), uuidString(workspace), uuidString(unknown)
		if err := d.db.QueryRowContext(context.Background(), `SELECT path FROM external_change_paths WHERE external_change_uuid=?`, id).Scan(&record.Path); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		intentRows, err := d.db.QueryContext(context.Background(), `SELECT intent_id FROM external_change_intents WHERE external_change_uuid=? ORDER BY intent_id`, id)
		if err != nil {
			return nil, err
		}
		for intentRows.Next() {
			var value string
			if err := intentRows.Scan(&value); err != nil {
				closeRows(intentRows)
				return nil, err
			}
			record.RelatedIntentIDs = append(record.RelatedIntentIDs, value)
		}
		if err := intentRows.Err(); err != nil {
			closeRows(intentRows)
			return nil, err
		}
		closeRows(intentRows)
		result = append(result, record)
	}
	return result, rows.Err()
}

// WorkspaceContinuity returns the latest projected watch continuity state.
func (d *DB) WorkspaceContinuity(workspaceUUID string) (protocol.WatchContinuity, error) {
	if protocol.ValidateUUID(workspaceUUID) != nil {
		return protocol.WatchContinuity{}, errors.New("workspace_uuid: invalid UUID")
	}
	var repo, workspace []byte
	var status protocol.WatchContinuity
	var at, clock string
	err := d.db.QueryRowContext(context.Background(), `SELECT workspace_uuid,repository_uuid,state,at,COALESCE(watchman_clock,'') FROM workspace_continuity WHERE workspace_uuid=?`, uuidBlob(workspaceUUID)).Scan(&workspace, &repo, &status.State, &at, &clock)
	if err != nil {
		return protocol.WatchContinuity{}, err
	}
	status.RepositoryUUID, status.WorkspaceUUID = uuidString(repo), uuidString(workspace)
	status.At = parseTime(at)
	status.WatchmanClock = clock
	return status, nil
}
