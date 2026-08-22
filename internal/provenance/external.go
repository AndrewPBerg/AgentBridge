package provenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func projectExternalChange(tx *sql.Tx, event *protocol.Event) error {
	var change protocol.ExternalChange
	if err := json.Unmarshal(event.Data, &change); err != nil {
		return err
	}
	for _, value := range []string{change.ID, change.RepositoryUUID, change.WorkspaceUUID, change.UnknownActor} {
		if protocol.ValidateUUID(value) != nil {
			return nil //nolint:nilerr // projection backfill prunes invalid legacy UUID rows
		}
	}
	before, err := marshalOptional(change.Before)
	if err != nil {
		return err
	}
	after, err := marshalOptional(change.After)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(context.Background(), `INSERT OR IGNORE INTO external_changes(external_change_uuid,repository_uuid,workspace_uuid,unknown_actor_uuid,interval_started_at,interval_ended_at,continuity_state,change_kind,watchman_clock,before_json,after_json,data,event_sequence) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, uuidBlob(change.ID), uuidBlob(change.RepositoryUUID), uuidBlob(change.WorkspaceUUID), uuidBlob(change.UnknownActor), change.IntervalStartedAt.UTC().Format(time.RFC3339Nano), change.IntervalEndedAt.UTC().Format(time.RFC3339Nano), change.ContinuityState, change.ChangeKind, change.WatchmanClock, before, after, string(event.Data), event.Sequence)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(context.Background(), `INSERT OR IGNORE INTO external_change_paths(external_change_uuid,path) VALUES(?,?)`, uuidBlob(change.ID), change.Path); err != nil {
		return fmt.Errorf("project external path: %w", err)
	}
	for _, intent := range change.RelatedIntentIDs {
		if intent == "" {
			continue
		}
		if _, err := tx.ExecContext(context.Background(), `INSERT OR IGNORE INTO external_change_intents(external_change_uuid,intent_id) VALUES(?,?)`, uuidBlob(change.ID), intent); err != nil {
			return err
		}
	}
	return nil
}

func projectWatchContinuity(tx *sql.Tx, event *protocol.Event) error {
	var status protocol.WatchContinuity
	if err := json.Unmarshal(event.Data, &status); err != nil {
		return err
	}
	if protocol.ValidateUUID(status.RepositoryUUID) != nil || protocol.ValidateUUID(status.WorkspaceUUID) != nil {
		return nil //nolint:nilerr // projection backfill prunes invalid legacy UUID rows
	}
	state := event.Type[len("watch.continuity_"):]
	_, err := tx.ExecContext(context.Background(), `INSERT INTO workspace_continuity(workspace_uuid,repository_uuid,state,at,watchman_clock,event_sequence) VALUES(?,?,?,?,?,?) ON CONFLICT(workspace_uuid) DO UPDATE SET repository_uuid=excluded.repository_uuid,state=excluded.state,at=excluded.at,watchman_clock=excluded.watchman_clock,event_sequence=excluded.event_sequence`, uuidBlob(status.WorkspaceUUID), uuidBlob(status.RepositoryUUID), state, status.At.UTC().Format(time.RFC3339Nano), nullable(status.WatchmanClock), event.Sequence)
	return err
}
