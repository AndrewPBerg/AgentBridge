package provenance

import (
	"context"
	"fmt"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

// RefreshActorPresence mirrors ephemeral in-memory presence into the SQL
// projection used by mutation-lease admission. Heartbeats are intentionally
// not journaled, so the actor.upserted projection alone cannot stay fresh.
func (d *DB) RefreshActorPresence(ctx context.Context, actor *protocol.Actor) error {
	actorID, err := parseLeaseUUID(actor.SessionUUID)
	if err != nil {
		return fmt.Errorf("parse actor session UUID: %w", err)
	}
	d.projectionMu.Lock()
	defer d.projectionMu.Unlock()
	_, err = d.db.ExecContext(ctx, `UPDATE actors SET state=?,cwd=?,heartbeat_at=? WHERE session_uuid=? AND generation=?`,
		actor.State, actor.CWD, actor.HeartbeatAt.UTC().Format(time.RFC3339Nano), actorID, actor.Generation)
	if err != nil {
		return fmt.Errorf("refresh actor presence: %w", err)
	}
	return nil
}
