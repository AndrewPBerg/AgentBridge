package provenance

import (
	"context"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestRefreshActorPresenceUpdatesEphemeralHeartbeat(t *testing.T) {
	db := openLeaseAcceptanceDB(t)
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	actorID := actorUUID("presence-refresh")
	seedLeaseActor(t, db, actorID, 7, at)

	refreshed := at.Add(time.Minute)
	if err := db.RefreshActorPresence(context.Background(), &protocol.Actor{
		SessionUUID: actorID,
		Generation:  7,
		State:       "waiting",
		CWD:         "/repo/subdir",
		HeartbeatAt: refreshed,
	}); err != nil {
		t.Fatal(err)
	}

	var state, cwd, heartbeat string
	if err := db.db.QueryRowContext(context.Background(), `SELECT state,cwd,heartbeat_at FROM actors WHERE session_uuid=?`, uuidBlob(actorID)).Scan(&state, &cwd, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if state != "waiting" || cwd != "/repo/subdir" || heartbeat != refreshed.Format(time.RFC3339Nano) {
		t.Fatalf("presence = state %q, cwd %q, heartbeat %q", state, cwd, heartbeat)
	}
}
