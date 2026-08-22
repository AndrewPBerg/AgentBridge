package state

import (
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestCheckpointRequestIsIdempotentAndCapturesRange(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	actor := register(t, engine, "pi:checkpoint")
	request := protocol.CheckpointRequest{ID: "checkpoint-request-1", Actor: actor.Address, CheckpointKind: "settled"}
	first, err := engine.RequestCheckpoint(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.RequestCheckpoint(request)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.JournalStart != first.JournalEnd || first.RepositoryUUID != actor.RepositoryUUID || first.WorkspaceUUID != actor.WorkspaceUUID {
		t.Fatalf("checkpoint request = %#v, duplicate = %#v", first, second)
	}
	if len(journal.events) != 2 {
		t.Fatalf("journal events = %d, want registration plus one request", len(journal.events))
	}
}
