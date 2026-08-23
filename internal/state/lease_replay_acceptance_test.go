package state

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func leaseReplayEvent(t *testing.T, sequence uint64, value *protocol.MutationLeaseLifecycleEvent) protocol.Event {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Event{Version: protocol.Version, Sequence: sequence, Type: "lease.takeover", At: value.Lease.GrantedAt, Data: data}
}

func TestLeaseReplayRejectsUnknownActorWithoutGhost(t *testing.T) {
	unknown := testActorUUID("unknown-lease-replay")
	lease := protocol.MutationLease{LeaseUUID: testActorUUID("lease"), FencingToken: testActorUUID("token"), ActorUUID: unknown, RepositoryUUID: testActorUUID("repo"), WorkspaceUUID: testActorUUID("workspace"), GrantedAt: time.Now().UTC(), RenewedAt: time.Now().UTC(), ExpiresAt: time.Now().Add(time.Minute), HardDeadline: time.Now().Add(time.Hour), State: protocol.LeaseActive}
	engine, err := New(&memoryJournal{}, []protocol.Event{leaseReplayEvent(t, 1, &protocol.MutationLeaseLifecycleEvent{Action: "takeover", Lease: lease, SuccessorLease: &lease})}, Options{})
	if err == nil {
		for _, actor := range engine.Sessions(true) {
			if actor.Address == unknown {
				t.Fatal("lease replay created a ghost actor")
			}
		}
		t.Fatal("lease replay accepted an unknown actor")
	}
}

//nolint:cyclop // End-to-end live/replay ordering acceptance scenario.
func TestInterleavedMailAndTakeoverNoticesHaveStableNonzeroOrderLiveAndReplay(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	sender := testActorUUID("mail-sender")
	recipient := testActorUUID("mail-recipient")
	register(t, engine, sender)
	register(t, engine, recipient)
	if _, err := engine.Send(protocol.SendParams{From: sender, To: recipient, Body: "ordinary-1"}); err != nil {
		t.Fatal(err)
	}
	seq := uint64(len(journal.events) + 1)
	external := make([]protocol.Event, 0, 2)
	for i, body := range []string{"takeover-1", "takeover-2"} {
		message := protocol.Message{ID: "takeover-message-" + string(rune('1'+i)), Kind: "lease.takeover", From: sender, To: recipient, Body: body, CreatedAt: time.Now().UTC(), RecipientSequence: uint64(i + 2)}
		lease := protocol.MutationLease{LeaseUUID: testActorUUID("notice-" + body), FencingToken: testActorUUID("notice-token-" + body), ActorUUID: sender, RepositoryUUID: testActorUUID("repo-" + body), WorkspaceUUID: testActorUUID("workspace-" + body), GrantedAt: message.CreatedAt, RenewedAt: message.CreatedAt, ExpiresAt: message.CreatedAt.Add(time.Minute), HardDeadline: message.CreatedAt.Add(time.Hour), State: protocol.LeaseActive}
		event := leaseReplayEvent(t, seq, &protocol.MutationLeaseLifecycleEvent{Action: "takeover", Lease: lease, SuccessorLease: &lease, Message: &message})
		if err := engine.ApplyExternal(event); err != nil {
			t.Fatal(err)
		}
		external = append(external, event)
		seq++
	}
	live := mustPoll(t, engine, recipient)
	if len(live) != 3 || live[0].Body == "" || live[1].Body == "" || live[2].Body == "" {
		t.Fatalf("live order = %#v", live)
	}
	replayEvents := append(append([]protocol.Event(nil), journal.events...), external...)
	replayed, err := New(&memoryJournal{}, replayEvents, Options{})
	if err != nil {
		t.Fatal(err)
	}
	replayedMessages := mustPoll(t, replayed, recipient)
	if len(replayedMessages) != 3 {
		t.Fatalf("replayed messages = %#v", replayedMessages)
	}
	for i := range live {
		if live[i].ID != replayedMessages[i].ID || live[i].RecipientSequence == 0 || replayedMessages[i].RecipientSequence == 0 {
			t.Fatalf("non-deterministic/nonzero order live=%#v replay=%#v", live, replayedMessages)
		}
	}
}
