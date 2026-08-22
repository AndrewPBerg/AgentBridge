package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type memoryJournal struct {
	mu     sync.Mutex
	events []protocol.Event
	fail   bool
	failAt int
	calls  int
}

func (j *memoryJournal) Append(event protocol.Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls++
	if j.fail || (j.failAt > 0 && j.calls == j.failAt) {
		return errors.New("simulated journal failure")
	}
	copy := event
	copy.Data = append(json.RawMessage(nil), event.Data...)
	j.events = append(j.events, copy)
	return nil
}

func newTestEngine(t *testing.T) (*Engine, *memoryJournal, *time.Time) {
	t.Helper()
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	journal := &memoryJournal{}
	engine, err := New(journal, nil, Options{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return engine, journal, &now
}

func register(t *testing.T, engine *Engine, address string) protocol.Actor {
	t.Helper()
	actor, err := engine.Register(protocol.Actor{
		Address:     address,
		Harness:     "pi",
		SessionUUID: address[3:],
		CWD:         "/repo",
		State:       "active",
	})
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestMailboxOrdersSenderAssignedBurstSequences(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:sender")
	register(t, engine, "pi:recipient")

	for _, sequence := range []uint64{4, 5, 1, 2, 3} {
		_, err := engine.Send(protocol.SendParams{
			From:           "pi:sender",
			To:             "pi:recipient",
			Body:           fmt.Sprintf("K%d", sequence),
			ClientSequence: sequence,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll("pi:recipient", 0)
	if err != nil {
		t.Fatal(err)
	}
	for index, message := range messages {
		want := fmt.Sprintf("K%d", index+1)
		if message.Body != want {
			t.Fatalf("message %d = %q, want %q", index, message.Body, want)
		}
	}
}

func TestMailboxPreservesSenderOrderAcrossInterleavedSenders(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	register(t, engine, "pi:recipient")
	for _, value := range []struct {
		from     string
		sequence uint64
		body     string
	}{
		{from: "pi:a", sequence: 2, body: "A2"},
		{from: "pi:b", sequence: 1, body: "B1"},
		{from: "pi:a", sequence: 1, body: "A1"},
	} {
		if _, err := engine.Send(protocol.SendParams{
			From: value.from, To: "pi:recipient", Body: value.body, ClientSequence: value.sequence, SessionGeneration: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll("pi:recipient", 0)
	if err != nil {
		t.Fatal(err)
	}
	positions := make(map[string]int)
	for index, message := range messages {
		positions[message.Body] = index
	}
	if positions["A1"] >= positions["A2"] {
		t.Fatalf("sender order lost across interleave: %#v", messages)
	}
}

func TestMailboxOrdersGenerationsBeforeResetClientSequence(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:sender")
	register(t, engine, "pi:recipient")
	for _, value := range []struct {
		generation uint64
		sequence   uint64
		body       string
	}{
		{generation: 100, sequence: 9, body: "before-reload"},
		{generation: 200, sequence: 1, body: "after-reload"},
	} {
		if _, err := engine.Send(protocol.SendParams{
			From:              "pi:sender",
			To:                "pi:recipient",
			Body:              value.body,
			ClientSequence:    value.sequence,
			SessionGeneration: value.generation,
		}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll("pi:recipient", 0)
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Body != "before-reload" || messages[1].Body != "after-reload" {
		t.Fatalf("generation order was lost: %#v", messages)
	}
}

func TestSendIdempotencyKeyPreventsDuplicateRetry(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:sender")
	register(t, engine, "pi:recipient")
	params := protocol.SendParams{ID: "pi:sender:1:1", From: "pi:sender", To: "pi:recipient", Body: "once"}
	first, err := engine.Send(params)
	if err != nil {
		t.Fatal(err)
	}
	second, err := engine.Send(params)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.GlobalSequence != second.GlobalSequence {
		t.Fatalf("retry created a new message: first=%#v second=%#v", first, second)
	}
	messages, _ := engine.Poll("pi:recipient", 0)
	if len(messages) != 1 {
		t.Fatalf("idempotent retry produced %d messages", len(messages))
	}
	conflict := params
	conflict.Body = "different"
	if _, err := engine.Send(conflict); err == nil {
		t.Fatal("reusing an idempotency key with different content succeeded")
	}
}

func TestGitHeadSelectorAddressesActiveActor(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:sender")
	actor := register(t, engine, "pi:git-worker")
	actor.Git = &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: "/repo", Head: "abcdef123456"}
	if _, err := engine.Register(actor); err != nil {
		t.Fatal(err)
	}
	message, err := engine.Send(protocol.SendParams{From: "pi:sender", To: "@git:abcdef", Body: "git-aware"})
	if err != nil {
		t.Fatal(err)
	}
	if message.To != "pi:git-worker" {
		t.Fatalf("git selector resolved to %s", message.To)
	}
}

func TestCanonicalAddressAcceptsMailWhileActorIsStale(t *testing.T) {
	engine, _, now := newTestEngine(t)
	register(t, engine, "pi:sender")
	register(t, engine, "pi:offline")
	*now = now.Add(time.Minute)
	if _, err := engine.Send(protocol.SendParams{From: "pi:sender", To: "@pi:offline", Body: "durable"}); err != nil {
		t.Fatalf("canonical stale delivery failed: %v", err)
	}
	messages, err := engine.Poll("pi:offline", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Body != "durable" {
		t.Fatalf("unexpected mailbox: %#v", messages)
	}
}

func TestCollisionLifecycleDeduplicatesAndCanResolve(t *testing.T) {
	engine, _, now := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	base := func(id, actor string) protocol.Intent {
		return protocol.Intent{
			ID:         id,
			Actor:      actor,
			ToolCallID: id,
			Tool:       "edit",
			Operation:  "edit",
			Paths:      []string{"/repo/shared.go"},
			CWD:        "/repo",
		}
	}
	if collisions, err := engine.BeginIntent(base("a-1", "pi:a")); err != nil || len(collisions) != 0 {
		t.Fatalf("first intent: collisions=%v err=%v", collisions, err)
	}
	collisions, err := engine.BeginIntent(base("b-1", "pi:b"))
	if err != nil || len(collisions) != 1 {
		t.Fatalf("second intent: collisions=%v err=%v", collisions, err)
	}
	collision := collisions[0]
	if collision.State != protocol.CollisionOpen {
		t.Fatalf("state = %s", collision.State)
	}
	if messages, _ := engine.Poll("pi:a", 0); len(messages) != 1 || messages[0].CollisionID != collision.ID {
		t.Fatalf("missing collision signal for a: %#v", messages)
	}
	if messages, _ := engine.Poll("pi:b", 0); len(messages) != 1 || messages[0].CollisionID != collision.ID {
		t.Fatalf("missing collision signal for b: %#v", messages)
	}

	*now = now.Add(time.Second)
	if _, err := engine.Send(protocol.SendParams{From: "pi:a", To: "pi:b", Body: "I will yield"}); err != nil {
		t.Fatal(err)
	}
	if got := engine.collisions[collision.ID].State; got != protocol.CollisionNegotiating {
		t.Fatalf("direct communication state = %s", got)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: collision.ID,
		Actor:       "pi:a",
		State:       protocol.CollisionYielded,
		Owner:       "pi:b",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: collision.ID,
		Actor:       "pi:b",
		State:       protocol.CollisionResolved,
		Resolution:  "b owns shared.go",
	}); err != nil {
		t.Fatal(err)
	}

	*now = now.Add(time.Second)
	newCollisions, err := engine.BeginIntent(base("a-2", "pi:a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(newCollisions) != 1 || newCollisions[0].ID == collision.ID {
		t.Fatalf("expected a new collision after resolution, got %#v", newCollisions)
	}
}

func TestIntentRetryRepairsPartiallyPublishedCollisionSignals(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	if _, err := engine.BeginIntent(intent("a", "pi:a")); err != nil {
		t.Fatal(err)
	}
	// intent.started, collision.upserted, and the first signal succeed; the
	// second recipient's signal fails.
	journal.failAt = journal.calls + 4
	if _, err := engine.BeginIntent(intent("b", "pi:b")); err == nil {
		t.Fatal("partially published collision unexpectedly succeeded")
	}
	journal.failAt = 0
	if _, err := engine.BeginIntent(intent("b", "pi:b")); err != nil {
		t.Fatalf("retry did not repair collision delivery: %v", err)
	}
	for _, actor := range []string{"pi:a", "pi:b"} {
		messages, err := engine.Poll(actor, 0)
		if err != nil {
			t.Fatal(err)
		}
		var collisionSignals int
		for _, message := range messages {
			if message.Kind == "collision" {
				collisionSignals++
			}
		}
		if collisionSignals != 1 {
			t.Fatalf("%s has %d collision signals, want 1: %#v", actor, collisionSignals, messages)
		}
	}
}

func TestInvalidCollisionTransitionsAreRejected(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	_, _ = engine.BeginIntent(intent("a", "pi:a"))
	collisions, err := engine.BeginIntent(intent("b", "pi:b"))
	if err != nil {
		t.Fatal(err)
	}
	id := collisions[0].ID
	for _, transition := range []protocol.TransitionParams{
		{CollisionID: id, Actor: "pi:a", State: protocol.CollisionYielded},
		{CollisionID: id, Actor: "pi:a", State: protocol.CollisionYielded, Owner: "pi:a"},
		{CollisionID: id, Actor: "pi:a", State: protocol.CollisionResolved, Resolution: "too early"},
	} {
		if _, err := engine.Transition(transition); err == nil {
			t.Fatalf("invalid transition unexpectedly succeeded: %#v", transition)
		}
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: "pi:a", State: protocol.CollisionYielded, Owner: "pi:b",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: "pi:a", State: protocol.CollisionResolved, Resolution: "wrong actor",
	}); err == nil {
		t.Fatal("non-owner resolved yielded collision")
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: "pi:b", State: protocol.CollisionResolved,
	}); err == nil {
		t.Fatal("empty resolution unexpectedly succeeded")
	}
}

func TestSendFailureNeverLeavesDurablyEnqueuedMessage(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	_, _ = engine.BeginIntent(intent("a", "pi:a"))
	_, _ = engine.BeginIntent(intent("b", "pi:b"))
	before, _ := engine.Poll("pi:b", 0)
	journal.fail = true
	if _, err := engine.Send(protocol.SendParams{From: "pi:a", To: "pi:b", Body: "coordinate"}); err == nil {
		t.Fatal("send unexpectedly succeeded during journal failure")
	}
	journal.fail = false
	after, _ := engine.Poll("pi:b", 0)
	if len(after) != len(before) {
		t.Fatalf("failed send changed durable mailbox: before=%d after=%d", len(before), len(after))
	}
}

func TestAckSurvivesReplay(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	message, err := engine.Send(protocol.SendParams{From: "pi:a", To: "pi:b", Body: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Ack(protocol.AckParams{Actor: "pi:b", MessageIDs: []string{message.ID}}); err != nil {
		t.Fatal(err)
	}

	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := replayed.Poll("pi:b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if messages == nil || len(messages) != 0 {
		t.Fatalf("acked mailbox must be an empty array, got %#v", messages)
	}
}

func TestConcurrentSendsAreRaceSafeAndUnique(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, "pi:a")
	register(t, engine, "pi:b")
	const count = 100
	var wg sync.WaitGroup
	for index := range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := engine.Send(protocol.SendParams{From: "pi:a", To: "pi:b", Body: fmt.Sprintf("%d", index)}); err != nil {
				t.Errorf("send: %v", err)
			}
		}()
	}
	wg.Wait()
	messages, err := engine.Poll("pi:b", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != count {
		t.Fatalf("got %d messages, want %d", len(messages), count)
	}
	seen := make(map[uint64]bool, count)
	for _, message := range messages {
		if seen[message.RecipientSequence] {
			t.Fatalf("duplicate recipient sequence %d", message.RecipientSequence)
		}
		seen[message.RecipientSequence] = true
	}
}

func TestSessionsMarksExpiredActorsDead(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	register(t, engine, "pi:waiting")
	*now = now.Add(defaultActorTTL + time.Second)

	actors := engine.Sessions(true)
	if len(actors) != 1 || actors[0].State != "dead" {
		t.Fatalf("expired actor state = %#v", actors)
	}
	if len(journal.events) != 2 {
		t.Fatalf("expected persisted death transition, got %d events", len(journal.events))
	}
	if live := engine.Sessions(false); len(live) != 0 {
		t.Fatalf("expired actor still considered live: %#v", live)
	}
}
