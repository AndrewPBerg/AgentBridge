package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
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

//nolint:gocritic // protocol.Journal requires a value event.
func (j *memoryJournal) Append(event protocol.Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.calls++
	if j.fail || (j.failAt > 0 && j.calls == j.failAt) {
		return errors.New("simulated journal failure")
	}
	eventCopy := event
	eventCopy.Data = append(json.RawMessage(nil), event.Data...)
	j.events = append(j.events, eventCopy)
	return nil
}

func mustPoll(t *testing.T, engine *Engine, actor string) []protocol.Message {
	t.Helper()
	messages, err := engine.Poll(actor, 0)
	if err != nil {
		t.Fatal(err)
	}
	return messages
}

func mustBeginIntent(t *testing.T, engine *Engine, value *protocol.Intent) {
	t.Helper()
	if _, err := engine.BeginIntent(*value); err != nil {
		t.Fatal(err)
	}
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

func testActorUUID(label string) string {
	digest := sha256.Sum256([]byte(label))
	bytes := digest[:]
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return hex.EncodeToString(bytes[0:4]) + "-" + hex.EncodeToString(bytes[4:6]) + "-" + hex.EncodeToString(bytes[6:8]) + "-" + hex.EncodeToString(bytes[8:10]) + "-" + hex.EncodeToString(bytes[10:16])
}

func register(t *testing.T, engine *Engine, address string) protocol.Actor {
	t.Helper()
	if len(address) != 36 || address[8] != '-' {
		address = testActorUUID(address)
	}
	actor, err := engine.Register(protocol.Actor{
		Address:     address,
		Harness:     "pi",
		SessionUUID: address,
		CWD:         "/repo",
		State:       "active",
	})
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func TestRegisterRejectsNonCanonicalActorIdentity(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	valid := testActorUUID("valid")
	cases := []protocol.Actor{
		{Address: "pi:" + valid, SessionUUID: valid, Harness: "pi", CWD: "/repo"},
		{Address: valid, SessionUUID: "pi:" + valid, Harness: "pi", CWD: "/repo"},
		{Address: valid, SessionUUID: testActorUUID("other"), Harness: "pi", CWD: "/repo"},
	}
	for _, actor := range cases {
		if _, err := engine.Register(actor); err == nil {
			t.Fatalf("Register(%+v) unexpectedly succeeded", actor)
		}
	}
}

func TestMailboxSuppressesStealthActorsWithoutDroppingOrder(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	sender := testActorUUID("stealth-sender")
	recipient := testActorUUID("stealth-recipient")
	register(t, engine, sender)
	register(t, engine, recipient)
	if _, err := engine.Heartbeat(protocol.HeartbeatParams{Address: recipient, State: protocol.ActorStateStealth}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{"first", "second"} {
		if _, err := engine.Send(protocol.SendParams{From: sender, To: recipient, Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if messages := mustPoll(t, engine, recipient); len(messages) != 0 {
		t.Fatalf("stealth actor received messages: %#v", messages)
	}
	if _, err := engine.Heartbeat(protocol.HeartbeatParams{Address: recipient, State: "active"}); err != nil {
		t.Fatal(err)
	}
	messages := mustPoll(t, engine, recipient)
	if len(messages) != 2 || messages[0].Body != "first" || messages[1].Body != "second" {
		t.Fatalf("messages were not retained in order: %#v", messages)
	}
}

func TestMailboxOrdersSenderAssignedBurstSequences(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, testActorUUID("sender"))
	register(t, engine, testActorUUID("recipient"))

	for _, sequence := range []uint64{4, 5, 1, 2, 3} {
		_, err := engine.Send(protocol.SendParams{
			From:           testActorUUID("sender"),
			To:             testActorUUID("recipient"),
			Body:           fmt.Sprintf("K%d", sequence),
			ClientSequence: sequence,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll(testActorUUID("recipient"), 0)
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
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	register(t, engine, testActorUUID("recipient"))
	for _, value := range []struct {
		from     string
		sequence uint64
		body     string
	}{
		{from: testActorUUID("a"), sequence: 2, body: "A2"},
		{from: testActorUUID("b"), sequence: 1, body: "B1"},
		{from: testActorUUID("a"), sequence: 1, body: "A1"},
	} {
		if _, err := engine.Send(protocol.SendParams{
			From: value.from, To: testActorUUID("recipient"), Body: value.body, ClientSequence: value.sequence, SessionGeneration: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll(testActorUUID("recipient"), 0)
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
	register(t, engine, testActorUUID("sender"))
	register(t, engine, testActorUUID("recipient"))
	for _, value := range []struct {
		generation uint64
		sequence   uint64
		body       string
	}{
		{generation: 100, sequence: 9, body: "before-reload"},
		{generation: 200, sequence: 1, body: "after-reload"},
	} {
		if _, err := engine.Send(protocol.SendParams{
			From:              testActorUUID("sender"),
			To:                testActorUUID("recipient"),
			Body:              value.body,
			ClientSequence:    value.sequence,
			SessionGeneration: value.generation,
		}); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := engine.Poll(testActorUUID("recipient"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if messages[0].Body != "before-reload" || messages[1].Body != "after-reload" {
		t.Fatalf("generation order was lost: %#v", messages)
	}
}

func TestSendIdempotencyKeyPreventsDuplicateRetry(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, testActorUUID("sender"))
	register(t, engine, testActorUUID("recipient"))
	params := protocol.SendParams{ID: "sender:1:1", From: testActorUUID("sender"), To: testActorUUID("recipient"), Body: "once"}
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
	messages := mustPoll(t, engine, testActorUUID("recipient"))
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
	register(t, engine, testActorUUID("sender"))
	actor := register(t, engine, "git-worker")
	actor.Git = &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: "/repo", Head: "abcdef123456"}
	if _, err := engine.Register(actor); err != nil {
		t.Fatal(err)
	}
	message, err := engine.Send(protocol.SendParams{From: testActorUUID("sender"), To: "@abcdef", Body: "git-aware"})
	if err != nil {
		t.Fatal(err)
	}
	if message.To != actor.Address {
		t.Fatalf("git selector resolved to %s", message.To)
	}
}

func TestCanonicalAddressAcceptsMailWhileActorIsStale(t *testing.T) {
	engine, _, now := newTestEngine(t)
	register(t, engine, testActorUUID("sender"))
	offline := register(t, engine, testActorUUID("offline"))
	offline.Alias = "offline"
	if _, err := engine.Register(offline); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Minute)
	if _, err := engine.Send(protocol.SendParams{From: testActorUUID("sender"), To: offline.Address, Body: "durable"}); err != nil {
		t.Fatalf("canonical stale delivery failed: %v", err)
	}
	messages, err := engine.Poll(testActorUUID("offline"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Body != "durable" {
		t.Fatalf("unexpected mailbox: %#v", messages)
	}
}

//nolint:cyclop,gocognit // end-to-end test keeps setup and assertions together.
func TestCollisionLifecycleDeduplicatesAndCanResolve(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
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
	if collisions, err := engine.BeginIntent(base("a-1", testActorUUID("a"))); err != nil || len(collisions) != 0 {
		t.Fatalf("first intent: collisions=%v err=%v", collisions, err)
	}
	collisions, err := engine.BeginIntent(base("b-1", testActorUUID("b")))
	if err != nil || len(collisions) != 1 {
		t.Fatalf("second intent: collisions=%v err=%v", collisions, err)
	}
	collision := collisions[0]
	if collision.State != protocol.CollisionOpen {
		t.Fatalf("state = %s", collision.State)
	}
	if messages := mustPoll(t, engine, testActorUUID("a")); len(messages) != 1 || messages[0].CollisionID != collision.ID {
		t.Fatalf("missing collision signal for a: %#v", messages)
	}
	if messages := mustPoll(t, engine, testActorUUID("b")); len(messages) != 1 || messages[0].CollisionID != collision.ID {
		t.Fatalf("missing collision signal for b: %#v", messages)
	}

	*now = now.Add(time.Second)
	if _, err := engine.Send(protocol.SendParams{From: testActorUUID("a"), To: testActorUUID("b"), Body: "I will yield"}); err != nil {
		t.Fatal(err)
	}
	if got := engine.collisions[collision.ID].State; got != protocol.CollisionNegotiating {
		t.Fatalf("direct communication state = %s", got)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: collision.ID,
		Actor:       testActorUUID("a"),
		State:       protocol.CollisionYielded,
		Owner:       testActorUUID("b"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: collision.ID,
		Actor:       testActorUUID("b"),
		State:       protocol.CollisionResolved,
		Resolution:  "b owns shared.go",
	}); err != nil {
		t.Fatal(err)
	}
	var transitions []protocol.CollisionTransitionEvent
	for _, event := range journal.events {
		if event.Type != "collision.transitioned" {
			continue
		}
		value, err := decode[protocol.CollisionTransitionEvent](event)
		if err != nil {
			t.Fatal(err)
		}
		transitions = append(transitions, value)
	}
	if len(transitions) != 3 || transitions[2].To != protocol.CollisionResolved {
		t.Fatalf("collision transitions = %#v", transitions)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	replayedCollision := replayed.collisions[collision.ID]
	if replayedCollision.State != protocol.CollisionResolved || replayedCollision.ResolvedBy != testActorUUID("b") || replayedCollision.Resolution != "b owns shared.go" {
		t.Fatalf("replayed collision = %#v", replayedCollision)
	}

	*now = now.Add(time.Second)
	newCollisions, err := engine.BeginIntent(base("a-2", testActorUUID("a")))
	if err != nil {
		t.Fatal(err)
	}
	if len(newCollisions) != 1 || newCollisions[0].ID == collision.ID {
		t.Fatalf("expected a new collision after resolution, got %#v", newCollisions)
	}
}

func TestIntentRetryRepairsPartiallyPublishedCollisionSignals(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	if _, err := engine.BeginIntent(intent(testActorUUID("a"), testActorUUID("a"))); err != nil {
		t.Fatal(err)
	}
	// intent.started, collision.upserted, and the first signal succeed; the
	// second recipient's signal fails.
	journal.failAt = journal.calls + 4
	if _, err := engine.BeginIntent(intent(testActorUUID("b"), testActorUUID("b"))); err == nil {
		t.Fatal("partially published collision unexpectedly succeeded")
	}
	journal.failAt = 0
	if _, err := engine.BeginIntent(intent(testActorUUID("b"), testActorUUID("b"))); err != nil {
		t.Fatalf("retry did not repair collision delivery: %v", err)
	}
	for _, actor := range []string{testActorUUID("a"), testActorUUID("b")} {
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
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	firstIntent := intent(testActorUUID("a"), testActorUUID("a"))
	mustBeginIntent(t, engine, &firstIntent)
	collisions, err := engine.BeginIntent(intent(testActorUUID("b"), testActorUUID("b")))
	if err != nil {
		t.Fatal(err)
	}
	id := collisions[0].ID
	for _, transition := range []protocol.TransitionParams{
		{CollisionID: id, Actor: testActorUUID("a"), State: protocol.CollisionYielded},
		{CollisionID: id, Actor: testActorUUID("a"), State: protocol.CollisionYielded, Owner: testActorUUID("a")},
		{CollisionID: id, Actor: testActorUUID("a"), State: protocol.CollisionResolved, Resolution: "too early"},
	} {
		if _, err := engine.Transition(transition); err == nil {
			t.Fatalf("invalid transition unexpectedly succeeded: %#v", transition)
		}
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: testActorUUID("a"), State: protocol.CollisionYielded, Owner: testActorUUID("b"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: testActorUUID("a"), State: protocol.CollisionResolved, Resolution: "wrong actor",
	}); err == nil {
		t.Fatal("non-owner resolved yielded collision")
	}
	if _, err := engine.Transition(protocol.TransitionParams{
		CollisionID: id, Actor: testActorUUID("b"), State: protocol.CollisionResolved,
	}); err == nil {
		t.Fatal("empty resolution unexpectedly succeeded")
	}
}

func TestSendFailureNeverLeavesDurablyEnqueuedMessage(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{"/repo/x"}, CWD: "/repo"}
	}
	firstIntent := intent(testActorUUID("a"), testActorUUID("a"))
	secondIntent := intent(testActorUUID("b"), testActorUUID("b"))
	mustBeginIntent(t, engine, &firstIntent)
	mustBeginIntent(t, engine, &secondIntent)
	before := mustPoll(t, engine, testActorUUID("b"))
	journal.fail = true
	if _, err := engine.Send(protocol.SendParams{From: testActorUUID("a"), To: testActorUUID("b"), Body: "coordinate"}); err == nil {
		t.Fatal("send unexpectedly succeeded during journal failure")
	}
	journal.fail = false
	after := mustPoll(t, engine, testActorUUID("b"))
	if len(after) != len(before) {
		t.Fatalf("failed send changed durable mailbox: before=%d after=%d", len(before), len(after))
	}
}

func TestAckSurvivesReplay(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	message, err := engine.Send(protocol.SendParams{From: testActorUUID("a"), To: testActorUUID("b"), Body: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Ack(protocol.AckParams{Actor: testActorUUID("b"), MessageIDs: []string{message.ID}}); err != nil {
		t.Fatal(err)
	}

	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := replayed.Poll(testActorUUID("b"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if messages == nil || len(messages) != 0 {
		t.Fatalf("acked mailbox must be an empty array, got %#v", messages)
	}
}

func TestConcurrentSendsAreRaceSafeAndUnique(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	const count = 100
	var wg sync.WaitGroup
	for index := range count {
		wg.Go(func() {
			if _, err := engine.Send(protocol.SendParams{From: testActorUUID("a"), To: testActorUUID("b"), Body: strconv.Itoa(index)}); err != nil {
				t.Errorf("send: %v", err)
			}
		})
	}
	wg.Wait()
	messages, err := engine.Poll(testActorUUID("b"), 0)
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

func TestDeadCollisionNotifiesSurvivorAndReplays(t *testing.T) {
	engine, journal, _ := newTestEngine(t)
	register(t, engine, testActorUUID("a"))
	register(t, engine, testActorUUID("b"))
	intent := func(id, actor string) protocol.Intent {
		return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "write", Paths: []string{"/repo/shared.go"}, CWD: "/repo"}
	}
	if _, err := engine.BeginIntent(intent(testActorUUID("a"), testActorUUID("a"))); err != nil {
		t.Fatal(err)
	}
	collisions, err := engine.BeginIntent(intent(testActorUUID("b"), testActorUUID("b")))
	if err != nil || len(collisions) != 1 {
		t.Fatalf("collision = %#v, err = %v", collisions, err)
	}
	collision := collisions[0]
	if _, err := engine.Heartbeat(protocol.HeartbeatParams{Address: testActorUUID("b"), State: "dead"}); err != nil {
		t.Fatal(err)
	}
	messages, err := engine.Poll(testActorUUID("a"), 0)
	if err != nil {
		t.Fatal(err)
	}
	deadMessages := 0
	for _, message := range messages {
		if message.Kind == "collision-dead" {
			deadMessages++
			if message.CollisionID != collision.ID {
				t.Fatalf("dead notification collision = %q, want %q", message.CollisionID, collision.ID)
			}
		}
	}
	if deadMessages != 1 {
		t.Fatalf("dead notifications = %d, want 1: %#v", deadMessages, messages)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	if got := replayed.collisions[collision.ID].DeadActor; got != testActorUUID("b") {
		t.Fatalf("replayed dead actor = %q", got)
	}
}

func TestSessionsMarksExpiredActorsDead(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	register(t, engine, testActorUUID("waiting"))
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
