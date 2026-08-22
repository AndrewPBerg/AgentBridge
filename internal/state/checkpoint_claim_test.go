package state

import (
	"reflect"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type checkpointClaimsScenario struct {
	engine                               *Engine
	journal                              *memoryJournal
	now                                  *time.Time
	a, b, other                          protocol.Actor
	collision                            protocol.Collision
	creationSequence, transitionSequence uint64
}

func TestCheckpointClaimsEndToEndEvidenceDerivationAndReplay(t *testing.T) {
	scenario := setupCheckpointClaimsScenario(t)
	assertCheckpointClaimsScenario(t, &scenario)
}

func checkpointIntent(id, actor, path string) protocol.Intent {
	return protocol.Intent{ID: id, Actor: actor, ToolCallID: id, Tool: "edit", Operation: "edit", Paths: []string{path}, CWD: "/repo"}
}

func setupCheckpointClaimsScenario(t *testing.T) checkpointClaimsScenario {
	engine, journal, now := newTestEngine(t)
	a := register(t, engine, "actor-a")
	b := register(t, engine, "actor-b")
	other := registerAt(t, engine, "actor-other", "/other")
	collision, creationSequence := setupCheckpointActivity(t, engine, journal, &a, &b)
	recordCheckpointTestResults(t, engine, &a)
	transitionSequence := advanceCheckpointCollision(t, engine, journal, &a, &b, collision.ID, creationSequence)
	return checkpointClaimsScenario{engine: engine, journal: journal, now: now, a: a, b: b, other: other, collision: collision, creationSequence: creationSequence, transitionSequence: transitionSequence}
}

func setupCheckpointActivity(t *testing.T, engine *Engine, journal *memoryJournal, a, b *protocol.Actor) (collision protocol.Collision, sequence uint64) {
	if _, err := engine.BeginIntent(checkpointIntent("mutation-a", a.Address, "/repo/file.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.EndIntent(protocol.IntentEndParams{IntentID: "mutation-a", Success: true}); err != nil {
		t.Fatal(err)
	}
	for _, message := range []protocol.SendParams{
		{ID: "a-to-b", From: a.Address, To: b.Address, Body: "sent"},
		{ID: "b-to-a", From: b.Address, To: a.Address, Body: "received"},
	} {
		if _, err := engine.Send(message); err != nil {
			t.Fatal(err)
		}
	}
	collisions, err := engine.BeginIntent(checkpointIntent("mutation-b", b.Address, "/repo/file.go"))
	if err != nil || len(collisions) != 1 {
		t.Fatalf("collision creation = %#v, err=%v", collisions, err)
	}
	return collisions[0], eventSequence(t, journal, "collision.upserted", collisions[0].ID)
}

func recordCheckpointTestResults(t *testing.T, engine *Engine, actor *protocol.Actor) {
	zero, one := 0, 1
	results := []protocol.TestResult{
		{ID: "test-pass", Actor: actor.Address, Command: "go test ./...", ExitCode: &zero},
		{ID: "test-fail", Actor: actor.Address, Command: "go test ./...", ExitCode: &one},
		{ID: "test-blocked", Actor: actor.Address, Command: "go test ./..."},
	}
	for index := range results {
		if _, err := engine.RecordTestResult(results[index]); err != nil {
			t.Fatal(err)
		}
	}
	if engine.testResults["test-pass"].Outcome != protocol.TestPassed || engine.testResults["test-fail"].Outcome != protocol.TestFailed || engine.testResults["test-blocked"].Outcome != protocol.TestBlocked {
		t.Fatalf("normalized outcomes = %#v", engine.testResults)
	}
	if _, err := engine.RecordTestResult(protocol.TestResult{ID: "inconsistent", Actor: actor.Address, Command: "go test ./...", ExitCode: &zero, Outcome: protocol.TestFailed}); err == nil {
		t.Fatal("inconsistent test outcome and exit code were accepted")
	}
}

func advanceCheckpointCollision(t *testing.T, engine *Engine, journal *memoryJournal, a, b *protocol.Actor, collisionID string, creationSequence uint64) uint64 {
	if _, err := engine.Heartbeat(protocol.HeartbeatParams{Address: b.Address, State: "dead"}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{CollisionID: collisionID, Actor: a.Address, State: protocol.CollisionNegotiating}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{CollisionID: collisionID, Actor: a.Address, State: protocol.CollisionYielded, Owner: b.Address}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Transition(protocol.TransitionParams{CollisionID: collisionID, Actor: b.Address, State: protocol.CollisionResolved, Resolution: "b owns file.go"}); err != nil {
		t.Fatal(err)
	}
	transitionSequence := journal.events[len(journal.events)-1].Sequence
	if transitionSequence <= creationSequence {
		t.Fatalf("transition sequence = %d, creation = %d", transitionSequence, creationSequence)
	}
	return transitionSequence
}

func assertCheckpointClaimsScenario(t *testing.T, scenario *checkpointClaimsScenario) {
	checkpoint := assertCheckpointEvidence(t, scenario.engine, &scenario.a, &scenario.collision)
	assertCheckpointBoundaries(t, scenario, &checkpoint)
}

func assertCheckpointEvidence(t *testing.T, engine *Engine, a *protocol.Actor, collision *protocol.Collision) protocol.CheckpointRequest {
	checkpoint, err := engine.RequestCheckpoint(protocol.CheckpointRequest{
		ID: "claims-e2e", Actor: a.Address, CheckpointKind: "settled", JournalStart: 1,
		Claims: []protocol.CheckpointClaim{
			{Kind: "test", Statement: "tests pass", Status: protocol.ClaimVerified, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 0}}},
			{Kind: "test", Statement: "tests failed", Status: protocol.ClaimFailed, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 1}}},
			{Kind: "test", Statement: "tests blocked", Status: protocol.ClaimBlocked, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 2}}},
			{Kind: "build", Statement: "failed result cannot verify", Status: protocol.ClaimVerified, Evidence: []protocol.CheckpointEvidenceRef{{Kind: "test_result", Ordinal: 1}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertDerivedCheckpointEvidence(t, &checkpoint, collision.ID)
	assertBareCheckpointOutcomes(t, engine, a.Address)
	return checkpoint
}

func assertDerivedCheckpointEvidence(t *testing.T, checkpoint *protocol.CheckpointRequest, collisionID string) {
	if !reflect.DeepEqual(checkpoint.MutationIDs, []string{"mutation-a"}) ||
		!reflect.DeepEqual(checkpoint.CollisionIDs, []string{collisionID}) || !reflect.DeepEqual(checkpoint.TestResultIDs, []string{"test-pass", "test-fail", "test-blocked"}) ||
		!containsAll(checkpoint.MessageIDs, "a-to-b", "b-to-a") {
		t.Fatalf("derived evidence = %#v", checkpoint)
	}
	if checkpoint.Claims[0].Status != protocol.ClaimVerified || checkpoint.Claims[1].Status != protocol.ClaimFailed || checkpoint.Claims[2].Status != protocol.ClaimBlocked || checkpoint.Claims[3].Status != protocol.ClaimAsserted {
		t.Fatalf("normalized claim statuses = %#v", checkpoint.Claims)
	}
}

func assertBareCheckpointOutcomes(t *testing.T, engine *Engine, actor string) {
	bare, err := engine.RequestCheckpoint(protocol.CheckpointRequest{
		ID: "bare-outcomes", Actor: actor, CheckpointKind: "settled", JournalStart: 1,
		Claims: []protocol.CheckpointClaim{
			{Kind: "test", Statement: "bare failed", Status: protocol.ClaimFailed},
			{Kind: "runtime", Statement: "bare blocked", Status: protocol.ClaimBlocked},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bare.Claims[0].Status != protocol.ClaimAsserted || bare.Claims[1].Status != protocol.ClaimAsserted {
		t.Fatalf("bare outcome claims were not downgraded: %#v", bare.Claims)
	}
}

func assertCheckpointBoundaries(t *testing.T, scenario *checkpointClaimsScenario, checkpoint *protocol.CheckpointRequest) {
	engine, journal, now := scenario.engine, scenario.journal, scenario.now
	a, other := scenario.a, scenario.other
	collision := scenario.collision
	creationSequence, transitionSequence := scenario.creationSequence, scenario.transitionSequence
	// A range beginning after collision creation still includes the transitioned collision.
	after, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "after-transition", Actor: a.Address, CheckpointKind: "settled", JournalStart: creationSequence + 1, JournalEnd: transitionSequence})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.CollisionIDs, []string{collision.ID}) {
		t.Fatalf("post-creation collision evidence = %#v", after.CollisionIDs)
	}

	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "unknown-ref", Actor: a.Address, CheckpointKind: "settled", TestResultIDs: []string{"missing"}}); err == nil {
		t.Fatal("unknown explicit evidence reference was accepted")
	}
	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "unsupported-claim", Actor: a.Address, CheckpointKind: "settled", Claims: []protocol.CheckpointClaim{{Kind: "settled", Statement: "not a claim kind", Status: protocol.ClaimAsserted}}}); err == nil {
		t.Fatal("unsupported checkpoint claim kind was accepted")
	}
	if _, err := engine.BeginIntent(checkpointIntent("mutation-other", other.Address, "/other/file.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.EndIntent(protocol.IntentEndParams{IntentID: "mutation-other", Success: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.RequestCheckpoint(protocol.CheckpointRequest{ID: "cross-scope", Actor: a.Address, CheckpointKind: "settled", MutationIDs: []string{"mutation-other"}}); err == nil {
		t.Fatal("cross-scope explicit evidence reference was accepted")
	}

	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	replayedCheckpoint, err := replayed.RequestCheckpoint(*checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(*checkpoint, replayedCheckpoint) {
		t.Fatalf("replayed checkpoint differs:\n got %#v\nwant %#v", replayedCheckpoint, checkpoint)
	}
}

func containsAll(values []string, wanted ...string) bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	for _, value := range wanted {
		if !set[value] {
			return false
		}
	}
	return true
}

func registerAt(t *testing.T, engine *Engine, address, cwd string) protocol.Actor {
	t.Helper()
	address = testActorUUID(address)
	actor, err := engine.Register(protocol.Actor{Address: address, Harness: "pi", SessionUUID: address, CWD: cwd, State: "active"})
	if err != nil {
		t.Fatal(err)
	}
	return actor
}

func eventSequence(t *testing.T, journal *memoryJournal, eventType, collisionID string) uint64 {
	t.Helper()
	for _, event := range journal.events {
		if event.Type != eventType {
			continue
		}
		value, err := decode[protocol.Collision](event)
		if err == nil && value.ID == collisionID {
			return event.Sequence
		}
	}
	t.Fatalf("event %s for collision %s not found", eventType, collisionID)
	return 0
}
