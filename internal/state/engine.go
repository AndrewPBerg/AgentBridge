package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

const (
	defaultActorTTL     = 12 * time.Second
	defaultRecentWindow = 30 * time.Second
)

type Appender interface {
	Append(protocol.Event) error
}

type Engine struct {
	mu sync.Mutex

	journal      Appender
	now          func() time.Time
	actorTTL     time.Duration
	recentWindow time.Duration

	eventSequence      uint64
	globalSequence     uint64
	senderSequences    map[string]uint64
	recipientSequences map[string]uint64
	actors             map[string]protocol.Actor
	intents            map[string]protocol.Intent
	messages           map[string]protocol.Message
	mailboxes          map[string][]string
	collisions         map[string]protocol.Collision
	collisionByKey     map[string]string
	checkpoints        map[string]protocol.CheckpointRequest
	intentSequence     map[string]uint64
	messageSequence    map[string]uint64
	collisionSequence  map[string]uint64
	testResults        map[string]protocol.TestResult
	testResultSequence map[string]uint64
	workUnits          map[string]protocol.WorkUnit
	workUnitActors     map[string]map[string]protocol.WorkUnitActor
	poisoned           error
}

type Options struct {
	Now          func() time.Time
	ActorTTL     time.Duration
	RecentWindow time.Duration
}

func New(journal Appender, events []protocol.Event, options Options) (*Engine, error) {
	now := options.Now
	if now == nil {
		now = time.Now
	}
	actorTTL := options.ActorTTL
	if actorTTL == 0 {
		actorTTL = defaultActorTTL
	}
	recentWindow := options.RecentWindow
	if recentWindow == 0 {
		recentWindow = defaultRecentWindow
	}
	engine := &Engine{
		journal:            journal,
		now:                now,
		actorTTL:           actorTTL,
		recentWindow:       recentWindow,
		senderSequences:    make(map[string]uint64),
		recipientSequences: make(map[string]uint64),
		actors:             make(map[string]protocol.Actor),
		intents:            make(map[string]protocol.Intent),
		messages:           make(map[string]protocol.Message),
		mailboxes:          make(map[string][]string),
		collisions:         make(map[string]protocol.Collision),
		collisionByKey:     make(map[string]string),
		checkpoints:        make(map[string]protocol.CheckpointRequest),
		intentSequence:     make(map[string]uint64),
		messageSequence:    make(map[string]uint64),
		collisionSequence:  make(map[string]uint64),
		testResults:        make(map[string]protocol.TestResult),
		testResultSequence: make(map[string]uint64),
		workUnits:          make(map[string]protocol.WorkUnit),
		workUnitActors:     make(map[string]map[string]protocol.WorkUnitActor),
	}
	for _, event := range events {
		if event.Sequence != engine.eventSequence+1 {
			return nil, fmt.Errorf("event sequence %d is not contiguous after %d", event.Sequence, engine.eventSequence)
		}
		if err := engine.apply(event); err != nil {
			return nil, fmt.Errorf("replay event %d: %w", event.Sequence, err)
		}
		engine.eventSequence = event.Sequence
	}
	return engine, nil
}

func (e *Engine) record(eventType string, value any) error {
	if e.poisoned != nil {
		return fmt.Errorf("state engine is poisoned: %w", e.poisoned)
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	event := protocol.Event{
		Version:  protocol.Version,
		Sequence: e.eventSequence + 1,
		Type:     eventType,
		At:       e.now().UTC(),
		Data:     data,
	}
	if err := e.journal.Append(event); err != nil {
		return err
	}
	if err := e.apply(event); err != nil {
		// The journal append is already durable. Continuing would reuse the
		// sequence or operate on state that no longer matches the journal.
		e.eventSequence = event.Sequence
		e.poisoned = fmt.Errorf("apply durable event %d: %w", event.Sequence, err)
		return e.poisoned
	}
	e.eventSequence = event.Sequence
	return nil
}

func decode[T any](event protocol.Event) (T, error) {
	var value T
	err := json.Unmarshal(event.Data, &value)
	return value, err
}

func (e *Engine) apply(event protocol.Event) error {
	switch event.Type {
	case "actor.upserted":
		actor, err := decode[protocol.Actor](event)
		if err != nil {
			return err
		}
		e.actors[actor.Address] = actor
	case "intent.started", "intent.completed":
		intent, err := decode[protocol.Intent](event)
		if err != nil {
			return err
		}
		e.intents[intent.ID] = intent
		e.intentSequence[intent.ID] = event.Sequence
	case "collision.upserted":
		collision, err := decode[protocol.Collision](event)
		if err != nil {
			return err
		}
		e.collisions[collision.ID] = collision
		e.collisionSequence[collision.ID] = event.Sequence
		e.collisionByKey[collision.Key] = collision.ID
	case "collision.actor_dead":
		dead, err := decode[protocol.CollisionActorDeadEvent](event)
		if err != nil {
			return err
		}
		collision, ok := e.collisions[dead.CollisionID]
		if !ok {
			return fmt.Errorf("dead-actor event references unknown collision %q", dead.CollisionID)
		}
		collision.DeadActor = dead.Actor
		collision.UpdatedAt = dead.At.UTC()
		e.collisions[collision.ID] = collision
		e.collisionSequence[collision.ID] = event.Sequence
	case "collision.transitioned":
		transition, err := decode[protocol.CollisionTransitionEvent](event)
		if err != nil {
			return err
		}
		collision, ok := e.collisions[transition.CollisionID]
		if !ok {
			return fmt.Errorf("collision transition references unknown collision %q", transition.CollisionID)
		}
		if collision.State != transition.From {
			return fmt.Errorf("collision %q transition expected state %s, got %s", transition.CollisionID, transition.From, collision.State)
		}
		collision.State = transition.To
		collision.Owner = transition.Owner
		collision.YieldedBy = transition.YieldedBy
		collision.Resolution = transition.Resolution
		collision.UpdatedAt = transition.At.UTC()
		if transition.To == protocol.CollisionResolved {
			at := transition.At.UTC()
			collision.ResolvedAt = &at
			collision.ResolvedBy = transition.Actor
		}
		e.collisions[collision.ID] = collision
		e.collisionSequence[collision.ID] = event.Sequence
	case "message.enqueued":
		message, err := decode[protocol.Message](event)
		if err != nil {
			return err
		}
		e.messages[message.ID] = message
		e.messageSequence[message.ID] = event.Sequence
		e.mailboxes[message.To] = append(e.mailboxes[message.To], message.ID)
		e.globalSequence = max(e.globalSequence, message.GlobalSequence)
		e.senderSequences[message.From] = max(e.senderSequences[message.From], message.SenderSequence)
		e.recipientSequences[message.To] = max(e.recipientSequences[message.To], message.RecipientSequence)
	case "session.event":
		if _, err := decode[protocol.SessionEvent](event); err != nil {
			return err
		}
	case "test.result":
		result, err := decode[protocol.TestResult](event)
		if err != nil {
			return err
		}
		if err := protocol.NormalizeTestResult(&result); err != nil {
			return err
		}
		if result.ID == "" || result.Actor == "" {
			return errors.New("invalid test result")
		}
		for name, value := range map[string]string{"repository_uuid": result.RepositoryUUID, "workspace_uuid": result.WorkspaceUUID} {
			if value != "" {
				if err := protocol.ValidateUUID(value); err != nil {
					return fmt.Errorf("%s: %w", name, err)
				}
			}
		}
		if actor, ok := e.actors[result.Actor]; ok && (result.RepositoryUUID != actor.RepositoryUUID || result.WorkspaceUUID != actor.WorkspaceUUID) {
			return errors.New("test result scope mismatch")
		}
		if existing, ok := e.testResults[result.ID]; ok && !reflect.DeepEqual(existing, result) {
			return errors.New("conflicting test result replay")
		}
		e.testResults[result.ID] = result
		e.testResultSequence[result.ID] = event.Sequence
	case "checkpoint.requested":
		request, err := decode[protocol.CheckpointRequest](event)
		if err != nil {
			return err
		}
		if request.ID == "" || protocol.ValidateUUID(request.RepositoryUUID) != nil || protocol.ValidateUUID(request.WorkspaceUUID) != nil {
			return errors.New("invalid checkpoint identity")
		}
		actor, ok := e.actors[request.Actor]
		if !ok || actor.RepositoryUUID != request.RepositoryUUID || actor.WorkspaceUUID != request.WorkspaceUUID {
			return errors.New("checkpoint actor scope mismatch")
		}
		if request.WorkUnitUUID != "" {
			unit, ok := e.workUnits[request.WorkUnitUUID]
			if !ok || unit.RepositoryUUID != request.RepositoryUUID || unit.WorkspaceUUID != request.WorkspaceUUID || !activeWorkUnitParticipant(e, request.WorkUnitUUID, request.Actor) {
				return errors.New("invalid checkpoint work unit")
			}
		}
		if err := e.validateCheckpointReferences(request); err != nil {
			return err
		}
		if err := validateCheckpointClaims(request, e.testResults); err != nil {
			return err
		}
		if existing, ok := e.checkpoints[request.ID]; ok && !reflect.DeepEqual(existing, request) {
			return errors.New("conflicting checkpoint replay")
		}
		e.checkpoints[request.ID] = request
	case "work_unit.created":
		created, err := decode[protocol.WorkUnitCreatedEvent](event)
		if err != nil {
			return err
		}
		if err := validateWorkUnitCreation(created.WorkUnit); err != nil {
			return err
		}
		creator, ok := e.actors[created.WorkUnit.CreatedBy]
		if !ok || creator.RepositoryUUID != created.WorkUnit.RepositoryUUID || creator.WorkspaceUUID != created.WorkUnit.WorkspaceUUID {
			return errors.New("work unit creator scope mismatch")
		}
		if _, exists := e.workUnits[created.WorkUnit.UUID]; exists {
			return fmt.Errorf("work unit %q already exists", created.WorkUnit.UUID)
		}
		e.workUnits[created.WorkUnit.UUID] = created.WorkUnit
		e.workUnitActors[created.WorkUnit.UUID] = make(map[string]protocol.WorkUnitActor)
	case "work_unit.updated":
		updated, err := decode[protocol.WorkUnitUpdatedEvent](event)
		if err != nil {
			return err
		}
		current, ok := e.workUnits[updated.UUID]
		if !ok {
			return fmt.Errorf("unknown work unit %q", updated.UUID)
		}
		if !reflect.DeepEqual(current, updated.Previous) {
			return fmt.Errorf("work unit %q update previous value mismatch", updated.UUID)
		}
		if err := validateWorkUnitStructure(updated.Result); err != nil || updated.UUID != current.UUID || updated.Result.UUID != current.UUID || updated.Result.RepositoryUUID != current.RepositoryUUID || updated.Result.WorkspaceUUID != current.WorkspaceUUID || updated.Result.State != current.State {
			return errors.New("invalid work unit update")
		}
		actor, ok := e.actors[updated.Actor]
		if !ok || protocol.ValidateUUID(updated.Actor) != nil || !sameWorkUnitScope(current, actor) || !activeWorkUnitParticipant(e, current.UUID, updated.Actor) {
			return errors.New("invalid work unit update actor")
		}
		e.workUnits[updated.UUID] = updated.Result
	case "work_unit.transitioned":
		transition, err := decode[protocol.WorkUnitTransitionEvent](event)
		if err != nil {
			return err
		}
		unit, ok := e.workUnits[transition.WorkUnitUUID]
		if !ok {
			return fmt.Errorf("unknown work unit %q", transition.WorkUnitUUID)
		}
		if protocol.ValidateUUID(transition.WorkUnitUUID) != nil || protocol.ValidateUUID(transition.Actor) != nil || unit.State != transition.From {
			return fmt.Errorf("invalid work unit transition")
		}
		actor, ok := e.actors[transition.Actor]
		if !ok || !sameWorkUnitScope(unit, actor) || !validWorkUnitTransition(transition.From, transition.To) || !activeWorkUnitParticipant(e, transition.WorkUnitUUID, transition.Actor) {
			return errors.New("invalid work unit transition")
		}
		unit.State, unit.UpdatedAt = transition.To, transition.At.UTC()
		if transition.To == protocol.WorkUnitCompleted {
			at := transition.At.UTC()
			unit.CompletedAt = &at
		}
		e.workUnits[unit.UUID] = unit
	case "work_unit.actor_joined", "work_unit.actor_left":
		membership, err := decode[protocol.WorkUnitActorEvent](event)
		if err != nil {
			return err
		}
		if _, ok := e.workUnits[membership.WorkUnitUUID]; !ok {
			return fmt.Errorf("unknown work unit %q", membership.WorkUnitUUID)
		}
		if err := protocol.ValidateUUID(membership.WorkUnitUUID); err != nil || membership.Actor == "" || protocol.ValidateUUID(membership.Actor) != nil || membership.Result.WorkUnitUUID != membership.WorkUnitUUID || membership.Result.Actor != membership.Actor {
			return errors.New("invalid work unit membership")
		}
		if event.Type == "work_unit.actor_joined" && membership.Result.LeftAt != nil {
			return errors.New("joined participant cannot be left")
		}
		if event.Type == "work_unit.actor_left" && membership.Result.LeftAt == nil {
			return errors.New("left participant must have left_at")
		}
		if membership.Previous != nil {
			current := e.workUnitActors[membership.WorkUnitUUID][membership.Actor]
			if !reflect.DeepEqual(current, *membership.Previous) {
				return fmt.Errorf("work unit actor %q previous value mismatch", membership.Actor)
			}
		}
		e.workUnitActors[membership.WorkUnitUUID][membership.Actor] = membership.Result
	case "message.acked":
		ack, err := decode[ackEvent](event)
		if err != nil {
			return err
		}
		for _, id := range ack.MessageIDs {
			message, ok := e.messages[id]
			if !ok || message.To != ack.Actor || message.AcknowledgedAt != nil {
				continue
			}
			at := ack.At
			message.AcknowledgedAt = &at
			e.messages[id] = message
		}
	default:
		return fmt.Errorf("unknown event type %q", event.Type)
	}
	return nil
}

func (e *Engine) Register(actor protocol.Actor) (protocol.Actor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if actor.Address == "" || actor.Harness == "" || actor.SessionUUID == "" || actor.CWD == "" {
		return protocol.Actor{}, errors.New("address, harness, session_uuid, and cwd are required")
	}
	previous := e.actors[actor.Address]
	if actor.Alias == "" {
		actor.Alias = previous.Alias
	}
	if actor.StartedAt.IsZero() {
		actor.StartedAt = e.now().UTC()
	}
	if !previous.StartedAt.IsZero() {
		actor.StartedAt = previous.StartedAt
	}
	actor.HeartbeatAt = e.now().UTC()
	if actor.State == "" {
		actor.State = "waiting"
	}
	actor = normalizeActorScope(actor)
	if err := e.record("actor.upserted", actor); err != nil {
		return protocol.Actor{}, err
	}
	return actor, nil
}

func (e *Engine) Heartbeat(params protocol.HeartbeatParams) (protocol.Actor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	actor, ok := e.actors[params.Address]
	if !ok {
		return protocol.Actor{}, fmt.Errorf("unknown actor %q", params.Address)
	}
	actor.HeartbeatAt = e.now().UTC()
	if params.State != "" {
		actor.State = params.State
	}
	if params.CWD != "" {
		actor.CWD = params.CWD
	}
	if params.Git != nil {
		actor.Git = params.Git
	}
	if params.JJ != nil {
		actor.JJ = params.JJ
	}
	if params.Generation != 0 {
		actor.Generation = params.Generation
	}
	actor = normalizeActorScope(actor)
	wasDead := actor.State == "dead"
	// Presence is lease-based and intentionally ephemeral. Journaling a fsynced
	// heartbeat every two seconds would grow the log and serialize all clients
	// behind disk latency; sessions re-register after daemon restart.
	e.actors[actor.Address] = actor
	if wasDead {
		e.notifyDeadCollisionsLocked(actor.Address)
	}
	return actor, nil
}

func (e *Engine) SetAlias(address, alias string) (protocol.Actor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	actor, ok := e.actors[address]
	if !ok {
		return protocol.Actor{}, fmt.Errorf("unknown actor %q", address)
	}
	alias = strings.TrimPrefix(strings.TrimSpace(alias), "@")
	if alias == "" {
		return protocol.Actor{}, errors.New("alias is required")
	}
	for _, candidate := range e.actors {
		if candidate.Address != address && candidate.Alias == alias && candidate.WorkspaceUUID == actor.WorkspaceUUID && e.active(candidate) {
			return protocol.Actor{}, fmt.Errorf("alias @%s is already active in workspace %s", alias, actor.WorkspaceUUID)
		}
	}
	actor.Alias = alias
	if err := e.record("actor.upserted", actor); err != nil {
		return protocol.Actor{}, err
	}
	return actor, nil
}

func (e *Engine) notifyDeadCollisionsLocked(deadActor string) {
	for _, collision := range e.collisions {
		if collision.DeadActor != "" || collision.State == protocol.CollisionResolved || (collision.Actors[0] != deadActor && collision.Actors[1] != deadActor) {
			continue
		}
		at := e.now().UTC()
		if err := e.record("collision.actor_dead", protocol.CollisionActorDeadEvent{CollisionID: collision.ID, Actor: deadActor, At: at}); err != nil {
			continue
		}
		for _, recipient := range collision.Actors {
			if recipient == deadActor {
				continue
			}
			message := e.nextMessage("agent-bridge", recipient, "collision-dead", fmt.Sprintf("Agent %s is no longer alive while collision %s remains active on %s. Reassess ownership before continuing.", deadActor, collision.ID, collision.Path), protocol.SendParams{ID: collision.ID + ":dead:" + recipient}, collision.ID)
			_ = e.enqueue(message)
		}
	}
}

func (e *Engine) active(actor protocol.Actor) bool {
	return actor.State != "dead" && e.now().Sub(actor.HeartbeatAt) <= e.actorTTL
}

// expireStaleLocked makes the persisted actor state agree with the liveness
// check. Staleness used to be only a filter, which left old actors reporting
// their last state (often "waiting") forever when stale sessions were listed.
func (e *Engine) expireStaleLocked() {
	now := e.now().UTC()
	for address, actor := range e.actors {
		if actor.State == "dead" || now.Sub(actor.HeartbeatAt) <= e.actorTTL {
			continue
		}
		actor.State = "dead"
		e.actors[address] = actor
		// A failed append should not make an actor appear live again in this
		// process; the next journal replay will retain the prior state instead.
		if err := e.record("actor.upserted", actor); err == nil {
			e.notifyDeadCollisionsLocked(address)
		}
	}
}

func (e *Engine) Sessions(includeStale bool) []protocol.Actor {
	return e.SessionsScoped(includeStale, protocol.ScopeFilter{})
}

func (e *Engine) SessionsScoped(includeStale bool, scope protocol.ScopeFilter) []protocol.Actor {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.expireStaleLocked()
	actors := make([]protocol.Actor, 0, len(e.actors))
	for _, actor := range e.actors {
		if !includeStale && !e.active(actor) {
			continue
		}
		if scope.RepositoryUUID != "" && actor.RepositoryUUID != scope.RepositoryUUID {
			continue
		}
		if scope.WorkspaceUUID != "" && actor.WorkspaceUUID != scope.WorkspaceUUID {
			continue
		}
		if scope.Directory != "" && !underDirectory(actor.CWD, scope.Directory) && !e.actorTouchesDirectory(actor.Address, scope.Directory) {
			continue
		}
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i].Address < actors[j].Address })
	return actors
}

func (e *Engine) actorTouchesDirectory(address, directory string) bool {
	for _, intent := range e.intents {
		if intent.Actor != address || !e.intentRecent(intent) {
			continue
		}
		for _, path := range intent.Paths {
			if underDirectory(path, directory) {
				return true
			}
		}
	}
	return false
}

func (e *Engine) resolve(selector, senderAddress string) (protocol.Actor, error) {
	normalized := strings.TrimPrefix(strings.TrimSpace(selector), "@")
	if actor, ok := e.actors[normalized]; ok {
		return actor, nil // canonical addresses remain mailbox-addressable while stale
	}
	sender := e.actors[senderAddress]
	var matches []protocol.Actor
	for _, actor := range e.actors {
		matched := actor.SessionUUID == normalized
		if !matched && actor.JJ != nil {
			matched = strings.HasPrefix(actor.JJ.ChangeID, normalized)
		}
		if !matched && actor.Git != nil {
			matched = strings.HasPrefix(actor.Git.Head, normalized)
		}
		if !matched && actor.Alias == normalized {
			matched = true
		}
		if matched && e.active(actor) {
			matches = append(matches, actor)
		}
	}
	if len(matches) > 1 && sender.WorkspaceUUID != "" {
		workspace := filterActors(matches, func(actor protocol.Actor) bool { return actor.WorkspaceUUID == sender.WorkspaceUUID })
		if len(workspace) > 0 {
			matches = workspace
		} else {
			repository := filterActors(matches, func(actor protocol.Actor) bool { return actor.RepositoryUUID == sender.RepositoryUUID })
			if len(repository) > 0 {
				matches = repository
			}
		}
	}
	if len(matches) != 1 {
		if len(matches) == 0 {
			return protocol.Actor{}, fmt.Errorf("no addressable actor matches %q", selector)
		}
		return protocol.Actor{}, fmt.Errorf("selector %q matches multiple actors at the same authority scope", selector)
	}
	return matches[0], nil
}

func filterActors(values []protocol.Actor, keep func(protocol.Actor) bool) []protocol.Actor {
	result := make([]protocol.Actor, 0, len(values))
	for _, actor := range values {
		if keep(actor) {
			result = append(result, actor)
		}
	}
	return result
}

func (e *Engine) nextMessage(from, to, kind, body string, params protocol.SendParams, collisionID string) protocol.Message {
	e.globalSequence++
	e.senderSequences[from]++
	e.recipientSequences[to]++
	id := params.ID
	if id == "" {
		id = randomUUID()
	}
	return protocol.Message{
		ID:                id,
		Kind:              kind,
		From:              from,
		To:                to,
		Body:              body,
		GlobalSequence:    e.globalSequence,
		SenderSequence:    e.senderSequences[from],
		RecipientSequence: e.recipientSequences[to],
		ClientSequence:    params.ClientSequence,
		SessionGeneration: params.SessionGeneration,
		CollisionID:       collisionID,
		CreatedAt:         e.now().UTC(),
	}
}

func (e *Engine) enqueue(message protocol.Message) error {
	return e.record("message.enqueued", message)
}

func (e *Engine) Send(params protocol.SendParams) (protocol.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.actors[params.From]; !ok {
		return protocol.Message{}, fmt.Errorf("unknown sender %q", params.From)
	}
	target, err := e.resolve(params.To, params.From)
	if err != nil {
		return protocol.Message{}, err
	}
	body := strings.TrimSpace(params.Body)
	if body == "" {
		return protocol.Message{}, errors.New("message body is required")
	}
	if params.ID != "" {
		if existing, ok := e.messages[params.ID]; ok {
			if existing.From != params.From || existing.To != target.Address || existing.Body != body {
				return protocol.Message{}, fmt.Errorf("message id %q was already used with different content", params.ID)
			}
			return existing, nil
		}
	}
	// Persist collision lifecycle changes before the message. A failed send must
	// never report failure after the message was already durably enqueued, which
	// would invite a duplicate retry.
	if err := e.markNegotiating(params.From, target.Address); err != nil {
		return protocol.Message{}, err
	}
	message := e.nextMessage(params.From, target.Address, "message", body, params, "")
	if err := e.enqueue(message); err != nil {
		return protocol.Message{}, err
	}
	return message, nil
}

func (e *Engine) markNegotiating(left, right string) error {
	for _, collision := range e.collisions {
		if collision.State != protocol.CollisionOpen || !sameActors(collision.Actors, left, right) {
			continue
		}
		at := e.now().UTC()
		if err := e.record("collision.transitioned", protocol.CollisionTransitionEvent{
			CollisionID: collision.ID, Actor: left, From: collision.State, To: protocol.CollisionNegotiating, At: at,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) BeginIntent(intent protocol.Intent) ([]protocol.Collision, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.actors[intent.Actor]; !ok {
		return nil, fmt.Errorf("unknown actor %q", intent.Actor)
	}
	if intent.ID == "" || intent.ToolCallID == "" || len(intent.Paths) == 0 {
		return nil, errors.New("intent id, tool_call_id, and paths are required")
	}
	intent = normalizeIntentScope(intent)
	if intent.StartedAt.IsZero() {
		intent.StartedAt = e.now().UTC()
	}
	if intent.ExpiresAt.IsZero() {
		intent.ExpiresAt = intent.StartedAt.Add(5 * time.Minute)
	}
	if err := e.record("intent.started", intent); err != nil {
		return nil, err
	}
	var found []protocol.Collision
	for _, other := range e.intents {
		if other.ID == intent.ID || other.Actor == intent.Actor || !e.intentRecent(other) {
			continue
		}
		for _, path := range intersect(intent.Paths, other.Paths) {
			collision, _, err := e.ensureCollision(intent, other, path)
			if err != nil {
				return nil, err
			}
			found = append(found, collision)
			if err := e.ensureCollisionSignals(collision, intent, other); err != nil {
				return nil, err
			}
		}
	}
	return uniqueCollisions(found), nil
}

func (e *Engine) intentRecent(intent protocol.Intent) bool {
	if intent.ExpiresAt.Before(e.now()) {
		return false
	}
	return intent.CompletedAt == nil || e.now().Sub(*intent.CompletedAt) <= e.recentWindow
}

func (e *Engine) ensureCollision(left, right protocol.Intent, path string) (protocol.Collision, bool, error) {
	actors := [2]string{left.Actor, right.Actor}
	intents := [2]string{left.ID, right.ID}
	if actors[1] < actors[0] {
		actors[0], actors[1] = actors[1], actors[0]
		intents[0], intents[1] = intents[1], intents[0]
	}
	key := collisionKey(actors, path)
	if id := e.collisionByKey[key]; id != "" {
		existing := e.collisions[id]
		if existing.State != protocol.CollisionResolved {
			return existing, false, nil
		}
	}
	now := e.now().UTC()
	collision := protocol.Collision{
		ID:        randomUUID(),
		Key:       key,
		Path:      path,
		Actors:    actors,
		IntentIDs: intents,
		State:     protocol.CollisionOpen,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := e.record("collision.upserted", collision); err != nil {
		return protocol.Collision{}, false, err
	}
	return collision, true, nil
}

func (e *Engine) ensureCollisionSignals(collision protocol.Collision, left, right protocol.Intent) error {
	byActor := map[string]protocol.Intent{left.Actor: left, right.Actor: right}
	for _, to := range collision.Actors {
		if e.hasCollisionSignal(collision.ID, to) {
			continue
		}
		other := collision.Actors[0]
		if other == to {
			other = collision.Actors[1]
		}
		otherIntent := byActor[other]
		body := fmt.Sprintf("AUTOMATIC COLLISION on %s\n%s is also operating on this file via %s/%s. Do not revert unfamiliar work; coordinate, yield, or resolve collision %s.", collision.Path, other, otherIntent.Tool, otherIntent.Operation, collision.ID)
		message := e.nextMessage("agent-bridge", to, "collision", body, protocol.SendParams{ID: collision.ID + ":" + to}, collision.ID)
		if err := e.enqueue(message); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) hasCollisionSignal(collisionID, recipient string) bool {
	for _, message := range e.messages {
		if message.CollisionID == collisionID && message.To == recipient && message.Kind == "collision" {
			return true
		}
	}
	return false
}

func (e *Engine) EndIntent(params protocol.IntentEndParams) (protocol.Intent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	intent, ok := e.intents[params.IntentID]
	if !ok {
		return protocol.Intent{}, fmt.Errorf("unknown intent %q", params.IntentID)
	}
	completedAt := e.now().UTC()
	if params.CompletedAt != nil {
		completedAt = params.CompletedAt.UTC()
	}
	intent.CompletedAt = &completedAt
	intent.Success = &params.Success
	intent.Error = strings.TrimSpace(params.Error)
	intent.After = params.After
	intent.GitAfter = params.GitAfter
	intent.JJAfter = params.JJAfter
	if err := e.record("intent.completed", intent); err != nil {
		return protocol.Intent{}, err
	}
	return intent, nil
}

func (e *Engine) RecordTestResult(result protocol.TestResult) (protocol.TestResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	actor, ok := e.actors[result.Actor]
	if !ok {
		return protocol.TestResult{}, fmt.Errorf("unknown actor %q", result.Actor)
	}
	if result.ID == "" || result.Command == "" {
		return protocol.TestResult{}, errors.New("test result id and command are required")
	}
	if result.RepositoryUUID != "" {
		if err := protocol.ValidateUUID(result.RepositoryUUID); err != nil {
			return protocol.TestResult{}, fmt.Errorf("repository_uuid: %w", err)
		}
	} else {
		result.RepositoryUUID = actor.RepositoryUUID
	}
	if result.WorkspaceUUID != "" {
		if err := protocol.ValidateUUID(result.WorkspaceUUID); err != nil {
			return protocol.TestResult{}, fmt.Errorf("workspace_uuid: %w", err)
		}
	} else {
		result.WorkspaceUUID = actor.WorkspaceUUID
	}
	if result.RepositoryUUID != actor.RepositoryUUID || result.WorkspaceUUID != actor.WorkspaceUUID {
		return protocol.TestResult{}, errors.New("test result scope mismatch")
	}
	if err := protocol.NormalizeTestResult(&result); err != nil {
		return protocol.TestResult{}, err
	}
	if result.CompletedAt.IsZero() {
		result.CompletedAt = e.now().UTC()
	}
	if result.StartedAt.IsZero() {
		result.StartedAt = result.CompletedAt
	}
	if err := e.record("test.result", result); err != nil {
		return protocol.TestResult{}, err
	}
	return result, nil
}

func validateWorkUnitStructure(unit protocol.WorkUnit) error {
	for name, value := range map[string]string{"work_unit_uuid": unit.UUID, "repository_uuid": unit.RepositoryUUID, "workspace_uuid": unit.WorkspaceUUID, "created_by": unit.CreatedBy} {
		if err := protocol.ValidateUUID(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if unit.Objective == "" || unit.State == "" || unit.CreatedAt.IsZero() || unit.UpdatedAt.IsZero() {
		return errors.New("invalid work unit")
	}
	return nil
}

func validateWorkUnitCreation(unit protocol.WorkUnit) error {
	if err := validateWorkUnitStructure(unit); err != nil {
		return err
	}
	if unit.State != protocol.WorkUnitProposed {
		return errors.New("new work units must be proposed")
	}
	return nil
}

func activeWorkUnitParticipant(e *Engine, workUnit, actor string) bool {
	member, ok := e.workUnitActors[workUnit][actor]
	return ok && member.LeftAt == nil && member.ParticipationState == "active"
}

func sameWorkUnitScope(unit protocol.WorkUnit, actor protocol.Actor) bool {
	return unit.RepositoryUUID == actor.RepositoryUUID && unit.WorkspaceUUID == actor.WorkspaceUUID
}

func (e *Engine) CreateWorkUnit(unit protocol.WorkUnit) (protocol.WorkUnit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if unit.UUID == "" || unit.Objective == "" || unit.CreatedBy == "" {
		return protocol.WorkUnit{}, errors.New("work unit UUID, objective, and created_by are required")
	}
	for name, value := range map[string]string{"work_unit_uuid": unit.UUID, "repository_uuid": unit.RepositoryUUID, "workspace_uuid": unit.WorkspaceUUID, "created_by": unit.CreatedBy} {
		if err := protocol.ValidateUUID(value); err != nil {
			return protocol.WorkUnit{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	creator, ok := e.actors[unit.CreatedBy]
	if !ok {
		return protocol.WorkUnit{}, fmt.Errorf("unknown creator %q", unit.CreatedBy)
	}
	if !sameWorkUnitScope(unit, creator) {
		return protocol.WorkUnit{}, errors.New("work unit scope does not match creator")
	}
	if unit.State == "" {
		unit.State = protocol.WorkUnitProposed
	}
	if unit.State != protocol.WorkUnitProposed {
		return protocol.WorkUnit{}, errors.New("new work units must be proposed")
	}
	if existing, ok := e.workUnits[unit.UUID]; ok {
		candidate := unit
		if candidate.CreatedAt.IsZero() {
			candidate.CreatedAt = existing.CreatedAt
		}
		candidate.UpdatedAt = candidate.CreatedAt
		if reflect.DeepEqual(existing, candidate) {
			return existing, nil
		}
		return protocol.WorkUnit{}, fmt.Errorf("work unit %q conflicts with existing payload", unit.UUID)
	}
	if unit.CreatedAt.IsZero() {
		unit.CreatedAt = e.now().UTC()
	}
	unit.UpdatedAt = unit.CreatedAt
	if err := e.record("work_unit.created", protocol.WorkUnitCreatedEvent{WorkUnit: unit}); err != nil {
		return protocol.WorkUnit{}, err
	}
	return unit, nil
}

func (e *Engine) WorkUnit(uuid string) (protocol.WorkUnit, []protocol.WorkUnitActor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	unit, ok := e.workUnits[uuid]
	if !ok {
		return protocol.WorkUnit{}, nil, fmt.Errorf("unknown work unit %q", uuid)
	}
	actors := make([]protocol.WorkUnitActor, 0, len(e.workUnitActors[uuid]))
	for _, actor := range e.workUnitActors[uuid] {
		actors = append(actors, actor)
	}
	sort.Slice(actors, func(i, j int) bool { return actors[i].Actor < actors[j].Actor })
	return unit, actors, nil
}

func (e *Engine) UpdateWorkUnit(params protocol.WorkUnitUpdateParams) (protocol.WorkUnit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current, ok := e.workUnits[params.WorkUnitUUID]
	if !ok {
		return protocol.WorkUnit{}, fmt.Errorf("unknown work unit %q", params.WorkUnitUUID)
	}
	actor, ok := e.actors[params.Actor]
	if !ok {
		return protocol.WorkUnit{}, fmt.Errorf("unknown actor %q", params.Actor)
	}
	if !sameWorkUnitScope(current, actor) || !activeWorkUnitParticipant(e, current.UUID, params.Actor) {
		return protocol.WorkUnit{}, errors.New("actor is not an active equal-scope participant")
	}
	if current.State == protocol.WorkUnitCompleted || current.State == protocol.WorkUnitAbandoned {
		return protocol.WorkUnit{}, errors.New("terminal work unit is immutable")
	}
	result := current
	if params.Objective != nil {
		result.Objective = strings.TrimSpace(*params.Objective)
	}
	if params.AcceptanceCriteria != nil {
		result.AcceptanceCriteria = *params.AcceptanceCriteria
	}
	if params.Context != nil {
		result.Context = *params.Context
	}
	if result.Objective == "" {
		return protocol.WorkUnit{}, errors.New("objective is required")
	}
	if reflect.DeepEqual(result, current) {
		return current, nil
	}
	result.UpdatedAt = e.now().UTC()
	if err := e.record("work_unit.updated", protocol.WorkUnitUpdatedEvent{UUID: current.UUID, Actor: params.Actor, Previous: current, Result: result}); err != nil {
		return protocol.WorkUnit{}, err
	}
	return result, nil
}

func (e *Engine) JoinWorkUnit(params protocol.WorkUnitActorParams) (protocol.WorkUnitActor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	unit, ok := e.workUnits[params.WorkUnitUUID]
	if !ok {
		return protocol.WorkUnitActor{}, fmt.Errorf("unknown work unit %q", params.WorkUnitUUID)
	}
	actor, ok := e.actors[params.Actor]
	if !ok {
		return protocol.WorkUnitActor{}, fmt.Errorf("unknown actor %q", params.Actor)
	}
	if !sameWorkUnitScope(unit, actor) {
		return protocol.WorkUnitActor{}, errors.New("work unit scope mismatch")
	}
	if existing, ok := e.workUnitActors[params.WorkUnitUUID][params.Actor]; ok {
		if existing.LeftAt == nil {
			return existing, nil
		}
		return protocol.WorkUnitActor{}, errors.New("rejoin after leave is not allowed")
	}
	if unit.State == protocol.WorkUnitCompleted || unit.State == protocol.WorkUnitAbandoned {
		return protocol.WorkUnitActor{}, errors.New("terminal work unit is immutable")
	}
	at := e.now().UTC()
	result := protocol.WorkUnitActor{WorkUnitUUID: params.WorkUnitUUID, Actor: params.Actor, JoinedAt: at, ParticipationState: "active"}
	if err := e.record("work_unit.actor_joined", protocol.WorkUnitActorEvent{WorkUnitUUID: params.WorkUnitUUID, Actor: params.Actor, At: at, Result: result}); err != nil {
		return protocol.WorkUnitActor{}, err
	}
	return result, nil
}

func (e *Engine) LeaveWorkUnit(params protocol.WorkUnitActorParams) (protocol.WorkUnitActor, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	unit, exists := e.workUnits[params.WorkUnitUUID]
	if !exists {
		return protocol.WorkUnitActor{}, fmt.Errorf("unknown work unit %q", params.WorkUnitUUID)
	}
	if unit.State == protocol.WorkUnitCompleted || unit.State == protocol.WorkUnitAbandoned {
		return protocol.WorkUnitActor{}, errors.New("terminal work unit is immutable")
	}
	actor, exists := e.actors[params.Actor]
	if !exists || !sameWorkUnitScope(unit, actor) {
		return protocol.WorkUnitActor{}, errors.New("work unit scope mismatch")
	}
	current, ok := e.workUnitActors[params.WorkUnitUUID][params.Actor]
	if !ok || current.LeftAt != nil {
		return protocol.WorkUnitActor{}, fmt.Errorf("actor is not an active work unit participant")
	}
	at := e.now().UTC()
	previous := current
	current.LeftAt = &at
	current.ParticipationState = "left"
	if err := e.record("work_unit.actor_left", protocol.WorkUnitActorEvent{WorkUnitUUID: params.WorkUnitUUID, Actor: params.Actor, At: at, Previous: &previous, Result: current}); err != nil {
		return protocol.WorkUnitActor{}, err
	}
	return current, nil
}

func validWorkUnitTransition(from, to protocol.WorkUnitState) bool {
	switch from {
	case protocol.WorkUnitProposed:
		return to == protocol.WorkUnitActive || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitActive:
		return to == protocol.WorkUnitBlocked || to == protocol.WorkUnitVerified || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitBlocked:
		return to == protocol.WorkUnitActive || to == protocol.WorkUnitVerified || to == protocol.WorkUnitAbandoned
	case protocol.WorkUnitVerified:
		return to == protocol.WorkUnitCompleted || to == protocol.WorkUnitAbandoned
	}
	return false
}

func (e *Engine) TransitionWorkUnit(params protocol.WorkUnitTransitionParams) (protocol.WorkUnit, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	unit, ok := e.workUnits[params.WorkUnitUUID]
	if !ok {
		return protocol.WorkUnit{}, fmt.Errorf("unknown work unit %q", params.WorkUnitUUID)
	}
	actor, ok := e.actors[params.Actor]
	if !ok {
		return protocol.WorkUnit{}, fmt.Errorf("unknown actor %q", params.Actor)
	}
	if !sameWorkUnitScope(unit, actor) || !activeWorkUnitParticipant(e, unit.UUID, params.Actor) {
		return protocol.WorkUnit{}, errors.New("actor is not an active equal-scope participant")
	}
	if params.State == unit.State {
		if unit.State == protocol.WorkUnitCompleted || unit.State == protocol.WorkUnitAbandoned {
			return protocol.WorkUnit{}, errors.New("terminal work unit is immutable")
		}
		return unit, nil
	}
	if !validWorkUnitTransition(unit.State, params.State) {
		return protocol.WorkUnit{}, fmt.Errorf("invalid work unit transition %s -> %s", unit.State, params.State)
	}
	at := e.now().UTC()
	if err := e.record("work_unit.transitioned", protocol.WorkUnitTransitionEvent{WorkUnitUUID: unit.UUID, Actor: params.Actor, From: unit.State, To: params.State, At: at}); err != nil {
		return protocol.WorkUnit{}, err
	}
	return e.workUnits[unit.UUID], nil
}

func (e *Engine) RequestCheckpoint(request protocol.CheckpointRequest) (protocol.CheckpointRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	originalJournalStart, originalJournalEnd := request.JournalStart, request.JournalEnd
	actor, ok := e.actors[request.Actor]
	if !ok {
		return protocol.CheckpointRequest{}, fmt.Errorf("unknown actor %q", request.Actor)
	}
	if request.ID == "" || request.CheckpointKind == "" {
		return protocol.CheckpointRequest{}, errors.New("checkpoint id and kind are required")
	}
	if request.DeclaredBy == "" {
		request.DeclaredBy = "agent"
	}
	if request.DeclaredBy != "agent" && request.DeclaredBy != "human" && request.DeclaredBy != "system" {
		return protocol.CheckpointRequest{}, fmt.Errorf("invalid checkpoint declarer %q", request.DeclaredBy)
	}
	if request.SessionGeneration == 0 {
		request.SessionGeneration = actor.Generation
	}
	if request.RepositoryUUID == "" {
		request.RepositoryUUID = actor.RepositoryUUID
	}
	if request.WorkspaceUUID == "" {
		request.WorkspaceUUID = actor.WorkspaceUUID
	}
	if request.RepositoryUUID != actor.RepositoryUUID || request.WorkspaceUUID != actor.WorkspaceUUID {
		return protocol.CheckpointRequest{}, errors.New("checkpoint scope does not match the declarer session")
	}
	for name, value := range map[string]string{
		"repository_uuid": request.RepositoryUUID,
		"workspace_uuid":  request.WorkspaceUUID,
		"work_unit_uuid":  request.WorkUnitUUID,
	} {
		if value != "" {
			if err := protocol.ValidateUUID(value); err != nil {
				return protocol.CheckpointRequest{}, fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if request.WorkUnitUUID != "" {
		unit, exists := e.workUnits[request.WorkUnitUUID]
		if !exists {
			return protocol.CheckpointRequest{}, errors.New("checkpoint references unknown work unit")
		}
		if unit.RepositoryUUID != request.RepositoryUUID || unit.WorkspaceUUID != request.WorkspaceUUID {
			return protocol.CheckpointRequest{}, errors.New("checkpoint work unit scope mismatch")
		}
		if unit.State == protocol.WorkUnitCompleted || unit.State == protocol.WorkUnitAbandoned {
			return protocol.CheckpointRequest{}, errors.New("terminal work unit rejects new checkpoints")
		}
		if !activeWorkUnitParticipant(e, request.WorkUnitUUID, request.Actor) {
			return protocol.CheckpointRequest{}, errors.New("checkpoint declarer is not an active participant")
		}
	}
	if request.JournalEnd == 0 {
		request.JournalEnd = e.eventSequence
	}
	if request.JournalEnd > e.eventSequence {
		return protocol.CheckpointRequest{}, fmt.Errorf("checkpoint journal end %d is ahead of current sequence %d", request.JournalEnd, e.eventSequence)
	}
	if request.JournalStart == 0 {
		var priorEnd uint64
		for _, previous := range e.checkpoints {
			if previous.Actor == request.Actor && previous.SessionGeneration == request.SessionGeneration &&
				previous.RepositoryUUID == request.RepositoryUUID && previous.WorkspaceUUID == request.WorkspaceUUID &&
				previous.WorkUnitUUID == request.WorkUnitUUID && previous.JournalEnd < request.JournalEnd && previous.JournalEnd > priorEnd {
				priorEnd = previous.JournalEnd
			}
		}
		if priorEnd > 0 {
			request.JournalStart = priorEnd + 1
		} else {
			request.JournalStart = earliestCheckpointEvidence(e, request)
		}
	}
	if request.JournalStart > request.JournalEnd {
		return protocol.CheckpointRequest{}, errors.New("checkpoint journal range is invalid")
	}
	derive := func(ids *[]string, sequences map[string]uint64, relevant func(string) bool) error {
		if len(*ids) == 0 {
			for id, sequence := range sequences {
				if sequence >= request.JournalStart && sequence <= request.JournalEnd && relevant(id) {
					*ids = append(*ids, id)
				}
			}
			sort.Slice(*ids, func(i, j int) bool {
				if sequences[(*ids)[i]] == sequences[(*ids)[j]] {
					return (*ids)[i] < (*ids)[j]
				}
				return sequences[(*ids)[i]] < sequences[(*ids)[j]]
			})
		}
		return nil
	}
	if err := derive(&request.MutationIDs, e.intentSequence, func(id string) bool {
		intent, ok := e.intents[id]
		return ok && intent.CompletedAt != nil && intent.Actor == request.Actor && intent.RepositoryUUID == request.RepositoryUUID && intent.WorkspaceUUID == request.WorkspaceUUID
	}); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if err := derive(&request.MessageIDs, e.messageSequence, func(id string) bool {
		message, ok := e.messages[id]
		return ok && (message.From == request.Actor || message.To == request.Actor)
	}); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if err := derive(&request.CollisionIDs, e.collisionSequence, func(id string) bool {
		collision, ok := e.collisions[id]
		return ok && (collision.Actors[0] == request.Actor || collision.Actors[1] == request.Actor)
	}); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if err := derive(&request.TestResultIDs, e.testResultSequence, func(id string) bool {
		result, ok := e.testResults[id]
		return ok && result.Actor == request.Actor && result.RepositoryUUID == request.RepositoryUUID && result.WorkspaceUUID == request.WorkspaceUUID
	}); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if err := e.validateCheckpointReferences(request); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if err := normalizeCheckpointClaims(&request, e.testResults); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	if existing, ok := e.checkpoints[request.ID]; ok {
		if existing.Actor != request.Actor || existing.DeclaredBy != request.DeclaredBy || existing.SessionGeneration != request.SessionGeneration ||
			existing.RepositoryUUID != request.RepositoryUUID || existing.WorkspaceUUID != request.WorkspaceUUID || existing.WorkUnitUUID != request.WorkUnitUUID ||
			existing.CheckpointKind != request.CheckpointKind || existing.BoundaryEventID != request.BoundaryEventID || existing.BoundaryType != request.BoundaryType ||
			existing.TurnID != request.TurnID || !reflect.DeepEqual(existing.TurnIndex, request.TurnIndex) || existing.CompactionEventID != request.CompactionEventID ||
			!reflect.DeepEqual(existing.Git, request.Git) || !reflect.DeepEqual(existing.JJ, request.JJ) ||
			!reflect.DeepEqual(existing.MutationIDs, request.MutationIDs) || !reflect.DeepEqual(existing.MessageIDs, request.MessageIDs) ||
			!reflect.DeepEqual(existing.CollisionIDs, request.CollisionIDs) || !reflect.DeepEqual(existing.TestResultIDs, request.TestResultIDs) ||
			!reflect.DeepEqual(existing.Claims, request.Claims) || !reflect.DeepEqual(existing.Metadata, request.Metadata) ||
			(originalJournalStart != 0 && existing.JournalStart != request.JournalStart) || (originalJournalEnd != 0 && existing.JournalEnd != request.JournalEnd) {
			return protocol.CheckpointRequest{}, fmt.Errorf("checkpoint ID %q conflicts with an existing checkpoint", request.ID)
		}
		return existing, nil
	}
	if err := e.record("checkpoint.requested", request); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	return request, nil
}

func earliestCheckpointEvidence(e *Engine, request protocol.CheckpointRequest) uint64 {
	earliest := request.JournalEnd
	found := false
	consider := func(sequence uint64, relevant bool) {
		if relevant && sequence <= request.JournalEnd && (!found || sequence < earliest) {
			earliest, found = sequence, true
		}
	}
	for id, sequence := range e.intentSequence {
		intent := e.intents[id]
		consider(sequence, intent.CompletedAt != nil && intent.Actor == request.Actor && intent.RepositoryUUID == request.RepositoryUUID && intent.WorkspaceUUID == request.WorkspaceUUID)
	}
	for id, sequence := range e.messageSequence {
		message := e.messages[id]
		consider(sequence, message.From == request.Actor || message.To == request.Actor)
	}
	for id, sequence := range e.collisionSequence {
		collision := e.collisions[id]
		consider(sequence, len(collision.Actors) >= 2 && (collision.Actors[0] == request.Actor || collision.Actors[1] == request.Actor))
	}
	for id, sequence := range e.testResultSequence {
		result := e.testResults[id]
		consider(sequence, result.Actor == request.Actor && result.RepositoryUUID == request.RepositoryUUID && result.WorkspaceUUID == request.WorkspaceUUID)
	}
	return earliest
}

func (e *Engine) validateCheckpointReferences(request protocol.CheckpointRequest) error {
	checkRange := func(kind, id string, sequence uint64) error {
		if sequence < request.JournalStart || sequence > request.JournalEnd {
			return fmt.Errorf("%s %q is outside checkpoint journal range", kind, id)
		}
		return nil
	}
	for _, id := range request.MutationIDs {
		intent, ok := e.intents[id]
		if !ok || intent.CompletedAt == nil {
			return fmt.Errorf("checkpoint references unknown or incomplete mutation %q", id)
		}
		if intent.Actor != request.Actor || intent.RepositoryUUID != request.RepositoryUUID || intent.WorkspaceUUID != request.WorkspaceUUID {
			return fmt.Errorf("mutation %q is outside checkpoint scope", id)
		}
		if err := checkRange("mutation", id, e.intentSequence[id]); err != nil {
			return err
		}
	}
	for _, id := range request.MessageIDs {
		message, ok := e.messages[id]
		if !ok || (message.From != request.Actor && message.To != request.Actor) {
			return fmt.Errorf("message %q is outside checkpoint scope", id)
		}
		if err := checkRange("message", id, e.messageSequence[id]); err != nil {
			return err
		}
	}
	for _, id := range request.CollisionIDs {
		collision, ok := e.collisions[id]
		if !ok || len(collision.Actors) < 2 || (collision.Actors[0] != request.Actor && collision.Actors[1] != request.Actor) {
			return fmt.Errorf("collision %q is outside checkpoint scope", id)
		}
		if err := checkRange("collision", id, e.collisionSequence[id]); err != nil {
			return err
		}
	}
	for _, id := range request.TestResultIDs {
		result, ok := e.testResults[id]
		if !ok || result.Actor != request.Actor || result.RepositoryUUID != request.RepositoryUUID || result.WorkspaceUUID != request.WorkspaceUUID {
			return fmt.Errorf("test result %q is outside checkpoint scope", id)
		}
		if err := checkRange("test result", id, e.testResultSequence[id]); err != nil {
			return err
		}
	}
	return nil
}

func validateCheckpointClaims(request protocol.CheckpointRequest, results map[string]protocol.TestResult) error {
	copy := request
	return normalizeCheckpointClaims(&copy, results)
}

func normalizeCheckpointClaims(request *protocol.CheckpointRequest, results map[string]protocol.TestResult) error {
	if request.Metadata != nil && len(request.Claims) == 0 && strings.TrimSpace(request.Metadata["summary"]) != "" {
		request.Claims = []protocol.CheckpointClaim{{Kind: "summary", Statement: strings.TrimSpace(request.Metadata["summary"]), Status: protocol.ClaimAsserted}}
	}
	refs := map[string][]string{"mutation": request.MutationIDs, "message": request.MessageIDs, "collision": request.CollisionIDs, "test_result": request.TestResultIDs}
	for _, id := range request.TestResultIDs {
		result, ok := results[id]
		if !ok {
			return fmt.Errorf("checkpoint references unknown test result %q", id)
		}
		if result.Actor != request.Actor || result.RepositoryUUID != request.RepositoryUUID || result.WorkspaceUUID != request.WorkspaceUUID {
			return fmt.Errorf("test result %q is outside checkpoint actor scope", id)
		}
	}
	for i := range request.Claims {
		claim := &request.Claims[i]
		claim.Kind = strings.TrimSpace(claim.Kind)
		if !protocol.ValidCheckpointClaimKind(claim.Kind) || strings.TrimSpace(claim.Statement) == "" {
			return errors.New("checkpoint claims require a supported kind and statement")
		}
		switch claim.Status {
		case protocol.ClaimAsserted, protocol.ClaimVerified, protocol.ClaimFailed, protocol.ClaimBlocked:
		default:
			return fmt.Errorf("invalid checkpoint claim status %q", claim.Status)
		}
		matchingOutcomes := map[protocol.TestOutcome]bool{}
		for _, evidence := range claim.Evidence {
			list, ok := refs[evidence.Kind]
			if !ok || evidence.Ordinal < 0 || evidence.Ordinal >= len(list) {
				return fmt.Errorf("claim %d references invalid evidence %s[%d]", i, evidence.Kind, evidence.Ordinal)
			}
			if evidence.Kind == "test_result" {
				if result, ok := results[list[evidence.Ordinal]]; ok {
					candidate := result
					if err := protocol.NormalizeTestResult(&candidate); err != nil {
						return fmt.Errorf("test result evidence %q: %w", list[evidence.Ordinal], err)
					}
					matchingOutcomes[candidate.Outcome] = true
				}
			}
		}
		if claim.Kind == "test" || claim.Kind == "build" || claim.Kind == "runtime" {
			required := map[protocol.CheckpointClaimStatus]protocol.TestOutcome{
				protocol.ClaimVerified: protocol.TestPassed,
				protocol.ClaimFailed:   protocol.TestFailed,
				protocol.ClaimBlocked:  protocol.TestBlocked,
			}[claim.Status]
			if required != "" && !matchingOutcomes[required] {
				claim.Status = protocol.ClaimAsserted
			}
		}
	}
	return nil
}

func (e *Engine) RecordSessionEvent(event protocol.SessionEvent) (protocol.SessionEvent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.actors[event.Actor]; !ok {
		return protocol.SessionEvent{}, fmt.Errorf("unknown actor %q", event.Actor)
	}
	if event.ID == "" || event.Type == "" {
		return protocol.SessionEvent{}, errors.New("session event id and type are required")
	}
	if event.At.IsZero() {
		event.At = e.now().UTC()
	}
	if err := e.record("session.event", event); err != nil {
		return protocol.SessionEvent{}, err
	}
	return event, nil
}

func (e *Engine) Poll(actor string, limit int) ([]protocol.Message, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.actors[actor]; !ok {
		return nil, fmt.Errorf("unknown actor %q", actor)
	}
	e.expireStaleLocked()
	messages := make([]protocol.Message, 0)
	for _, id := range e.mailboxes[actor] {
		message := e.messages[id]
		if message.AcknowledgedAt == nil {
			messages = append(messages, message)
		}
	}
	messages = orderMailbox(messages)
	if limit <= 0 || limit > len(messages) {
		limit = len(messages)
	}
	result := make([]protocol.Message, limit)
	copy(result, messages[:limit])
	return result, nil
}

func orderMailbox(messages []protocol.Message) []protocol.Message {
	if len(messages) < 2 {
		return messages
	}
	groups := make(map[string][]protocol.Message)
	for _, message := range messages {
		groups[message.From] = append(groups[message.From], message)
	}
	for sender := range groups {
		group := groups[sender]
		sort.SliceStable(group, func(i, j int) bool {
			left, right := group[i], group[j]
			if left.ClientSequence == 0 || right.ClientSequence == 0 {
				return left.RecipientSequence < right.RecipientSequence
			}
			if left.SessionGeneration != 0 && right.SessionGeneration != 0 && left.SessionGeneration != right.SessionGeneration {
				return left.SessionGeneration < right.SessionGeneration
			}
			return left.ClientSequence < right.ClientSequence
		})
		groups[sender] = group
	}

	// K-way merge chooses the earliest daemon-accepted head while preserving
	// each sender's adapter-assigned causal order, even when other senders
	// interleave between out-of-order arrivals.
	ordered := make([]protocol.Message, 0, len(messages))
	for len(ordered) < len(messages) {
		var chosen string
		var sequence uint64
		for sender, group := range groups {
			if len(group) == 0 {
				continue
			}
			if chosen == "" || group[0].RecipientSequence < sequence {
				chosen = sender
				sequence = group[0].RecipientSequence
			}
		}
		ordered = append(ordered, groups[chosen][0])
		groups[chosen] = groups[chosen][1:]
	}
	return ordered
}

type ackEvent struct {
	Actor      string    `json:"actor"`
	MessageIDs []string  `json:"message_ids"`
	At         time.Time `json:"at"`
}

func (e *Engine) Ack(params protocol.AckParams) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.actors[params.Actor]; !ok {
		return fmt.Errorf("unknown actor %q", params.Actor)
	}
	return e.record("message.acked", ackEvent{Actor: params.Actor, MessageIDs: params.MessageIDs, At: e.now().UTC()})
}

func (e *Engine) Transition(params protocol.TransitionParams) (protocol.Collision, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	collision, ok := e.collisions[params.CollisionID]
	if !ok {
		return protocol.Collision{}, fmt.Errorf("unknown collision %q", params.CollisionID)
	}
	if params.Actor != collision.Actors[0] && params.Actor != collision.Actors[1] {
		return protocol.Collision{}, errors.New("actor is not a collision participant")
	}
	switch params.State {
	case protocol.CollisionNegotiating:
		if collision.State != protocol.CollisionOpen && collision.State != protocol.CollisionNegotiating {
			return protocol.Collision{}, fmt.Errorf("cannot negotiate collision in state %s", collision.State)
		}
		collision.State = params.State
	case protocol.CollisionYielded:
		if collision.State != protocol.CollisionOpen && collision.State != protocol.CollisionNegotiating {
			return protocol.Collision{}, fmt.Errorf("cannot yield collision in state %s", collision.State)
		}
		if params.Owner == "" || params.Owner == params.Actor || (params.Owner != collision.Actors[0] && params.Owner != collision.Actors[1]) {
			return protocol.Collision{}, errors.New("yield requires the other collision participant as owner")
		}
		collision.State = params.State
		collision.YieldedBy = params.Actor
		collision.Owner = params.Owner
	case protocol.CollisionResolved:
		if collision.State != protocol.CollisionYielded {
			return protocol.Collision{}, fmt.Errorf("collision must be yielded before resolution, currently %s", collision.State)
		}
		if params.Actor != collision.Owner {
			return protocol.Collision{}, errors.New("only the collision owner can resolve after yield")
		}
		if strings.TrimSpace(params.Resolution) == "" {
			return protocol.Collision{}, errors.New("resolution is required")
		}
		collision.State = params.State
		collision.Resolution = strings.TrimSpace(params.Resolution)
		collision.ResolvedBy = params.Actor
		now := e.now().UTC()
		collision.ResolvedAt = &now
	default:
		return protocol.Collision{}, fmt.Errorf("invalid collision transition %q", params.State)
	}
	at := e.now().UTC()
	if err := e.record("collision.transitioned", protocol.CollisionTransitionEvent{
		CollisionID: collision.ID, Actor: params.Actor, From: e.collisions[params.CollisionID].State,
		To: collision.State, Owner: collision.Owner, YieldedBy: collision.YieldedBy,
		Resolution: collision.Resolution, At: at,
	}); err != nil {
		return protocol.Collision{}, err
	}
	return e.collisions[params.CollisionID], nil
}

func sameActors(pair [2]string, left, right string) bool {
	return (pair[0] == left && pair[1] == right) || (pair[0] == right && pair[1] == left)
}

func intersect(left, right []string) []string {
	set := make(map[string]struct{}, len(right))
	for _, path := range right {
		set[path] = struct{}{}
	}
	var result []string
	for _, path := range left {
		if _, ok := set[path]; ok {
			result = append(result, path)
		}
	}
	return result
}

func uniqueCollisions(values []protocol.Collision) []protocol.Collision {
	seen := make(map[string]struct{}, len(values))
	result := make([]protocol.Collision, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value.ID]; ok {
			continue
		}
		seen[value.ID] = struct{}{}
		result = append(result, value)
	}
	return result
}

func collisionKey(actors [2]string, path string) string {
	sum := sha256.Sum256([]byte(actors[0] + "\x00" + actors[1] + "\x00" + path))
	return hex.EncodeToString(sum[:])
}

func randomUUID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(value[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}
