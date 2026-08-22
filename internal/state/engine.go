package state

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	}
	for _, event := range events {
		if event.Sequence <= engine.eventSequence {
			return nil, fmt.Errorf("event sequence %d is not increasing", event.Sequence)
		}
		if err := engine.apply(event); err != nil {
			return nil, fmt.Errorf("replay event %d: %w", event.Sequence, err)
		}
		engine.eventSequence = event.Sequence
	}
	return engine, nil
}

func (e *Engine) record(eventType string, value any) error {
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
		return err
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
	case "collision.upserted":
		collision, err := decode[protocol.Collision](event)
		if err != nil {
			return err
		}
		e.collisions[collision.ID] = collision
		e.collisionByKey[collision.Key] = collision.ID
	case "message.enqueued":
		message, err := decode[protocol.Message](event)
		if err != nil {
			return err
		}
		e.messages[message.ID] = message
		e.mailboxes[message.To] = append(e.mailboxes[message.To], message.ID)
		e.globalSequence = max(e.globalSequence, message.GlobalSequence)
		e.senderSequences[message.From] = max(e.senderSequences[message.From], message.SenderSequence)
		e.recipientSequences[message.To] = max(e.recipientSequences[message.To], message.RecipientSequence)
	case "session.event":
		if _, err := decode[protocol.SessionEvent](event); err != nil {
			return err
		}
	case "test.result":
		if _, err := decode[protocol.TestResult](event); err != nil {
			return err
		}
	case "checkpoint.requested":
		request, err := decode[protocol.CheckpointRequest](event)
		if err != nil {
			return err
		}
		e.checkpoints[request.ID] = request
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
	// Presence is lease-based and intentionally ephemeral. Journaling a fsynced
	// heartbeat every two seconds would grow the log and serialize all clients
	// behind disk latency; sessions re-register after daemon restart.
	e.actors[actor.Address] = actor
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
		_ = e.record("actor.upserted", actor)
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
		if strings.HasPrefix(normalized, "change:") && actor.JJ != nil {
			matched = strings.HasPrefix(actor.JJ.ChangeID, strings.TrimPrefix(normalized, "change:"))
		} else if strings.HasPrefix(normalized, "git:") && actor.Git != nil {
			matched = strings.HasPrefix(actor.Git.Head, strings.TrimPrefix(normalized, "git:"))
		} else if actor.Alias == normalized {
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
		id = randomID("msg")
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
		collision.State = protocol.CollisionNegotiating
		collision.UpdatedAt = e.now().UTC()
		if err := e.record("collision.upserted", collision); err != nil {
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
	if _, ok := e.actors[result.Actor]; !ok {
		return protocol.TestResult{}, fmt.Errorf("unknown actor %q", result.Actor)
	}
	if result.ID == "" || result.Command == "" {
		return protocol.TestResult{}, errors.New("test result id and command are required")
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

func (e *Engine) RequestCheckpoint(request protocol.CheckpointRequest) (protocol.CheckpointRequest, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if existing, ok := e.checkpoints[request.ID]; ok {
		return existing, nil
	}
	actor, ok := e.actors[request.Actor]
	if !ok {
		return protocol.CheckpointRequest{}, fmt.Errorf("unknown actor %q", request.Actor)
	}
	if request.ID == "" || request.CheckpointKind == "" {
		return protocol.CheckpointRequest{}, errors.New("checkpoint id and kind are required")
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
	if request.JournalEnd == 0 {
		request.JournalEnd = e.eventSequence
	}
	if request.JournalStart == 0 {
		request.JournalStart = request.JournalEnd
		for _, previous := range e.checkpoints {
			if previous.Actor == request.Actor && previous.SessionGeneration == request.SessionGeneration && previous.RepositoryUUID == request.RepositoryUUID && previous.WorkspaceUUID == request.WorkspaceUUID && previous.CheckpointKind == request.CheckpointKind && previous.JournalEnd < request.JournalStart {
				request.JournalStart = previous.JournalEnd + 1
			}
		}
	}
	if request.JournalStart > request.JournalEnd {
		return protocol.CheckpointRequest{}, errors.New("checkpoint journal range is invalid")
	}
	if err := e.record("checkpoint.requested", request); err != nil {
		return protocol.CheckpointRequest{}, err
	}
	return request, nil
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
		now := e.now().UTC()
		collision.ResolvedAt = &now
	default:
		return protocol.Collision{}, fmt.Errorf("invalid collision transition %q", params.State)
	}
	collision.UpdatedAt = e.now().UTC()
	if err := e.record("collision.upserted", collision); err != nil {
		return protocol.Collision{}, err
	}
	return collision, nil
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

func randomID(prefix string) string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return prefix + ":" + hex.EncodeToString(value[:])
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
