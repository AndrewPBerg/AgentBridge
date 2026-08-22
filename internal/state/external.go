package state

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:gocritic // actor values are immutable state snapshots.
func isUnknownActor(actor protocol.Actor) bool {
	return actor.ActorKind == "unknown" || !actor.Addressable
}

//nolint:gocritic // continuity values are immutable validation inputs.
func validateWatchContinuity(status protocol.WatchContinuity) error {
	if protocol.ValidateUUID(status.RepositoryUUID) != nil || protocol.ValidateUUID(status.WorkspaceUUID) != nil || status.At.IsZero() {
		return errors.New("invalid watch continuity identity")
	}
	if !slices.Contains([]string{"lost", "restored"}, status.State) {
		return errors.New("invalid watch continuity state")
	}
	return nil
}

//nolint:cyclop,gocritic // validation keeps every externally supplied invariant explicit.
func validateExternalChange(change protocol.ExternalChange) error {
	for name, value := range map[string]string{"external_change_uuid": change.ID, "repository_uuid": change.RepositoryUUID, "workspace_uuid": change.WorkspaceUUID, "unknown_actor_uuid": change.UnknownActor} {
		if err := protocol.ValidateUUID(value); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	if change.UnknownActor != UnknownActorUUID(change.WorkspaceUUID) || change.Actor != change.UnknownActor {
		return errors.New("external change must use the workspace unknown actor")
	}
	if change.ID == "" || change.Path == "" || change.ChangeKind == "" || change.IntervalEndedAt.Before(change.IntervalStartedAt) {
		return errors.New("invalid external change")
	}
	if !slices.Contains([]string{"current", "reconciled", "gap"}, change.ContinuityState) {
		return errors.New("invalid external change continuity state")
	}
	if !slices.Contains([]string{"created", "modified", "deleted", "type_changed"}, change.ChangeKind) {
		return errors.New("invalid external change kind")
	}
	for _, snapshot := range []*protocol.FileSnapshot{change.Before, change.After} {
		if snapshot != nil && snapshot.Path != change.Path {
			return errors.New("external change snapshot path mismatch")
		}
	}
	if slices.Contains(change.RelatedIntentIDs, "") {
		return errors.New("empty related intent")
	}
	return nil
}

func (e *Engine) externalWorkspaceRoot(repositoryUUID, workspaceUUID string) (string, bool) {
	for address := range e.actors {
		actor := e.actors[address]
		if actor.RepositoryUUID != repositoryUUID || actor.WorkspaceUUID != workspaceUUID {
			continue
		}
		root := actor.RepositoryRoot
		if root == "" {
			root = actor.CWD
		}
		if actor.WorkspaceRoot != "" {
			root = actor.WorkspaceRoot
		}
		return root, true
	}
	return "", false
}

// ObserveExternalChange durably records one external observation. Repeating an
// identical ID is idempotent; a different payload is rejected.
//
//nolint:gocritic // public API captures an immutable observation value
func (e *Engine) ObserveExternalChange(change protocol.ExternalChange) (protocol.ExternalChange, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if change.ID != "" {
		if existing, ok := e.externalChanges[change.ID]; ok {
			if reflect.DeepEqual(existing, change) {
				return existing, nil
			}
			return protocol.ExternalChange{}, fmt.Errorf("external change %q conflicts with existing payload", change.ID)
		}
	}
	change.UnknownActor = UnknownActorUUID(change.WorkspaceUUID)
	if change.Actor == "" {
		change.Actor = change.UnknownActor
	}
	if root, ok := e.externalWorkspaceRoot(change.RepositoryUUID, change.WorkspaceUUID); !ok {
		return protocol.ExternalChange{}, errors.New("external change references unknown workspace")
	} else if !underDirectory(change.Path, root) {
		return protocol.ExternalChange{}, errors.New("external change path is outside workspace")
	}
	if change.IntervalStartedAt.IsZero() {
		change.IntervalStartedAt = e.now().UTC()
	}
	if change.IntervalEndedAt.IsZero() {
		change.IntervalEndedAt = change.IntervalStartedAt
	}
	if change.ContinuityState == "" {
		change.ContinuityState = "current"
	}
	if err := validateExternalChange(change); err != nil {
		return protocol.ExternalChange{}, err
	}
	if err := e.record("external_change.observed", change); err != nil {
		return protocol.ExternalChange{}, err
	}
	return change, nil
}

func (e *Engine) setWatchContinuity(status *protocol.WatchContinuity, eventType string) (protocol.WatchContinuity, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if status.At.IsZero() {
		status.At = e.now().UTC()
	}
	status.State = strings.TrimPrefix(eventType, "watch.continuity_")
	if _, ok := e.externalWorkspaceRoot(status.RepositoryUUID, status.WorkspaceUUID); !ok {
		return protocol.WatchContinuity{}, errors.New("watch continuity references unknown workspace")
	}
	if err := validateWatchContinuity(*status); err != nil {
		return protocol.WatchContinuity{}, err
	}
	if existing, ok := e.continuity[status.WorkspaceUUID]; ok && reflect.DeepEqual(existing, *status) {
		return existing, nil
	}
	if err := e.record(eventType, *status); err != nil {
		return protocol.WatchContinuity{}, err
	}
	return *status, nil
}

// WatchContinuityLost records a lost filesystem-watch interval.
//
//nolint:gocritic // public API captures an immutable continuity value.
func (e *Engine) WatchContinuityLost(status protocol.WatchContinuity) (protocol.WatchContinuity, error) {
	return e.setWatchContinuity(&status, "watch.continuity_lost")
}

// WatchContinuityRestored records restored filesystem-watch continuity.
//
//nolint:gocritic // public API captures an immutable continuity value.
func (e *Engine) WatchContinuityRestored(status protocol.WatchContinuity) (protocol.WatchContinuity, error) {
	return e.setWatchContinuity(&status, "watch.continuity_restored")
}

func snapshotIdentityEqual(left, right *protocol.FileSnapshot) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Path == right.Path && left.Exists == right.Exists && left.Kind == right.Kind && left.SHA256 == right.SHA256 && left.Size == right.Size
}

// HasActiveIntent reports whether an instrumented mutation is still active on
// an exact path in a workspace.
func (e *Engine) HasActiveIntent(workspaceUUID, path string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.intents {
		intent := e.intents[id]
		if intent.WorkspaceUUID == workspaceUUID && intent.CompletedAt == nil && slices.Contains(intent.Paths, path) {
			return true
		}
	}
	return false
}

// MatchIntentTransition returns instrumented intents that exactly explain one
// observed filesystem transition inside the supplied observation interval.
func (e *Engine) MatchIntentTransition(workspaceUUID, path string, before, after *protocol.FileSnapshot, startedAt, endedAt time.Time) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	result := make([]string, 0)
	for id := range e.intents {
		intent := e.intents[id]
		if intent.WorkspaceUUID != workspaceUUID || intent.CompletedAt == nil || intent.CompletedAt.Before(startedAt) || intent.CompletedAt.After(endedAt) {
			continue
		}
		var intentBefore, intentAfter *protocol.FileSnapshot
		for index := range intent.Before {
			if intent.Before[index].Path == path {
				intentBefore = &intent.Before[index]
				break
			}
		}
		for index := range intent.After {
			if intent.After[index].Path == path {
				intentAfter = &intent.After[index]
				break
			}
		}
		if snapshotIdentityEqual(before, intentBefore) && snapshotIdentityEqual(after, intentAfter) {
			result = append(result, intent.ID)
		}
	}
	slices.Sort(result)
	return result
}

// NotifyExternalChange delivers one non-addressable observation to every
// active actor in the affected workspace. The unknown actor is the source
// label; only the daemon can enqueue this notification.
//
//nolint:gocritic // public API captures an immutable external-change value.
func (e *Engine) NotifyExternalChange(change protocol.ExternalChange) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for address := range e.actors {
		actor := e.actors[address]
		if actor.WorkspaceUUID != change.WorkspaceUUID || !e.active(actor) || isUnknownActor(actor) {
			continue
		}
		body := fmt.Sprintf("UNATTRIBUTED EXTERNAL CHANGE observed on %s (%s). Source %s is unknown and non-addressable. Re-read before writing.", change.Path, change.ChangeKind, change.UnknownActor)
		message := e.nextMessage(change.UnknownActor, actor.Address, "external_change", body, protocol.SendParams{ID: change.ID + ":" + actor.Address}, "")
		if err := e.enqueue(message); err != nil {
			return err
		}
	}
	return nil
}

// ExternalChange returns an observed change by canonical UUID.
func (e *Engine) ExternalChange(id string) (protocol.ExternalChange, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	change, ok := e.externalChanges[id]
	if !ok {
		return protocol.ExternalChange{}, fmt.Errorf("unknown external change %q", id)
	}
	return change, nil
}
