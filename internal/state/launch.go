package state

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func normalizeLaunchParents(parents []string) ([]string, error) {
	result := append([]string(nil), parents...)
	sort.Strings(result)
	for index, parent := range result {
		if err := protocol.ValidateUUID(parent); err != nil {
			return nil, fmt.Errorf("parent_actor_uuids[%d]: %w", index, err)
		}
		if index > 0 && parent == result[index-1] {
			return nil, fmt.Errorf("duplicate parent actor %q", parent)
		}
	}
	return result, nil
}

//nolint:cyclop // Immutable launch attachments require paired identity/time validation.
func validateLaunch(launch *protocol.Launch) error {
	if err := protocol.ValidateUUID(launch.UUID); err != nil {
		return fmt.Errorf("launch_uuid: %w", err)
	}
	parents, err := normalizeLaunchParents(launch.ParentActors)
	if err != nil || !reflect.DeepEqual(parents, launch.ParentActors) {
		if err != nil {
			return err
		}
		return errors.New("launch parent actors are not normalized")
	}
	for name, value := range map[string]string{"child_actor_uuid": launch.ChildActor, "work_unit_uuid": launch.WorkUnitUUID} {
		if value != "" {
			if err := protocol.ValidateUUID(value); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if launch.ChildActor != "" && launch.ChildAttachedAt == nil {
		return errors.New("attached child requires child_attached_at")
	}
	if launch.ChildActor == "" && launch.ChildAttachedAt != nil {
		return errors.New("unattached launch cannot have child_attached_at")
	}
	if launch.WorkUnitUUID != "" && launch.WorkUnitAttachedAt == nil {
		return errors.New("attached work unit requires work_unit_attached_at")
	}
	if launch.WorkUnitUUID == "" && launch.WorkUnitAttachedAt != nil {
		return errors.New("unattached launch cannot have work_unit_attached_at")
	}
	return nil
}

// CreateLaunch records a stable, caller-supplied launch and explicit parents.
func (e *Engine) CreateLaunch(params protocol.LaunchCreateParams) (protocol.Launch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := protocol.ValidateUUID(params.LaunchUUID); err != nil {
		return protocol.Launch{}, fmt.Errorf("launch_uuid: %w", err)
	}
	parents, err := normalizeLaunchParents(params.ParentActors)
	if err != nil {
		return protocol.Launch{}, err
	}
	for _, parent := range parents {
		if _, ok := e.actors[parent]; !ok {
			return protocol.Launch{}, fmt.Errorf("unknown parent actor %q", parent)
		}
	}
	if existing, ok := e.launches[params.LaunchUUID]; ok {
		if reflect.DeepEqual(existing.ParentActors, parents) {
			return existing, nil
		}
		return protocol.Launch{}, fmt.Errorf("launch %q conflicts with existing parents", params.LaunchUUID)
	}
	launch := protocol.Launch{UUID: params.LaunchUUID, ParentActors: parents, CreatedAt: e.now().UTC()}
	if err := e.record("launch.created", protocol.LaunchCreatedEvent{Launch: launch}); err != nil {
		return protocol.Launch{}, err
	}
	return launch, nil
}

// AttachLaunchChild records the child actor selected by a launch.
func (e *Engine) AttachLaunchChild(params protocol.LaunchChildAttachParams) (protocol.Launch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	launch, ok := e.launches[params.LaunchUUID]
	if !ok {
		return protocol.Launch{}, fmt.Errorf("unknown launch %q", params.LaunchUUID)
	}
	if err := protocol.ValidateUUID(params.ChildActor); err != nil {
		return protocol.Launch{}, fmt.Errorf("child_actor_uuid: %w", err)
	}
	if _, ok := e.actors[params.ChildActor]; !ok {
		return protocol.Launch{}, fmt.Errorf("unknown child actor %q", params.ChildActor)
	}
	if launch.ChildActor != "" {
		if launch.ChildActor == params.ChildActor {
			return launch, nil
		}
		return protocol.Launch{}, fmt.Errorf("launch %q already has child actor %q", launch.UUID, launch.ChildActor)
	}
	at := e.now().UTC()
	if err := e.record("launch.child_attached", protocol.LaunchChildAttachedEvent{LaunchUUID: launch.UUID, ChildActor: params.ChildActor, At: at}); err != nil {
		return protocol.Launch{}, err
	}
	return e.launches[launch.UUID], nil
}

// AttachLaunchWorkUnit records the optional WorkUnit associated with a launch.
func (e *Engine) AttachLaunchWorkUnit(params protocol.LaunchWorkUnitAttachParams) (protocol.Launch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	launch, ok := e.launches[params.LaunchUUID]
	if !ok {
		return protocol.Launch{}, fmt.Errorf("unknown launch %q", params.LaunchUUID)
	}
	if err := protocol.ValidateUUID(params.WorkUnitUUID); err != nil {
		return protocol.Launch{}, fmt.Errorf("work_unit_uuid: %w", err)
	}
	if _, ok := e.workUnits[params.WorkUnitUUID]; !ok {
		return protocol.Launch{}, fmt.Errorf("unknown work unit %q", params.WorkUnitUUID)
	}
	if launch.WorkUnitUUID != "" {
		if launch.WorkUnitUUID == params.WorkUnitUUID {
			return launch, nil
		}
		return protocol.Launch{}, fmt.Errorf("launch %q already has work unit %q", launch.UUID, launch.WorkUnitUUID)
	}
	at := e.now().UTC()
	if err := e.record("launch.work_unit_attached", protocol.LaunchWorkUnitAttachedEvent{LaunchUUID: launch.UUID, WorkUnitUUID: params.WorkUnitUUID, At: at}); err != nil {
		return protocol.Launch{}, err
	}
	return e.launches[launch.UUID], nil
}

// Launch returns one launch record.
func (e *Engine) Launch(uuid string) (protocol.Launch, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	launch, ok := e.launches[uuid]
	if !ok {
		return protocol.Launch{}, fmt.Errorf("unknown launch %q", uuid)
	}
	return launch, nil
}

func (e *Engine) applyLaunchCreated(created *protocol.LaunchCreatedEvent) error {
	launch := created.Launch
	if err := validateLaunch(&launch); err != nil {
		return err
	}
	if existing, ok := e.launches[launch.UUID]; ok {
		if reflect.DeepEqual(existing, launch) {
			return nil
		}
		return fmt.Errorf("launch %q already exists", launch.UUID)
	}
	for _, parent := range launch.ParentActors {
		if _, ok := e.actors[parent]; !ok {
			return fmt.Errorf("unknown launch parent actor %q", parent)
		}
	}
	e.launches[launch.UUID] = launch
	return nil
}

func (e *Engine) applyLaunchChildAttached(attached protocol.LaunchChildAttachedEvent) error {
	launch, ok := e.launches[attached.LaunchUUID]
	if !ok {
		return fmt.Errorf("unknown launch %q", attached.LaunchUUID)
	}
	if protocol.ValidateUUID(attached.LaunchUUID) != nil || protocol.ValidateUUID(attached.ChildActor) != nil || attached.At.IsZero() {
		return errors.New("invalid launch child attachment")
	}
	if _, ok := e.actors[attached.ChildActor]; !ok {
		return fmt.Errorf("unknown child actor %q", attached.ChildActor)
	}
	if launch.ChildActor != "" {
		if launch.ChildActor == attached.ChildActor && launch.ChildAttachedAt != nil && launch.ChildAttachedAt.Equal(attached.At) {
			return nil
		}
		return errors.New("launch child already attached")
	}
	at := attached.At.UTC()
	launch.ChildActor, launch.ChildAttachedAt = attached.ChildActor, &at
	e.launches[launch.UUID] = launch
	return nil
}

// launchFamilyAllows confines attached children to explicit parents and
// same-WorkUnit siblings. Unattached actors retain the existing open policy.
func (e *Engine) launchFamilyAllows(sender, recipient string) bool {
	var senderLaunches, recipientLaunches []protocol.Launch
	for _, launch := range e.launches {
		if launch.ChildActor == sender {
			senderLaunches = append(senderLaunches, launch)
		}
		if launch.ChildActor == recipient {
			recipientLaunches = append(recipientLaunches, launch)
		}
	}
	if len(senderLaunches) == 0 && len(recipientLaunches) == 0 {
		return true
	}
	for _, launch := range senderLaunches {
		if slices.Contains(launch.ParentActors, recipient) {
			return true
		}
	}
	for _, launch := range recipientLaunches {
		if slices.Contains(launch.ParentActors, sender) {
			return true
		}
	}
	for _, left := range senderLaunches {
		for _, right := range recipientLaunches {
			if left.WorkUnitUUID != "" && left.WorkUnitUUID == right.WorkUnitUUID && sharesLaunchParent(left, right) {
				return true
			}
		}
	}
	return false
}

func sharesLaunchParent(left, right protocol.Launch) bool {
	for _, parent := range left.ParentActors {
		if slices.Contains(right.ParentActors, parent) {
			return true
		}
	}
	return false
}

func (e *Engine) applyLaunchWorkUnitAttached(attached protocol.LaunchWorkUnitAttachedEvent) error {
	launch, ok := e.launches[attached.LaunchUUID]
	if !ok {
		return fmt.Errorf("unknown launch %q", attached.LaunchUUID)
	}
	if protocol.ValidateUUID(attached.LaunchUUID) != nil || protocol.ValidateUUID(attached.WorkUnitUUID) != nil || attached.At.IsZero() {
		return errors.New("invalid launch work unit attachment")
	}
	if _, ok := e.workUnits[attached.WorkUnitUUID]; !ok {
		return fmt.Errorf("unknown work unit %q", attached.WorkUnitUUID)
	}
	if launch.WorkUnitUUID != "" {
		if launch.WorkUnitUUID == attached.WorkUnitUUID && launch.WorkUnitAttachedAt != nil && launch.WorkUnitAttachedAt.Equal(attached.At) {
			return nil
		}
		return errors.New("launch work unit already attached")
	}
	at := attached.At.UTC()
	launch.WorkUnitUUID, launch.WorkUnitAttachedAt = attached.WorkUnitUUID, &at
	e.launches[launch.UUID] = launch
	return nil
}
