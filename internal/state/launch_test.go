package state

import (
	"reflect"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // One lifecycle test deliberately covers creation, idempotency, attachment, and replay.
func TestLaunchSupportsMultipleParentsAndReplayableAttachments(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	parentA := register(t, engine, "parent-a")
	parentB := register(t, engine, "parent-b")
	child := register(t, engine, "child")
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: testActorUUID("unit"), RepositoryUUID: parentA.RepositoryUUID, WorkspaceUUID: parentA.WorkspaceUUID, Objective: "work", CreatedBy: parentA.Address})
	if err != nil {
		t.Fatal(err)
	}
	launchUUID := testActorUUID("launch")
	launch, err := engine.CreateLaunch(protocol.LaunchCreateParams{LaunchUUID: launchUUID, ParentActors: []string{parentB.Address, parentA.Address}})
	if err != nil {
		t.Fatal(err)
	}
	if len(launch.ParentActors) != 2 || launch.ParentActors[0] > launch.ParentActors[1] || launch.ChildActor != "" || launch.WorkUnitUUID != "" {
		t.Fatalf("created launch = %#v", launch)
	}
	duplicate, err := engine.CreateLaunch(protocol.LaunchCreateParams{LaunchUUID: launchUUID, ParentActors: []string{parentA.Address, parentB.Address}})
	if err != nil || !reflect.DeepEqual(duplicate, launch) || len(journal.events) != 5 {
		t.Fatalf("idempotent create = %#v, %v, events=%d", duplicate, err, len(journal.events))
	}
	*now = now.Add(time.Second)
	launch, err = engine.AttachLaunchChild(protocol.LaunchChildAttachParams{LaunchUUID: launchUUID, ChildActor: child.Address})
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	launch, err = engine.AttachLaunchWorkUnit(protocol.LaunchWorkUnitAttachParams{LaunchUUID: launchUUID, WorkUnitUUID: unit.UUID})
	if err != nil {
		t.Fatal(err)
	}
	if launch.ChildActor != child.Address || launch.WorkUnitUUID != unit.UUID || launch.ChildAttachedAt == nil || launch.WorkUnitAttachedAt == nil {
		t.Fatalf("attached launch = %#v", launch)
	}
	if _, err := engine.AttachLaunchChild(protocol.LaunchChildAttachParams{LaunchUUID: launchUUID, ChildActor: parentA.Address}); err == nil {
		t.Fatal("different child attachment was accepted")
	}
	if _, err := engine.AttachLaunchWorkUnit(protocol.LaunchWorkUnitAttachParams{LaunchUUID: launchUUID, WorkUnitUUID: testActorUUID("other-unit")}); err == nil {
		t.Fatal("different work unit attachment was accepted")
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Launch(launchUUID)
	if err != nil || got.UUID != launch.UUID || got.ChildActor != child.Address || got.WorkUnitUUID != unit.UUID || len(got.ParentActors) != 2 {
		t.Fatalf("replayed launch = %#v, %v", got, err)
	}
}

func TestLaunchRejectsUnknownOrDuplicateParents(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	parent := register(t, engine, "parent")
	for name, params := range map[string]protocol.LaunchCreateParams{
		"unknown":   {LaunchUUID: testActorUUID("unknown"), ParentActors: []string{testActorUUID("missing")}},
		"duplicate": {LaunchUUID: testActorUUID("duplicate"), ParentActors: []string{parent.Address, parent.Address}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := engine.CreateLaunch(params); err == nil {
				t.Fatal("invalid launch parents accepted")
			}
		})
	}
}
