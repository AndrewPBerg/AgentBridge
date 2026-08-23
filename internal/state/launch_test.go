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

func TestLaunchTerminationIsReplayableAndPreventsAttachment(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	parent := register(t, engine, "parent")
	child := register(t, engine, "child")
	launchUUID := testActorUUID("launch")
	if _, err := engine.CreateLaunch(protocol.LaunchCreateParams{LaunchUUID: launchUUID, ParentActors: []string{parent.Address}}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	launch, err := engine.TerminateLaunch(protocol.LaunchTerminateParams{LaunchUUID: launchUUID, Reason: "pi unavailable"})
	if err != nil || launch.TerminatedAt == nil || launch.TerminationReason != "pi unavailable" {
		t.Fatalf("terminated launch = %#v, %v", launch, err)
	}
	if _, err := engine.AttachLaunchChild(protocol.LaunchChildAttachParams{LaunchUUID: launchUUID, ChildActor: child.Address}); err == nil {
		t.Fatal("terminated launch accepted a child")
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	got, err := replayed.Launch(launchUUID)
	if err != nil || got.TerminatedAt == nil || got.TerminationReason != "pi unavailable" {
		t.Fatalf("replayed terminated launch = %#v, %v", got, err)
	}
}

//nolint:cyclop // One regression test verifies rejection, atomic registration, attachment, and replay.
func TestRegisterWithLaunchIsAtomicAndReplayable(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	parent := register(t, engine, "parent")
	childUUID := testActorUUID("atomic-child")
	child := protocol.Actor{
		Address: childUUID, SessionUUID: childUUID, Harness: "pi", CWD: "/repo",
		RepositoryUUID: parent.RepositoryUUID, WorkspaceUUID: parent.WorkspaceUUID,
	}
	if _, err := engine.RegisterWithLaunch(child, testActorUUID("missing-launch")); err == nil {
		t.Fatal("unknown launch registration succeeded")
	}
	for _, actor := range engine.Sessions(true) {
		if actor.Address == childUUID {
			t.Fatal("failed launch registration left an actor behind")
		}
	}
	launchUUID := testActorUUID("atomic-launch")
	if _, err := engine.CreateLaunch(protocol.LaunchCreateParams{LaunchUUID: launchUUID, ParentActors: []string{parent.Address}}); err != nil {
		t.Fatal(err)
	}
	registered, err := engine.RegisterWithLaunch(child, launchUUID)
	if err != nil || registered.Address != childUUID {
		t.Fatalf("register with launch = %#v, %v", registered, err)
	}
	launch, err := engine.Launch(launchUUID)
	if err != nil || launch.ChildActor != childUUID {
		t.Fatalf("attached launch = %#v, %v", launch, err)
	}
	if got := journal.events[len(journal.events)-1].Type; got != "actor.registered_with_launch" {
		t.Fatalf("atomic event type = %q", got)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	launch, err = replayed.Launch(launchUUID)
	if err != nil || launch.ChildActor != childUUID {
		t.Fatalf("replayed launch = %#v, %v", launch, err)
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
