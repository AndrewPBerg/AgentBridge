package state

import (
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestLaunchFamilyMessagingAllowsParentsAndWorkUnitSiblingsOnly(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	parent := register(t, engine, "parent")
	childA := register(t, engine, "child-a")
	childB := register(t, engine, "child-b")
	outsider := register(t, engine, "outsider")
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: testActorUUID("swarm-unit"), RepositoryUUID: parent.RepositoryUUID, WorkspaceUUID: parent.WorkspaceUUID, Objective: "swarm", CreatedBy: parent.Address})
	if err != nil {
		t.Fatal(err)
	}
	for _, launch := range []struct{ id, child string }{{testActorUUID("launch-a"), childA.Address}, {testActorUUID("launch-b"), childB.Address}} {
		if _, err := engine.CreateLaunch(protocol.LaunchCreateParams{LaunchUUID: launch.id, ParentActors: []string{parent.Address}}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.AttachLaunchWorkUnit(protocol.LaunchWorkUnitAttachParams{LaunchUUID: launch.id, WorkUnitUUID: unit.UUID}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.AttachLaunchChild(protocol.LaunchChildAttachParams{LaunchUUID: launch.id, ChildActor: launch.child}); err != nil {
			t.Fatal(err)
		}
	}
	for _, pair := range [][2]string{{childA.Address, parent.Address}, {parent.Address, childA.Address}, {childA.Address, childB.Address}, {parent.Address, outsider.Address}} {
		if _, err := engine.Send(protocol.SendParams{From: pair[0], To: pair[1], Body: "allowed"}); err != nil {
			t.Fatalf("%s -> %s: %v", pair[0], pair[1], err)
		}
	}
	for _, pair := range [][2]string{{childA.Address, outsider.Address}, {outsider.Address, childA.Address}} {
		if _, err := engine.Send(protocol.SendParams{From: pair[0], To: pair[1], Body: "denied"}); err == nil {
			t.Fatalf("%s -> %s unexpectedly allowed", pair[0], pair[1])
		}
	}
}
