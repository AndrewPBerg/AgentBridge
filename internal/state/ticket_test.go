package state

import (
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // One end-to-end replay test keeps both mutable ticket aggregates aligned.
func TestDirectionAndWorkUnitTicketUpdatesReplay(t *testing.T) {
	engine, journal, now := newTestEngine(t)
	actor := register(t, engine, "01234567-89ab-4def-8123-456789abcdef")
	direction, err := engine.CreateDirection(protocol.Direction{UUID: "11234567-89ab-4def-8123-456789abcdef", Objective: "direction", CreatedBy: actor.Address})
	if err != nil {
		t.Fatal(err)
	}
	unit, err := engine.CreateWorkUnit(protocol.WorkUnit{UUID: "21234567-89ab-4def-8123-456789abcdef", RepositoryUUID: actor.RepositoryUUID, WorkspaceUUID: actor.WorkspaceUUID, Objective: "unit", CreatedBy: actor.Address, DirectionUUID: direction.UUID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.JoinWorkUnit(protocol.WorkUnitActorParams{WorkUnitUUID: unit.UUID, Actor: actor.Address}); err != nil {
		t.Fatal(err)
	}
	directionTickets := protocol.Tickets(`[{"key":"DIR-1","label":"direction"}]`)
	unitTickets := protocol.Tickets(`[{"key":"WU-1","label":"unit"}]`)
	*now = now.Add(time.Second)
	updatedDirection, err := engine.UpdateDirection(protocol.DirectionUpdateParams{DirectionUUID: direction.UUID, Actor: actor.Address, Tickets: &directionTickets})
	if err != nil {
		t.Fatal(err)
	}
	updatedUnit, err := engine.UpdateWorkUnit(protocol.WorkUnitUpdateParams{WorkUnitUUID: unit.UUID, Actor: actor.Address, Tickets: &unitTickets})
	if err != nil {
		t.Fatal(err)
	}
	if updatedDirection.Tickets != directionTickets || updatedUnit.Tickets != unitTickets {
		t.Fatalf("ticket updates = %s, %s", updatedDirection.Tickets, updatedUnit.Tickets)
	}
	replayed, err := New(&memoryJournal{}, journal.events, Options{Now: func() time.Time { return *now }})
	if err != nil {
		t.Fatal(err)
	}
	gotDirection, err := replayed.Direction(direction.UUID)
	if err != nil || gotDirection.Tickets != directionTickets {
		t.Fatalf("replayed direction tickets = %s, err %v", gotDirection.Tickets, err)
	}
	gotUnit, _, err := replayed.WorkUnit(unit.UUID)
	if err != nil || gotUnit.Tickets != unitTickets {
		t.Fatalf("replayed work unit tickets = %s, err %v", gotUnit.Tickets, err)
	}
}
