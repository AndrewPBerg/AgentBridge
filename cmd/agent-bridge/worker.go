package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

type workerContext struct {
	actor    protocol.Actor
	workUnit protocol.WorkUnit
}

type workerWorkUnitResponse struct {
	Unit protocol.WorkUnit `json:"work_unit"`
}

type workerResultIDs struct{ values []string }

func (r *workerResultIDs) String() string { return strings.Join(r.values, ",") }
func (r *workerResultIDs) Set(value string) error {
	if strings.TrimSpace(value) == "" {
		return errors.New("--test-result must not be empty")
	}
	r.values = append(r.values, value)
	return nil
}

func workerContextFromEnvironment() (workerContext, error) {
	actorUUID := os.Getenv("AGENT_BRIDGE_ACTOR_UUID")
	workUnitUUID := os.Getenv("AGENT_BRIDGE_WORK_UNIT_UUID")
	if err := protocol.ValidateUUID(actorUUID); err != nil {
		return workerContext{}, fmt.Errorf("AGENT_BRIDGE_ACTOR_UUID: %w", err)
	}
	if err := protocol.ValidateUUID(workUnitUUID); err != nil {
		return workerContext{}, fmt.Errorf("AGENT_BRIDGE_WORK_UNIT_UUID: %w", err)
	}
	var sessions struct {
		Actors []protocol.Actor `json:"actors"`
	}
	if err := workerCall("sessions.list", map[string]any{"include_stale": true}, &sessions); err != nil {
		return workerContext{}, err
	}
	var actor *protocol.Actor
	for index := range sessions.Actors {
		candidate := &sessions.Actors[index]
		if candidate.SessionUUID == actorUUID {
			if actor != nil {
				return workerContext{}, fmt.Errorf("actor UUID %q matches multiple sessions", actorUUID)
			}
			actor = candidate
		}
	}
	if actor == nil {
		return workerContext{}, fmt.Errorf("actor UUID %q is not registered", actorUUID)
	}
	var work workerWorkUnitResponse
	if err := workerCall("work_unit.get", map[string]any{"work_unit_uuid": workUnitUUID}, &work); err != nil {
		return workerContext{}, err
	}
	if work.Unit.UUID != workUnitUUID {
		return workerContext{}, fmt.Errorf("work unit response has unexpected UUID %q", work.Unit.UUID)
	}
	if actor.RepositoryUUID != work.Unit.RepositoryUUID || actor.WorkspaceUUID != work.Unit.WorkspaceUUID {
		return workerContext{}, errors.New("actor and work unit scopes do not match")
	}
	return workerContext{actor: *actor, workUnit: work.Unit}, nil
}

func workerCall(method string, params, result any) error {
	return client.New(defaultSocket()).Call(context.Background(), method, params, result)
}

func worker(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: agent-bridge worker <status|poll|ack|send|test|checkpoint|transition>")
	}
	ctx, err := workerContextFromEnvironment()
	if err != nil {
		return err
	}
	switch args[0] {
	case "status":
		if len(args) != 1 {
			return errors.New("usage: agent-bridge worker status")
		}
		return callAndPrintValue(map[string]any{"actor": ctx.actor, "work_unit": ctx.workUnit})
	case "poll":
		return workerPoll(&ctx, args[1:])
	case "ack":
		return workerAck(&ctx, args[1:])
	case "send":
		return workerSend(&ctx, args[1:])
	case "test":
		return workerTest(&ctx, args[1:])
	case "checkpoint":
		return workerCheckpoint(&ctx, args[1:])
	case "transition":
		return workerTransition(&ctx, args[1:])
	default:
		return fmt.Errorf("unknown worker command %q", args[0])
	}
}

func callAndPrintValue(value any) error {
	if jsonOutput {
		return printJSON(map[string]any{"ok": true, "data": value})
	}
	return printJSON(value)
}

func workerPoll(ctx *workerContext, args []string) error {
	flags := flag.NewFlagSet("worker poll", flag.ContinueOnError)
	limit := flags.Int("limit", 0, "maximum messages")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 || *limit < 0 {
		return errors.New("usage: agent-bridge worker poll [--limit N]")
	}
	return callAndPrint("mailbox.poll", protocol.PollParams{Actor: ctx.actor.Address, Limit: *limit})
}

func workerAck(ctx *workerContext, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: agent-bridge worker ack <message-id> [message-id ...]")
	}
	return callAndPrint("mailbox.ack", protocol.AckParams{Actor: ctx.actor.Address, MessageIDs: args})
}

func workerSend(ctx *workerContext, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: agent-bridge worker send <target> <body>")
	}
	return callAndPrint("message.send", protocol.SendParams{From: ctx.actor.Address, To: args[0], Body: strings.Join(args[1:], " "), SessionGeneration: ctx.actor.Generation})
}

func workerTest(ctx *workerContext, args []string) error {
	flags := flag.NewFlagSet("worker test", flag.ContinueOnError)
	id, command := flags.String("id", "", "test result ID"), flags.String("command", "", "command that ran")
	exitCode := flags.Int("exit-code", -999999, "process exit code")
	outcome := flags.String("outcome", "", "passed, failed, or blocked")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 || *id == "" || *command == "" || *exitCode == -999999 {
		return errors.New("usage: agent-bridge worker test --id ID --command COMMAND --exit-code CODE [--outcome OUTCOME]")
	}
	code := *exitCode
	result := protocol.TestResult{ID: *id, Actor: ctx.actor.Address, SessionGeneration: ctx.actor.Generation, Command: *command, CWD: mustGetwd(), ExitCode: &code, Outcome: protocol.TestOutcome(*outcome), RepositoryUUID: ctx.workUnit.RepositoryUUID, WorkspaceUUID: ctx.workUnit.WorkspaceUUID}
	return callAndPrint("test.result", map[string]any{"result": result})
}

func mustGetwd() string {
	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return cwd
}

func workerCheckpoint(ctx *workerContext, args []string) error {
	flags := flag.NewFlagSet("worker checkpoint", flag.ContinueOnError)
	id, kind, claim := flags.String("id", "", "checkpoint ID"), flags.String("kind", "", "checkpoint kind"), flags.String("claim", "", "claim statement")
	status := flags.String("status", string(protocol.ClaimAsserted), "asserted, verified, failed, or blocked")
	var testResults workerResultIDs
	flags.Var(&testResults, "test-result", "test result ID (repeatable)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 || *id == "" || *kind == "" || *claim == "" {
		return errors.New("usage: agent-bridge worker checkpoint --id ID --kind KIND --claim CLAIM [--status STATUS] [--test-result ID ...]")
	}
	claimStatus := protocol.CheckpointClaimStatus(*status)
	if claimStatus == protocol.ClaimVerified && (*kind == "test" || *kind == "build" || *kind == "runtime") && len(testResults.values) == 0 {
		return errors.New("verified test/build/runtime claims require --test-result evidence")
	}
	claims := []protocol.CheckpointClaim{{Kind: workerClaimKind(*kind), Statement: *claim, Status: claimStatus}}
	for ordinal := range testResults.values {
		claims[0].Evidence = append(claims[0].Evidence, protocol.CheckpointEvidenceRef{Kind: "test_result", Ordinal: ordinal})
	}
	request := protocol.CheckpointRequest{ID: *id, Actor: ctx.actor.Address, DeclaredBy: "agent", SessionGeneration: ctx.actor.Generation, RepositoryUUID: ctx.workUnit.RepositoryUUID, WorkspaceUUID: ctx.workUnit.WorkspaceUUID, WorkUnitUUID: ctx.workUnit.UUID, CheckpointKind: *kind, TestResultIDs: testResults.values, Claims: claims}
	return callAndPrint("checkpoint.request", protocol.CheckpointRequestParams{Request: request})
}

func workerClaimKind(checkpointKind string) string {
	if protocol.ValidCheckpointClaimKind(checkpointKind) {
		return checkpointKind
	}
	return "summary"
}

func workerTransition(ctx *workerContext, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: agent-bridge worker transition <state>")
	}
	return callAndPrint("work_unit.transition", protocol.WorkUnitTransitionParams{WorkUnitUUID: ctx.workUnit.UUID, Actor: ctx.actor.Address, State: protocol.WorkUnitState(args[0])})
}
