package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/AndrewPBerg/agent-bridge/internal/client"
	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
	"github.com/AndrewPBerg/agent-bridge/internal/provenance"
	"github.com/AndrewPBerg/agent-bridge/internal/server"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
	"github.com/AndrewPBerg/agent-bridge/internal/store"
)

func main() {
	// Provenance, journals, WAL files, and sockets are private by default.
	syscall.Umask(0o077)
	if err := execute(os.Args[1:]); err != nil {
		if !jsonOutput {
			fmt.Fprintln(os.Stderr, "agent-bridge:", err)
		}
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}
	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "ping":
		return callAndPrint("ping", map[string]any{})
	case "stop":
		return callAndPrint("daemon.shutdown", map[string]any{})
	case "sessions":
		return sessions(args[1:])
	case "scopes":
		return callAndPrint("provenance.scopes", map[string]any{})
	case "send":
		return send(args[1:])
	case "request":
		return request(args[1:])
	case "provenance":
		return provenanceCommand(args[1:])
	case "doctor":
		return doctor(args[1:])
	case "version", "--version", "-v":
		fmt.Println("agent-bridge dev")
		return nil
	default:
		return usageError()
	}
}

func usageError() error {
	return errors.New("usage: agent-bridge <serve|stop|ping|sessions|scopes|send|request|provenance|doctor|version>")
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	socket := flags.String("socket", defaultSocket(), "Unix socket path")
	journalPath := flags.String("journal", defaultJournal(), "append-only event journal")
	databasePath := flags.String("database", defaultDatabase(), "SQLite provenance database")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if os.Getenv("AGENT_BRIDGE_STATE_DIR") == "" && *journalPath == defaultJournal() {
		if err := migrateLegacyJournal(*journalPath); err != nil {
			return err
		}
	}
	journal, events, err := store.Open(*journalPath)
	if err != nil {
		return err
	}
	defer journal.Close()
	database, err := provenance.Open(*databasePath)
	if err != nil {
		return err
	}
	defer database.Close()
	if err := database.ProjectAll(events); err != nil {
		return fmt.Errorf("backfill provenance database: %w", err)
	}
	initialSequence := uint64(0)
	if len(events) > 0 {
		initialSequence = events[len(events)-1].Sequence
	}
	appender := provenance.NewProjectingAppender(journal, database, initialSequence)
	defer appender.Close()
	engine, err := state.New(appender, events, state.Options{})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	fmt.Fprintf(os.Stderr, "agent-bridge listening on %s (replayed %d events, provenance %s)\n", *socket, len(events), *databasePath)
	return server.NewWithProvenance(engine, database, *socket, appender).Serve(ctx)
}

func sessions(args []string) error {
	flags := flag.NewFlagSet("sessions", flag.ContinueOnError)
	includeStale := flags.Bool("all", false, "include stale sessions")
	repositoryID := flags.String("repo", "", "repository ID authority scope")
	workspaceID := flags.String("workspace", "", "workspace ID authority scope")
	directory := flags.String("under", "", "directory scope (cwd or recent mutation)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return callAndPrint("sessions.list", map[string]any{
		"include_stale": *includeStale, "repository_id": *repositoryID, "workspace_id": *workspaceID, "directory": *directory,
	})
}

func send(args []string) error {
	flags := flag.NewFlagSet("send", flag.ContinueOnError)
	from := flags.String("from", "", "canonical sender address")
	id := flags.String("id", "", "idempotency key for safe retries")
	clientSequence := flags.Uint64("sequence", 0, "sender-assigned sequence")
	generation := flags.Uint64("generation", 0, "sender session generation")
	if err := flags.Parse(args); err != nil {
		return err
	}
	values := flags.Args()
	if *from == "" || len(values) < 2 {
		return errors.New("usage: agent-bridge send --from harness:session [--sequence N] <target> <message>")
	}
	return callAndPrint("message.send", protocol.SendParams{
		ID:                *id,
		From:              *from,
		To:                values[0],
		Body:              strings.Join(values[1:], " "),
		ClientSequence:    *clientSequence,
		SessionGeneration: *generation,
	})
}

func request(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: agent-bridge request <method> '<json params>'")
	}
	var params any
	if err := json.Unmarshal([]byte(args[1]), &params); err != nil {
		return fmt.Errorf("decode JSON params: %w", err)
	}
	return callAndPrint(args[0], params)
}

func callAndPrint(method string, params any) error {
	var result any
	if err := client.New(defaultSocket()).Call(context.Background(), method, params, &result); err != nil {
		return err
	}
	if jsonOutput {
		return printJSON(map[string]any{"ok": true, "data": result})
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func stateDir() string {
	if value := os.Getenv("AGENT_BRIDGE_STATE_DIR"); value != "" {
		return value
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "agent-bridge")
	}
	return filepath.Join(home, ".agent-bridge")
}

func defaultSocket() string {
	if value := os.Getenv("AGENT_BRIDGE_SOCKET"); value != "" {
		return value
	}
	return filepath.Join(stateDir(), "bridge.sock")
}

func defaultJournal() string {
	return filepath.Join(stateDir(), "events.jsonl")
}

func defaultDatabase() string {
	return filepath.Join(stateDir(), "agent-bridge.db")
}
