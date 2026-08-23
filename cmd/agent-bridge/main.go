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
	watchsidecar "github.com/AndrewPBerg/agent-bridge/internal/watchman"
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

//nolint:cyclop // startup keeps ownership, replay, projection, Watchman, and serving in one ordered lifecycle.
func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	socket := flags.String("socket", defaultSocket(), "Unix socket path")
	journalPath := flags.String("journal", defaultJournal(), "append-only event journal")
	databasePath := flags.String("database", defaultDatabase(), "SQLite provenance database")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if os.Getenv("AGENT_BRIDGE_STATE_DIR") == "" && os.Getenv("INVOCATION_ID") == "" && os.Getenv("AGENT_BRIDGE_ALLOW_UNMANAGED") != "1" {
		return errors.New("production daemon lifecycle is managed by systemd; run: systemctl --user start agent-bridge.service")
	}
	if os.Getenv("AGENT_BRIDGE_STATE_DIR") == "" && *journalPath == defaultJournal() {
		if err := migrateLegacyJournal(*journalPath); err != nil {
			return err
		}
	}
	metadata, err := newDaemonMetadata(*socket, *databasePath, *journalPath)
	if err != nil {
		return fmt.Errorf("create daemon metadata: %w", err)
	}
	if err := writeDaemonMetadata(metadata); err != nil {
		return fmt.Errorf("write daemon metadata: %w", err)
	}
	defer removeDaemonMetadata(metadata)
	journal, events, err := store.Open(*journalPath)
	if err != nil {
		return err
	}
	defer closeQuietly(journal)
	database, err := provenance.Open(*databasePath)
	if err != nil {
		return err
	}
	defer closeQuietly(database)
	projectedSequence, err := database.ProjectedSequence()
	if err != nil {
		return fmt.Errorf("read provenance projection sequence: %w", err)
	}
	if projectedSequence > uint64(len(events)) {
		return fmt.Errorf("provenance projection sequence %d is ahead of journal sequence %d", projectedSequence, len(events))
	}
	if err := database.ProjectAll(events[projectedSequence:]); err != nil {
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
	watchManager := watchsidecar.New(engine)
	go watchManager.Run(ctx)
	fmt.Fprintf(os.Stderr, "agent-bridge listening on %s (replayed %d events, provenance %s, watchman %t)\n", *socket, len(events), *databasePath, watchManager.Available())
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
		"include_stale": *includeStale, "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID, "directory": *directory,
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
