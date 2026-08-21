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
	"github.com/AndrewPBerg/agent-bridge/internal/server"
	"github.com/AndrewPBerg/agent-bridge/internal/state"
	"github.com/AndrewPBerg/agent-bridge/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "agent-bridge:", err)
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
	case "send":
		return send(args[1:])
	case "request":
		return request(args[1:])
	case "version", "--version", "-v":
		fmt.Println("agent-bridge dev")
		return nil
	default:
		return usageError()
	}
}

func usageError() error {
	return errors.New("usage: agent-bridge <serve|stop|ping|sessions|send|request|version>")
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	socket := flags.String("socket", defaultSocket(), "Unix socket path")
	journalPath := flags.String("journal", defaultJournal(), "append-only event journal")
	if err := flags.Parse(args); err != nil {
		return err
	}
	journal, events, err := store.Open(*journalPath)
	if err != nil {
		return err
	}
	defer journal.Close()
	engine, err := state.New(journal, events, state.Options{})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()
	fmt.Fprintf(os.Stderr, "agent-bridge listening on %s (replayed %d events)\n", *socket, len(events))
	return server.New(engine, *socket).Serve(ctx)
}

func sessions(args []string) error {
	flags := flag.NewFlagSet("sessions", flag.ContinueOnError)
	includeStale := flags.Bool("all", false, "include stale sessions")
	if err := flags.Parse(args); err != nil {
		return err
	}
	return callAndPrint("sessions.list", map[string]any{"include_stale": *includeStale})
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
	if value := os.Getenv("XDG_STATE_HOME"); value != "" {
		return filepath.Join(value, "agent-bridge")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "agent-bridge")
	}
	return filepath.Join(home, ".local", "state", "agent-bridge")
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
