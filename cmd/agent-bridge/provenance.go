package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func provenanceCommand(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: agent-bridge provenance <status|snapshot|who-changed|why|agent|since-compaction|mutations|explain|timeline|session>")
	}
	switch args[0] {
	case "snapshot":
		if len(args) != 2 {
			return errors.New("usage: agent-bridge provenance snapshot <absolute-output-path>")
		}
		return callAndPrint("provenance.snapshot", map[string]any{"path": args[1]})
	}
	switch args[0] {
	case "status":
		return callAndPrint("provenance.status", map[string]any{})
	case "who-changed":
		flags := flag.NewFlagSet("provenance who-changed", flag.ContinueOnError)
		limit := flags.Int("limit", 20, "maximum records")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if len(flags.Args()) != 1 {
			return errors.New("usage: agent-bridge provenance who-changed [--limit N] <canonical-path>")
		}
		return callAndPrint("provenance.who_changed", map[string]any{"path": flags.Args()[0], "limit": *limit})
	case "why":
		flags := flag.NewFlagSet("provenance why", flag.ContinueOnError)
		limit := flags.Int("limit", 20, "maximum related records")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if len(flags.Args()) != 1 {
			return errors.New("usage: agent-bridge provenance why [--limit N] <mutation-id>")
		}
		return callAndPrint("provenance.why", map[string]any{"id": flags.Args()[0], "limit": *limit})
	case "agent":
		flags := flag.NewFlagSet("provenance agent", flag.ContinueOnError)
		limit := flags.Int("limit", 20, "maximum records")
		repositoryID := flags.String("repo", "", "repository ID authority scope")
		workspaceID := flags.String("workspace", "", "workspace ID authority scope")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if len(flags.Args()) != 1 {
			return errors.New("usage: agent-bridge provenance agent [--limit N] <actor-or-alias>")
		}
		return callAndPrint("provenance.agent", map[string]any{
			"actor": flags.Args()[0], "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID, "limit": *limit,
		})
	case "since-compaction":
		flags := flag.NewFlagSet("provenance since-compaction", flag.ContinueOnError)
		limit := flags.Int("limit", 50, "maximum records")
		repositoryID := flags.String("repo", "", "repository ID authority scope")
		workspaceID := flags.String("workspace", "", "workspace ID authority scope")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if len(flags.Args()) != 1 {
			return errors.New("usage: agent-bridge provenance since-compaction [--limit N] <actor-or-alias>")
		}
		return callAndPrint("provenance.since_compaction", map[string]any{
			"actor": flags.Args()[0], "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID, "limit": *limit,
		})
	case "mutations":
		flags := flag.NewFlagSet("provenance mutations", flag.ContinueOnError)
		actor := flags.String("actor", "", "canonical actor or @alias")
		path := flags.String("path", "", "exact canonical path")
		repositoryID := flags.String("repo", "", "repository ID filter")
		workspaceID := flags.String("workspace", "", "workspace ID filter")
		limit := flags.Int("limit", 50, "maximum records")
		failed := flags.Bool("failed", false, "only failed mutations")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return callAndPrint("provenance.mutations", map[string]any{
			"actor": *actor, "path": *path, "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID,
			"limit": *limit, "failed": *failed,
		})
	case "explain":
		if len(args) != 2 {
			return errors.New("usage: agent-bridge provenance explain <mutation-id>")
		}
		return callAndPrint("provenance.explain", map[string]any{"id": args[1]})
	case "timeline":
		flags := flag.NewFlagSet("provenance timeline", flag.ContinueOnError)
		actor := flags.String("actor", "", "canonical actor or @alias")
		repositoryID := flags.String("repo", "", "repository ID authority scope")
		workspaceID := flags.String("workspace", "", "workspace ID authority scope")
		eventType := flags.String("type", "", "event type filter")
		limit := flags.Int("limit", 100, "maximum records")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		return callAndPrint("provenance.timeline", map[string]any{
			"actor": *actor, "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID, "type": *eventType, "limit": *limit,
		})
	case "session":
		flags := flag.NewFlagSet("provenance session", flag.ContinueOnError)
		actor := flags.String("actor", "", "canonical actor or @alias")
		repositoryID := flags.String("repo", "", "repository ID authority scope")
		workspaceID := flags.String("workspace", "", "workspace ID authority scope")
		limit := flags.Int("limit", 100, "maximum records")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if *actor == "" {
			return errors.New("provenance session requires --actor")
		}
		return callAndPrint("provenance.session", map[string]any{
			"actor": *actor, "repository_uuid": *repositoryID, "workspace_uuid": *workspaceID, "limit": *limit,
		})
	default:
		return fmt.Errorf("unknown provenance command %q", args[0])
	}
}

func migrateLegacyJournal(target string) error {
	if _, err := os.Stat(target); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	legacy := filepath.Join(home, ".local", "state", "agent-bridge", "events.jsonl")
	source, err := os.Open(legacy)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open legacy journal: %w", err)
	}
	defer source.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	if err := os.Chmod(filepath.Dir(target), 0o700); err != nil {
		return err
	}
	temporary := target + ".migrating"
	destination, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(destination, source); err != nil {
		destination.Close()
		os.Remove(temporary)
		return fmt.Errorf("copy legacy journal: %w", err)
	}
	if err := destination.Sync(); err != nil {
		destination.Close()
		os.Remove(temporary)
		return fmt.Errorf("sync migrated journal: %w", err)
	}
	if err := destination.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, target); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(target))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
