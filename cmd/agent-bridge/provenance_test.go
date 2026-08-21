package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMigrateLegacyJournal(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("AGENT_BRIDGE_STATE_DIR", "")
	legacy := filepath.Join(home, ".local", "state", "agent-bridge", "events.jsonl")
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatal(err)
	}
	content := []byte("{\"version\":1}\n")
	if err := os.WriteFile(legacy, content, 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(home, ".agent-bridge", "events.jsonl")
	if err := migrateLegacyJournal(target); err != nil {
		t.Fatal(err)
	}
	migrated, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(migrated) != string(content) {
		t.Fatalf("migrated content = %q", migrated)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}
