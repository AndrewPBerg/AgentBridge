//go:build !race

package watchman

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

//nolint:cyclop // opt-in integration test covers full external process lifecycle.
func TestManagerObservesRealWatchmanWrite(t *testing.T) {
	if os.Getenv("AGENT_BRIDGE_WATCHMAN_INTEGRATION") != "1" {
		t.Skip("set AGENT_BRIDGE_WATCHMAN_INTEGRATION=1 for the real Watchman test")
	}
	if _, err := exec.LookPath("watchman"); err != nil {
		t.Skip("watchman is not installed")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(home, ".agent-bridge-watchman-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	actor := testActor(root)
	coordination := &fakeCoordination{sessions: []protocol.Actor{actor}, restoredCh: make(chan struct{}, 1), changeCh: make(chan protocol.ExternalChange, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher := &workspaceWatcher{binary: "watchman", actor: actor, processor: newProcessor(coordination, &actor)}
	errCh := make(chan error, 1)
	go func() { errCh <- watcher.runOnce(ctx, true) }()
	select {
	case <-coordination.restoredCh:
	case err := <-errCh:
		t.Fatalf("watchman baseline failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("watchman baseline did not become ready")
	}
	path := filepath.Join(root, "manual.txt")
	if err := os.WriteFile(path, []byte("manual editor change"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case change := <-coordination.changeCh:
		if change.Path != path || change.ChangeKind != "created" {
			t.Fatalf("change = %#v", change)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("watchman change was not observed")
	}
	cancel()
	if err := exec.CommandContext(context.Background(), "watchman", "watch-del", root).Run(); err != nil {
		t.Logf("watchman cleanup: %v", err)
	}
}
