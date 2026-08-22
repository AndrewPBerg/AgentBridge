package state

import (
	"testing"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func TestGitWorktreesShareRepositoryButNotWorkspace(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	registerGit := func(address, root, branch string) protocol.Actor {
		actor, err := engine.Register(protocol.Actor{
			Address: address, Harness: "pi", SessionUUID: address[3:], CWD: root, State: "active",
			Git: &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: root, CommonDir: "/repo/.git", Branch: branch},
		})
		if err != nil {
			t.Fatal(err)
		}
		return actor
	}
	main := registerGit("main", "/repo", "main")
	feature := registerGit("feature", "/repo-feature", "feature")
	if main.RepositoryUUID != feature.RepositoryUUID {
		t.Fatalf("same Git common dir produced different repositories: %s != %s", main.RepositoryUUID, feature.RepositoryUUID)
	}
	if main.WorkspaceUUID == feature.WorkspaceUUID {
		t.Fatalf("different worktrees produced the same workspace: %s", main.WorkspaceUUID)
	}
}

func TestAliasResolutionPrefersSenderWorkspaceThenRepository(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	actor := func(address, root string) protocol.Actor {
		value, err := engine.Register(protocol.Actor{
			Address: address, Harness: "pi", SessionUUID: address[3:], CWD: root, State: "active",
			Git: &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: root, CommonDir: "/repo/.git"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	sender := actor("sender", "/repo")
	local := actor("local", "/repo")
	remote := actor("remote", "/repo-feature")
	if _, err := engine.SetAlias(local.Address, "builder"); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SetAlias(remote.Address, "builder"); err != nil {
		t.Fatalf("same alias in another workspace should be allowed: %v", err)
	}
	message, err := engine.Send(protocol.SendParams{From: sender.Address, To: "@builder", Body: "local authority"})
	if err != nil {
		t.Fatal(err)
	}
	if message.To != local.Address {
		t.Fatalf("workspace-local alias resolved to %s", message.To)
	}
}

func TestIntentGetsRepositoryRelativePathsAndDirectoryScope(t *testing.T) {
	engine, _, _ := newTestEngine(t)
	actor, err := engine.Register(protocol.Actor{
		Address: "worker", Harness: "pi", SessionUUID: "worker", CWD: "/repo", State: "active",
		Git: &protocol.GitContext{RepoRoot: "/repo", WorktreeRoot: "/repo", CommonDir: "/repo/.git"},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := protocol.Intent{
		ID: "intent", Actor: actor.Address, ToolCallID: "tool", Tool: "edit", Operation: "edit",
		Paths: []string{"/repo/src/schema.ts"}, CWD: "/repo", Git: actor.Git,
	}
	if _, err := engine.BeginIntent(intent); err != nil {
		t.Fatal(err)
	}
	stored := engine.intents[intent.ID]
	if stored.RepositoryUUID != actor.RepositoryUUID || stored.WorkspaceUUID != actor.WorkspaceUUID {
		t.Fatalf("intent scope mismatch: %#v vs %#v", stored, actor)
	}
	if len(stored.RelativePaths) != 1 || stored.RelativePaths[0] != "src/schema.ts" {
		t.Fatalf("relative paths = %#v", stored.RelativePaths)
	}
	actors := engine.SessionsScoped(false, protocol.ScopeFilter{Directory: "/repo/src"})
	if len(actors) != 1 || actors[0].Address != actor.Address {
		t.Fatalf("directory scope actors = %#v", actors)
	}
}
