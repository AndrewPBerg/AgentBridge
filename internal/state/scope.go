package state

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func scopeID(prefix, key string) string {
	sum := sha256.Sum256([]byte(filepath.Clean(key)))
	return prefix + ":" + hex.EncodeToString(sum[:16])
}

func normalizeActorScope(actor protocol.Actor) protocol.Actor {
	repositoryKey := "dir:" + actor.CWD
	repositoryRoot := actor.CWD
	workspaceRoot := actor.CWD
	kind := "directory"
	if actor.Git != nil && actor.Git.CommonDir != "" {
		repositoryKey = "git:" + actor.Git.CommonDir
		repositoryRoot = actor.Git.RepoRoot
		workspaceRoot = actor.Git.WorktreeRoot
		kind = "git-worktree"
		if actor.JJ != nil {
			kind = "git-jj-workspace"
		}
	} else if actor.JJ != nil && actor.JJ.RepoPath != "" {
		repositoryKey = "jj:" + actor.JJ.RepoPath
		repositoryRoot = actor.JJ.WorkspaceRoot
		workspaceRoot = actor.JJ.WorkspaceRoot
		kind = "jj-workspace"
	}
	actor.RepositoryID = scopeID("repo", repositoryKey)
	actor.RepositoryRoot = filepath.Clean(repositoryRoot)
	actor.WorkspaceID = scopeID("workspace", actor.RepositoryID+"\x00"+workspaceRoot)
	actor.WorkspaceRoot = filepath.Clean(workspaceRoot)
	actor.WorkspaceKind = kind
	return actor
}

func normalizeIntentScope(intent protocol.Intent) protocol.Intent {
	actor := normalizeActorScope(protocol.Actor{CWD: intent.CWD, Git: intent.Git, JJ: intent.JJ})
	intent.RepositoryID = actor.RepositoryID
	intent.RepositoryRoot = actor.RepositoryRoot
	intent.WorkspaceID = actor.WorkspaceID
	intent.WorkspaceRoot = actor.WorkspaceRoot
	intent.WorkspaceKind = actor.WorkspaceKind
	intent.WorkspaceKey = actor.WorkspaceRoot
	intent.RelativePaths = make([]string, 0, len(intent.Paths))
	for _, path := range intent.Paths {
		relative, err := filepath.Rel(actor.WorkspaceRoot, path)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			intent.RelativePaths = append(intent.RelativePaths, filepath.Clean(path))
			continue
		}
		intent.RelativePaths = append(intent.RelativePaths, filepath.ToSlash(relative))
	}
	return intent
}

func underDirectory(path, directory string) bool {
	relative, err := filepath.Rel(filepath.Clean(directory), filepath.Clean(path))
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
