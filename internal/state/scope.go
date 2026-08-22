package state

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"

	"github.com/AndrewPBerg/agent-bridge/internal/protocol"
)

func scopeID(_, key string) string {
	return deterministicUUID(filepath.Clean(key))
}

// UnknownActorUUID returns the deterministic synthetic actor for a workspace.
// It is deliberately workspace-scoped and never addressable.
func UnknownActorUUID(workspaceUUID string) string {
	return deterministicUUID("unknown-external\x00" + workspaceUUID)
}

func deterministicUUID(key string) string {
	sum := sha256.Sum256([]byte(key))
	bytes := sum[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50 // UUIDv5-compatible deterministic ID.
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

//nolint:gocritic // Actor is intentionally normalized by value.
func normalizeActorScope(actor protocol.Actor) protocol.Actor {
	repositoryKey := "dir\x00" + actor.CWD
	repositoryRoot := actor.CWD
	workspaceRoot := actor.CWD
	kind := "directory"
	if actor.Git != nil && actor.Git.CommonDir != "" {
		repositoryKey = "git\x00" + actor.Git.CommonDir
		repositoryRoot = actor.Git.RepoRoot
		workspaceRoot = actor.Git.WorktreeRoot
		kind = "git-worktree"
		if actor.JJ != nil {
			kind = "git-jj-workspace"
		}
	} else if actor.JJ != nil && actor.JJ.RepoPath != "" {
		repositoryKey = "jj\x00" + actor.JJ.RepoPath
		repositoryRoot = actor.JJ.WorkspaceRoot
		workspaceRoot = actor.JJ.WorkspaceRoot
		kind = "jj-workspace"
	}
	actor.RepositoryUUID = scopeID("repo", repositoryKey)
	repositoryRoot = filepath.Clean(repositoryRoot)
	workspaceRoot = filepath.Clean(workspaceRoot)
	actor.RepositoryRoot = repositoryRoot
	actor.WorkspaceUUID = scopeID("workspace", actor.RepositoryUUID+"\x00"+workspaceRoot)
	// The repository-root workspace is the default workspace. Omit its
	// duplicate root and kind from the wire model; consumers inherit them.
	if workspaceRoot == repositoryRoot {
		actor.WorkspaceRoot = ""
		actor.WorkspaceKind = ""
	} else {
		actor.WorkspaceRoot = workspaceRoot
		actor.WorkspaceKind = kind
	}
	if repositoryRoot == filepath.Clean(actor.CWD) {
		actor.RepositoryRoot = ""
	}
	return actor
}

//nolint:gocritic // Intent is intentionally normalized by value.
func normalizeIntentScope(intent protocol.Intent) protocol.Intent {
	actor := normalizeActorScope(protocol.Actor{CWD: intent.CWD, Git: intent.Git, JJ: intent.JJ})
	intent.RepositoryUUID = actor.RepositoryUUID
	intent.RepositoryRoot = actor.RepositoryRoot
	intent.WorkspaceUUID = actor.WorkspaceUUID
	workspaceRoot := actor.RepositoryRoot
	if workspaceRoot == "" {
		workspaceRoot = filepath.Clean(intent.CWD)
	}
	if actor.WorkspaceRoot != "" {
		workspaceRoot = actor.WorkspaceRoot
	}
	intent.WorkspaceRoot = actor.WorkspaceRoot
	intent.WorkspaceKind = actor.WorkspaceKind
	intent.WorkspaceKey = workspaceRoot
	intent.RelativePaths = make([]string, 0, len(intent.Paths))
	for _, path := range intent.Paths {
		relative, err := filepath.Rel(workspaceRoot, path)
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
