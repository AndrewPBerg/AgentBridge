# VCS identity

Agent Bridge treats Git as the baseline repository model and JJ as an optional, richer change layer.

## Git identity

Every adapter should report, when available:

```json
{
  "git": {
    "repo_root": "/repo",
    "worktree_root": "/repo/worktrees/feature",
    "git_dir": "/repo/.git/worktrees/feature",
    "common_dir": "/repo/.git",
    "branch": "feature/bridge",
    "head": "abcdef...",
    "detached": false
  }
}
```

Meanings:

- `repo_root`: primary repository root inferred from the common Git directory
- `worktree_root`: physical working copy used for path ownership and collision scope
- `git_dir`: worktree-specific Git metadata directory
- `common_dir`: shared metadata directory across linked worktrees
- `branch` / `head`: current branch and commit identity

Selectors can address active actors by a direct Git HEAD prefix. A selector must resolve to exactly one active actor.

## JJ identity

When `.jj` is present, adapters additionally report:

```json
{
  "jj": {
    "workspace_root": "/repo/worktrees/feature",
    "change_id": "qpvuntsm..."
  }
}
```

JJ does not replace Git identity. In a co-located repository, Agent Bridge exposes both:

```text
Git repository/worktree → baseline workspace identity
JJ workspace/change     → change ownership and dependency identity
```

Selectors can address active actors by a direct JJ change ID prefix.

## Authority scopes

Agent Bridge normalizes every actor and mutation into:

```text
repository_id    shared Git common directory or JJ repository
workspace_id     one physical Git worktree/JJ workspace
workspace_root   physical working copy root
relative_paths   paths relative to that workspace
cwd              launch/current directory
```

IDs are stable hashes of canonical local identity paths; raw paths remain owner-local metadata.

Routing and alias authority narrow in this order:

```text
same workspace → same repository → global
```

The same alias can exist in separate workspaces. A sender resolves its workspace-local peer first, then repository-local, and only then a globally unique actor. Canonical session UUID addresses always bypass scope ambiguity.

Discovery supports:

```bash
agent-bridge scopes
agent-bridge sessions --repo <repository-id>
agent-bridge sessions --workspace <workspace-id>
agent-bridge sessions --under /absolute/directory
```

Directory scope includes actors launched beneath that directory or with recent mutation intents beneath it.

## Collision scope

Canonical absolute path equality is the highest-confidence collision signal. The Pi adapter resolves VCS identity from the mutation target path, not only the actor launch directory. It uses Git worktree root as its first workspace key, then JJ workspace root, then cwd. This supports:

- ordinary Git repositories
- linked Git worktrees
- native JJ repositories
- co-located Git/JJ repositories
- unversioned disposable directories

Agent Bridge does not run mutating Git or JJ operations automatically.
