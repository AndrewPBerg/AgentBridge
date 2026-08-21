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

Selectors can address active actors by `@git:<HEAD prefix>`. A selector must resolve to exactly one active actor.

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

Selectors can address active actors by `@change:<JJ change ID prefix>`.

## Collision scope

Canonical absolute path equality is the highest-confidence collision signal. The Pi adapter uses Git worktree root as its first workspace key, then JJ workspace root, then cwd. This supports:

- ordinary Git repositories
- linked Git worktrees
- native JJ repositories
- co-located Git/JJ repositories
- unversioned disposable directories

Agent Bridge does not run mutating Git or JJ operations automatically.
