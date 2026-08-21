# Provenance reference

## Pi tool

Load once per session:

```json
{"domain":"provenance"}
```

Then call `bridge_provenance`:

```json
{"action":"who-changed","path":"/repo/src/schema.ts","limit":10}
{"action":"why","target":"<mutation-id>","limit":10}
{"action":"agent","target":"@walkie","limit":10}
{"action":"since-compaction","target":"@walkie","limit":20}
{"action":"mutations","target":"@walkie","failed":true,"limit":10}
{"action":"timeline","target":"@walkie","eventType":"collision.upserted","limit":10}
{"action":"session","target":"@walkie","limit":20}
{"action":"status"}
```

## CLI equivalents

```bash
agent-bridge scopes
agent-bridge sessions --repo <repository-id>
agent-bridge sessions --workspace <workspace-id>
agent-bridge sessions --under /repo/src
agent-bridge provenance who-changed /repo/src/schema.ts
agent-bridge provenance why <mutation-id>
agent-bridge provenance agent --workspace <workspace-id> @walkie
agent-bridge provenance since-compaction --workspace <workspace-id> @walkie
agent-bridge provenance mutations --workspace <workspace-id>
agent-bridge provenance timeline --actor @walkie
agent-bridge provenance session --actor @walkie
agent-bridge provenance status
```

Aliases may repeat across workspaces. Supply `workspaceId`/`repositoryId` to the Pi tool or `--workspace`/`--repo` to the CLI; ambiguous unscoped aliases fail rather than guessing.

## Mutation evidence

A current mutation can include:

```text
actor and session generation
turn ID and turn index
tool call and operation
repository/workspace IDs
absolute and repository-relative paths
Git repository/worktree/branch/HEAD
JJ repository/workspace/change/commit
success or failure
before/after SHA-256, size, and mtime
assistant explanation excerpt
```

No file contents or full patches are stored. Symlinks are not followed or hashed.

## Reading collisions

A collision lifecycle is:

```text
open → negotiating → yielded → resolved
```

Use `why` for the mutation and `who-changed` for the path. Confirm which workspace each actor occupied. Two identical repository-relative paths in separate worktrees are related work, not a physical-file collision.

## Post-compaction diagnosis

Use `since-compaction` first. It returns the latest compaction summary plus subsequent mutations and session events. If the needed event predates the compaction, use `session` or `timeline` with a small limit and widen only as necessary.
