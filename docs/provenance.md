# Provenance ledger

Agent Bridge stores a local causal index at:

```text
~/.agent-bridge/agent-bridge.db
```

The database uses the Turso Database Go engine (`tursogo`), not the legacy SQLite driver. This provides local-first operation, MVCC concurrent writes, asynchronous I/O, `database/sql` compatibility, and a path toward explicit push/pull sync. Agent Bridge also projects journal events through a buffered background worker, so provenance indexing does not add database latency to the coordination path.

The append-only `events.jsonl` journal remains coordination truth. Turso is an idempotent read model that is rebuilt/backfilled from the journal on daemon startup. Projection uses bounded buffering with backpressure and retries each sequence before advancing, so live queries cannot silently skip a journal event. Query RPCs wait for the journal tail captured at query time; `provenance status` exposes journal sequence, projected sequence, queue depth, lag, and the latest projection error. A projection failure never compromises mailbox durability.

## Current records

- actor registrations, aliases, harness/session identity, capabilities, Git, and JJ context
- mutation attempts and completion outcomes
- canonical paths and before/after metadata
- SHA-256 hashes for ordinary files up to 16 MiB by default
- Git and JJ context before and after mutations
- collision lifecycle
- session start/shutdown
- turn boundaries
- compaction summaries and token metadata

File contents and full patches are not stored. Symlinks are recorded as symlinks and are never followed or hashed, preventing repository links from exposing metadata about targets outside the workspace. Configure the regular-file hash ceiling with:

```text
AGENT_BRIDGE_HASH_MAX_BYTES
```

## CLI

```bash
agent-bridge provenance status
agent-bridge provenance mutations --limit 20
agent-bridge provenance mutations --actor @walkie
agent-bridge provenance mutations --path /repo/src/schema.ts
agent-bridge provenance mutations --failed
agent-bridge provenance explain <mutation-id>
agent-bridge provenance timeline --actor @walkie
agent-bridge provenance session --actor @walkie
```

## Turso sync

Cloud sync is intentionally not automatic yet. Provenance contains sensitive local metadata and session summaries, and multi-machine sync requires globally namespaced event identities plus explicit conflict and retention policy.

The chosen engine supports `NewTursoSyncDb`, local writes, `Push`, `Pull`, checkpoints, stats, and encryption. We should expose those only through Agent Bridge-specific configuration and an explicit opt-in policy; generic `TURSO_*` environment variables must never silently upload provenance.

References:

- https://docs.turso.tech/sdk/go/quickstart
- https://docs.turso.tech/sdk/go/reference
