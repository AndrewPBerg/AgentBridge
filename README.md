# Agent Bridge

Agent Bridge is a local coordination daemon for independently launched coding-agent sessions. It provides canonical identities, ordered durable mailboxes, automatic file-collision events, collision lifecycles, and semantic interrupts without requiring a parent/subagent relationship.

The Go daemon owns coordination truth. Harness integrations remain thin adapters.

This repository is the canonical product source; installed daemon and Pi files are deployment artifacts. The earlier `AgentBridge` watcher/SSH prototype is historical, not a second implementation target. See [canonical direction and first-prototype lessons](docs/retrospective.md).

Design references: [roadmap](docs/roadmap.md), [vision](docs/vision.md), [VCS identity](docs/vcs.md), [provenance](docs/provenance.md), [quality gates](docs/quality.md), and [harness compatibility](docs/harnesses.md).

## Current vertical slice

- Unix-domain-socket JSON RPC with owner-only permissions
- append-only, fsynced event journal with crash-tail recovery
- local Turso provenance database with MVCC, async I/O, mutation hashes, turn boundaries, and compaction summaries
- actor registration, aliases, heartbeat leases, capabilities, and session generations
- first-class Git repository/worktree/branch/HEAD identity, with optional JJ repository/workspace/change identity layered on top
- normalized global, repository, workspace, and directory authority scopes
- canonical `harness:session` addressing plus `@alias`, `@git:<HEAD>`, and `@change:<JJ ID>` selectors
- durable mailbox polling and explicit acknowledgement
- global, sender, recipient, and adapter-assigned sequence metadata
- client-assigned idempotency keys for retry-safe sends
- exact-path collision detection from automatically observed tool intents
- collision lifecycle: `open -> negotiating -> yielded -> resolved`
- canonical delivery to known sessions while temporarily stale/reloading
- concurrent request handling with serialized state transitions

## Build and run

```bash
go build -o agent-bridge ./cmd/agent-bridge
./agent-bridge serve
```

Defaults:

```text
socket:     ~/.agent-bridge/bridge.sock
journal:    ~/.agent-bridge/events.jsonl
provenance: ~/.agent-bridge/agent-bridge.db
```

Override with `AGENT_BRIDGE_STATE_DIR` or `AGENT_BRIDGE_SOCKET`.

## CLI

All commands support standard Cobra help and machine-readable output:

```bash
agent-bridge --help
agent-bridge doctor --help
agent-bridge --json doctor
agent-bridge doctor --json
```

JSON failures use a non-zero exit status and an `{\"ok\":false,\"error\":...}` envelope.

```bash
agent-bridge ping
agent-bridge stop
agent-bridge sessions --all
agent-bridge scopes
agent-bridge sessions --workspace <workspace-id>
agent-bridge send --from pi:01K... --id pi:01K:1:1 --sequence 1 @walkie "Can you yield schema.ts?"
agent-bridge request mailbox.poll '{"actor":"pi:01K..."}'
agent-bridge provenance who-changed /repo/file.ts
agent-bridge provenance why <mutation-id>
agent-bridge provenance agent @walkie
agent-bridge provenance since-compaction @walkie
```

The low-level `request` command is primarily for development and harness adapters.

## Protocol

Requests and responses are newline-delimited JSON over the Unix socket:

```json
{"id":"1","method":"message.send","params":{"id":"pi:abc:2:4","from":"pi:abc","to":"@walkie","body":"hello","client_sequence":4,"session_generation":2}}
```

```json
{"id":"1","result":{"id":"msg:...","global_sequence":42,"sender_sequence":7,"recipient_sequence":9}}
```

Implemented methods:

```text
ping
daemon.shutdown
actor.register
actor.heartbeat
actor.alias
sessions.list
message.send
mailbox.poll
mailbox.ack
intent.begin
intent.end
collision.transition
session.event
```

## Ordering and durability

The daemon serializes state transitions and journals an event before applying it. Mail remains pending until the recipient explicitly acknowledges it. A harness adapter should acknowledge only after the injected message has been processed and its agent has settled.

Adapters assign `client_sequence` before asynchronous work begins. Mailbox polling performs a deterministic k-way merge that preserves each sender's generation and client sequence even when other senders interleave. This fixes the Pi prototype's observed `K4 K5 K1 K2 K3` burst reordering.

## Collision behavior

Adapters automatically report mutating tool calls with `intent.begin`; agents do not manually claim files. A matching canonical path from another recent actor creates one open collision and two durable collision messages. Direct communication between the participants moves it to `negotiating`. An adapter or agent can then transition it to `yielded` or `resolved`.

The daemon reports collision state; harness policy decides whether a specific operation is warning-only or blocked. Destructive restore/reset operations should eventually use a stricter policy than ordinary edits.

## Repository layout

```text
cmd/agent-bridge/       CLI and foreground daemon entrypoint
internal/client/        Go socket client
internal/protocol/      harness-neutral wire types
internal/provenance/    Turso projection and causal queries
internal/server/        concurrent Unix socket server
internal/state/         event-sourced coordination state machine
internal/store/         durable JSONL journal
packages/pi-extension/  canonical Pi adapter source
skills/agent-bridge/     progressively disclosed provenance workflow
scripts/                local adapter installation
```

Install the Pi adapter with:

```bash
./scripts/install-pi-extension.sh
```

Pi exposes the concise client command:

```text
/bus talk                         # multi-select modal composer
/bus talk @walkie message
/bus talk @walkie,@talkie message
/bus talk --repo message
/bus list
/bus name walkie
/bus status
```

`/bridge` remains a deprecated compatibility alias.

## Harness adapters

The Pi adapter is operational and remains the active product focus. Cross-harness adapters, including Codex, are intentionally deferred while provenance, Turso sync, and Pi workflows mature. The daemon remains the only owner of queues, collision state, and durable identity.
