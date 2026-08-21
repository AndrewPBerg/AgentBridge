# Agent Bridge Pi adapter

This is the thin Pi adapter for the Go Agent Bridge daemon in:

```text
/home/andrew/ideas/agent-bridge
```

The adapter owns only Pi-specific behavior:

- session registration and heartbeat
- Herdr plus first-class Git and optional JJ metadata collection
- automatic tool-call intent inference
- mailbox polling and `steer` injection
- acknowledgement after `agent_settled`
- `/bridge` commands
- `bridge_message` and `bridge_collision` tools

The Go daemon owns actors, aliases, ordered durable queues, collision state, selector resolution, and event persistence.

## Install daemon

```bash
cd /home/andrew/ideas/agent-bridge
go install ./cmd/agent-bridge
```

The extension starts `agent-bridge serve` automatically when the socket is unavailable. Existing Pi sessions require `/reload` after adapter changes.

## Commands

```text
/bridge sessions
/bridge name walkie
/bridge send @talkie I will finish schema.ts first.
```

## Environment

```text
AGENT_BRIDGE_BIN        daemon binary, default: agent-bridge
AGENT_BRIDGE_SOCKET     explicit Unix socket path
AGENT_BRIDGE_STATE_DIR  state directory override
```

## Prototype boundary

Git repositories are first-class: the adapter reports repository/worktree roots, git/common directories, branch or detached HEAD, and supports `@git:<HEAD prefix>` selectors. Co-located JJ metadata is layered on top when available.

Attribution is exact for Pi's direct `edit`/`write` tools and conservative for recognized shell operations (`jj restore`, `git restore`, `git checkout --`, `rm`, `mv`, and `cp`). Arbitrary shell scripts and external editors remain best-effort because filesystem events do not carry a reliable agent identity.
