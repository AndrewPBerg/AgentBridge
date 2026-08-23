# Agent Bridge Pi adapter

This is the thin Pi adapter for the Go Agent Bridge daemon in:

```text
/home/andrew/Desktop/personal/AgentBridge
```

The adapter owns only Pi-specific behavior:

- session registration and heartbeat
- Herdr plus first-class Git and optional JJ metadata collection
- automatic tool-call intent inference
- mailbox polling and `steer` injection
- acknowledgement after `agent_settled`
- `/bridge` commands
- `/checkpoint [kind]` human checkpoint declaration
- `/work` WorkUnit creation and session selection
- `bridge_awaken`, `bridge_message`, `bridge_collision`, and `bridge_checkpoint` tools

The Go daemon owns actors, aliases, ordered durable queues, collision state, selector resolution, and event persistence.

## Install daemon

```bash
cd /home/andrew/Desktop/personal/AgentBridge
go install ./cmd/agent-bridge
```

The extension starts `agent-bridge serve` automatically when the socket is unavailable. Existing Pi sessions require `/reload` after adapter changes.

## Commands

```text
/bus talk                              # modal recipient picker + composer
/bus talk @talkie                      # modal composer, recipient preselected
/bus talk @walkie,@talkie Message      # immediate multi-recipient send
/bus talk --repo Message               # immediate send to active repo peers
/bus list
/bus name walkie
/bus status
/direction <objective>              # create and select
/direction | /direction status       # objective, state, and compact WorkUnit counts
/direction use <uuid> | /direction clear
/direction start|pause|converge|verify|complete|abandon
/work <objective> (includes the selected Direction)
/work use <uuid>
/work status | /work clear
/checkpoint [manual|settled|handoff|test]
/awaken <dead Pi actor> <bounded request>
```

The talk overlay supports multi-select and an `All in this repo` recipient. It shows harness, state, cwd, and Git/JJ identity before sending.

`/bridge` remains as a deprecated compatibility alias (`sessions→list`, `send→talk`).

## Environment

```text
AGENT_BRIDGE_BIN        daemon binary, default: agent-bridge
AGENT_BRIDGE_SOCKET     explicit Unix socket path
AGENT_BRIDGE_STATE_DIR  state directory override (default: ~/.agent-bridge)
AGENT_BRIDGE_PI_BIN     Pi executable for awakening (default: pi)
```

## Prototype boundary

Git repositories are first-class: the adapter reports repository/worktree roots, git/common directories, branch or detached HEAD, and supports direct `<HEAD prefix>` selectors. Co-located JJ metadata is layered on top when available.

Mutation provenance records before/after metadata and SHA-256 hashes (not file contents), plus turn boundaries and compaction summaries, in the daemon's local Turso read model.

Attribution is exact for Pi's direct `edit`/`write` tools and conservative for recognized shell operations (`jj restore`, `git restore`, `git checkout --`, `rm`, `mv`, and `cp`). Arbitrary shell scripts and external editors remain best-effort because filesystem events do not carry a reliable agent identity.

## Awakening a dead Pi session

`bridge_awaken` accepts a dead, same-workspace Pi actor and a bounded task, then forks its saved session into a detached Pi child. The original actor stays dead; the new child registers with a new identity, retains explicit launch provenance, and selects the supplied or inherited WorkUnit. Launch-family policy limits direct `bridge_message` communication to explicit parents and same-parent, same-WorkUnit siblings. The child must re-read the repository and mailbox because its forked transcript may be stale.

A created launch is terminated with an auditable reason when WorkUnit attachment or process spawning fails. After a successful OS spawn, the parent adapter checks for child registration after 30 seconds and terminates an orphaned launch if no child attached. Cross-process reconciliation after the parent adapter exits remains future daemon lifecycle work.
