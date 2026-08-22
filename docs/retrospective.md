# Canonical direction and first-prototype lessons

## Decision

This repository is the canonical Agent Bridge implementation.

The runtime layout is:

| Role | Canonical location |
| --- | --- |
| product source | `/home/andrew/Desktop/personal/AgentBridge` |
| daemon source | `cmd/agent-bridge`, `internal/*` |
| Pi adapter source | `packages/pi-extension` |
| Pi skill source | `skills/agent-bridge` |
| installed Pi adapter | `~/.pi/agent/extensions/agent-bridge` |
| installed Pi skill | `~/.pi/agent/skills/agent-bridge` |
| installed daemon | `~/.local/bin/agent-bridge` |
| runtime state | `~/.agent-bridge` |

The installed Pi files and daemon are deployment artifacts, not alternate source trees. Change this repository first, run its tests, then install from `scripts/install-pi-extension.sh` and `go install ./cmd/agent-bridge`.

The earlier watcher/SSH repository was a historical prototype and has been removed. It is not a second implementation target.

## What the first prototype got right

The first prototype found an important safety loop:

1. record the hash an agent read;
2. observe that the file changed;
3. distinguish a known agent write from an unmatched external write;
4. warn the agent; and
5. stop a stale edit or destructive repository command before it discards unfamiliar work.

That remains valuable. In particular:

- filesystem state is stronger evidence than declared intent;
- coordination must be native to the harness rather than dependent on launch wrappers;
- protected external work should outrank an agent's plan;
- the system should record structured facts and explain why an action was interrupted; and
- ordinary edits should be low-friction while destructive operations receive stricter policy.

The prototype also correctly separated a local daemon from a thin Pi extension and proved the basic Unix-socket shape.

## What the first prototype taught us not to repeat

### Do not mix unrelated capability brokers into the product

The SSH lease broker, root helper, host configuration, and gate binaries expanded the security surface without advancing local agent coordination. Agent Bridge should remain focused on identity, messaging, observed work, collision policy, and provenance.

### Do not infer authority from a filesystem watcher

A recursive `fsnotify` watcher combined with a five-second pending-write window was inherently ambiguous. Save-via-rename, formatters, scripts, slow writes, nested repositories, ignored build trees, and external editors can all produce misleading attribution.

Harness preflight hooks should provide exact attribution where available. Filesystem observation should later be added as a conservative evidence source for otherwise unattributed changes, not treated as proof of who wrote them.

### Do not collapse file states into one hash string

Missing, unreadable, non-regular, symlink, and empty-file states need distinct representations. A blank hash cannot safely stand for all of them. The canonical provenance snapshots therefore preserve file kind and avoid following symlinks.

### Do not guess the harness API

The old extension tests checked source text rather than executing extension behavior. The canonical adapter uses typed Pi APIs and runtime-oriented tests for registration, tool preflight, delivery, settlement, VCS context, and UI behavior.

### Do not make agents manually coordinate every operation

Manual claims and voluntary polling fail under pressure. Adapters should automatically report observable mutation intent and inject durable messages. Agents become involved when judgment is needed: negotiate, yield, resolve, review, or ask the human.

### Do not let prototypes exist outside reviewable history

Almost the entire first implementation remained untracked while only its roadmap was committed. Canonical work must remain reviewable, reproducible, and testable in one repository.

## What the canonical implementation improves

The current system makes the Go daemon the sole coordination authority and keeps harness code as an adapter. It adds:

- canonical `harness:session` identity and scoped aliases;
- Git repository/worktree identity with optional JJ workspace/change identity;
- an fsynced append-only journal with replay and crash-tail recovery;
- durable mailboxes, explicit acknowledgement, retry idempotency, and deterministic ordering;
- automatic exact-path collision detection from tool intent;
- an explicit `open -> negotiating -> yielded -> resolved` collision lifecycle;
- a local Turso causal projection for attribution, turn boundaries, mutation outcomes, and compaction continuity; and
- real Pi tools and UI for communication, collision handling, and progressively disclosed provenance queries.

These are stronger foundations than a watcher-centered protection database because they model peers, causality, delivery, and negotiation directly.

## Lessons still to carry forward

The rewrite does not make every first-prototype goal obsolete. The important unfinished safety work is:

1. **External-change awareness.** Human editors and arbitrary shell processes remain only partially attributable. Add conservative observation without pretending an unmatched event proves a human authored it.
2. **Stale-baseline protection.** Compare mutation preflight state with the agent's observed baseline and warn or block high-confidence stale overwrites.
3. **Destructive-command admission.** Exact-path collisions are advisory today. Restore/reset/clean/delete operations need broader workspace-aware policy and explicit human override.
4. **Richer collision scope.** Exact canonical path equality is a high-confidence start, not semantic conflict detection. Directory, generated-output, symbol, and dependency relationships belong in later evidence layers.
5. **Adapter trust and capabilities.** Owner-only local sockets are necessary but not sufficient for every future harness. Capability claims and authorization policy must remain explicit.
6. **Deployment integrity.** Installed copies and binaries can drift from source. Add a version/doctor workflow before distribution or more adapters.

## Product boundary going forward

Build a local, flat coordination control plane for independently launched coding agents:

- daemon owns identity, queues, collision state, ordering, and causal facts;
- adapters observe harness events, enforce harness policy, and deliver interrupts;
- Git/JJ describe repository and workspace authority;
- agents negotiate only when automatic evidence reveals overlap;
- the human remains root and can inspect or override policy; and
- LLM reviewers, swarm managers, editor integrations, and cross-harness adapters remain clients of these primitives rather than new sources of coordination truth.

Near-term work should deepen the Pi vertical slice and safety guarantees before adding another harness or autonomous orchestration layer.
