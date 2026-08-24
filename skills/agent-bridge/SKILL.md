---
name: agent-bridge
description: Coordinate with peer coding agents, swarm on bounded work, discover Agent Bridge actors, Directions and WorkUnits, record findings/proposals/decisions, diagnose collisions, or query provenance. Use when peer coordination or attribution is relevant; load optional coordination and provenance tools progressively.
---

# Agent Bridge

Agent Bridge is a peer-to-peer coordination layer, not a manager/worker hierarchy. Use it when another agent can reduce risk or make progress; do not narrate every step or ask permission for routine work.

## Load coordination lazily

When coordination is needed, load `bridge_tools` with the coordination domain once, then use the returned tools. This keeps the normal prompt light and makes the tool surface explicit. Typical uses are actor discovery, messaging, Direction/WorkUnit status, and durable checkpoints. Do not load a domain merely to check whether peers exist.

```json
{"domain":"coordination"}
```

Load provenance separately only when attribution, recovery, or collision history is needed:

```json
{"domain":"provenance"}
```

## Peer coordination and swarming

Trigger coordination for a real dependency, overlapping files, a useful independent review, recovery after compaction, or a bounded parallel investigation. Before messaging, discover live peers from the Agent Bridge binding or `/bus list`; use the displayed **actor UUID/address** directly. Do not normally call `agent-bridge sessions` to translate a Pi session UUID: session IDs are lifecycle/provenance metadata, while actor UUIDs are messaging identities.

Keep requests bounded and actionable: name the objective, relevant paths, expected artifact, and whether a reply is needed. Prefer one direct message to the actor UUID over broadcast. Stop when the dependency is answered. Do not over-coordinate: do not send progress pings, duplicate a question already answered in the mailbox, split trivial work, or ask several peers to do the same investigation.

Keep the communication graph sparse. Default to one useful edge: Agent A asks Agent B because B has relevant context; Agent C does not need the request, reply, or full swarm transcript. Recipients do not forward or rebroadcast unless a concrete dependency requires another peer. Never acknowledge merely to acknowledge, and do not request status twice.

Use lightweight asymmetric provisional lenses when they help convergence (for example, one peer checks correctness while another checks compatibility or tests). A lens is provisional and advisory—not ownership, a veto, or a durable role assignment—and should be stated in the request. Re-evaluate it when evidence changes.

### Message cues

Use short behavioral cues rather than another coordination protocol:

- `ASK:` a bounded response or action is requested; reply only with the answer, result, or blocker.
- `FYI:` absorb the information without replying, forwarding, or checkpointing it unless it materially changes your work.
- `HANDOFF:` accept or decline once; if accepted, state the scope you are taking.
- `Oi! Quiet.` (or `QUIET:`) ends peer chatter. Do not reply—even with an acknowledgement—do not forward it, do not ask for final status, and do not create a checkpoint merely for the cue. Finish the current local task and settle.

An unmarked message does not require acknowledgement unless it contains a direct question, explicit action, or blocker that the recipient can resolve. Cues are behavioral context carried in ordinary messages; they create no daemon state.

## Event-driven waiting

After starting background agents or sending peer requests, do not poll with `sleep`, repeated status commands, or mailbox queries. Finish the turn and remain idle; Pi's mailbox/job completion event will reactivate the parent session. Pure wait-only `sleep` commands are blocked by the extension. Sleeps embedded in a real test or compound workflow remain available.

### Transient messages versus durable state

- `bridge_message` is transient coordination: requests, clarifications, hypotheses, and pointers. It is ordered/delivered, but do not treat it as the project record.
- `bridge_checkpoint` is durable: record a meaningful proposal, finding, decision, handoff, collision resolution, or verification boundary. Include a concise statement and distinguish `asserted`, `verified`, `failed`, and `blocked`; test/build/runtime claims are verified only with captured successful evidence.
- A useful pattern is **proposal → peer finding → decision checkpoint**. Link each to the selected WorkUnit when applicable, and message peers with the checkpoint ID when they need to act.

### Directions and WorkUnits

A **Direction** is the project-level objective and success context. A **WorkUnit** is a repository/workspace-scoped slice of that objective. `/direction <objective>` proposes/creates a Direction; `/direction use <uuid>` selects an existing one. `/work <objective>` proposes a new WorkUnit and joins it; `/work use <uuid>` joins an existing, already-proposed WorkUnit. Joining is explicit—do not assume that mentioning a WorkUnit means participation. Use `/direction status` or `/work status` for compact state, participants, collisions, and recent checkpoints. Keep objectives narrow enough that ownership and collision risk are clear.

A proposed WorkUnit is an offer of bounded work; a joined WorkUnit is an active commitment by the actor. The agent-callable `bridge_work` action `propose` creates and selects a proposal without joining it; `join` or `use` commits participation. The human compatibility shortcut `/work <objective>` still creates, joins, and selects in one step. Tell peers whether you are proposing or joining, and leave/update the WorkUnit when the scope changes.

## Common questions

```text
Who changed a file?        action=who-changed, path=/absolute/path
Why did this happen?       action=why, target=<mutation-id>
What is an agent doing?    action=agent, target=@alias
Since last compaction?     action=since-compaction, target=@alias
Raw mutation filter?       action=mutations, target=@alias or path=...
Session history?           action=session, target=@alias
Projection health?         action=status
```

Prefer deterministic `who-changed`, `why`, `agent`, and `since-compaction` answers over dumping raw timelines. Cite actor, session generation, turn ID, Git/JJ identity, success, and before/after hashes when available.

## Authority scopes

Interpret scope explicitly:

- global: all known actors
- repository: shared Git common directory or JJ repository
- workspace: one physical Git worktree/JJ workspace
- directory: cwd or recent mutation beneath a path

Same physical workspace can collide. Separate worktrees in one repository are related but do not share a physical file.

Read [references/provenance.md](references/provenance.md) only for advanced CLI, schema, or diagnosis workflows.

## Direct actor messaging

Use Agent Bridge **actor UUIDs** as messaging recipients. If a binding or `/bus list` gives you an actor UUID, pass it directly to `bridge_message`:

```json
{"to":"<actor-uuid>","body":"..."}
```

Do not resolve a recipient through `agent-bridge sessions`; aliases remain acceptable when available. `bridge_awaken` can accept a Pi session UUID for recovery, but prefer the actor UUID whenever you have it.
