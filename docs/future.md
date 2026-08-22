# Future directions

This document is a parking lot for promising Agent Bridge directions that are intentionally **not current implementation commitments**. It preserves product and architecture context until there is bandwidth to explore it.

## Product thesis

Agent Bridge can grow from a local coordination daemon into a **local control plane for coding agents**:

> Reconcile desired work with live, independently launched agent sessions while providing identity, messaging, policy, collision handling, and causal provenance.

The memorable analogy is "Kubernetes for agents," but only as an architectural abstraction. Agent Bridge should borrow the control-plane, observed-state, reconciliation, and policy ideas—not Kubernetes' YAML, CRDs, distributed complexity, or operational surface area.

The topology should remain flat. An agent may initiate a group or propose work, but participating agents become addressable peers rather than descendants that can report only to a parent.

## Positioning and prior art

Flat agent communication itself is not novel. Relevant systems already demonstrate pieces of the idea:

- MCP Agent Mail: identities, asynchronous mail, searchable threads, and voluntary file reservations
- Claude Code Agent Teams: direct teammate messaging, shared tasks, mailboxes, and quality hooks
- AutoGen and LangGraph Swarm: conversational agents, dynamic routing, and handoffs
- A2A: discovery, capability cards, asynchronous tasks, messages, and artifacts
- Agent Mesh: local peer discovery and cross-repository questions
- Cursor and GitHub agent products: parallel execution, isolation, steering, and human oversight
- W3C PROV: agents, activities, entities, derivation, attribution, and delegation

The stronger Agent Bridge wedge is the combination of:

- independently launched, heterogeneous sessions
- automatic observation of actual tool intent rather than mandatory manual reservations
- pre-damage collision detection and explicit negotiation/yield/resolution
- push delivery with durable ordering and acknowledgement
- repository, worktree, Git, and JJ-aware identity
- causal provenance across turns, mutations, reloads, and context compaction
- human control without requiring a permanent supervisor agent

In short: build coordination infrastructure, not another role-playing multi-agent framework.

## Control-plane analogy

Useful conceptual mappings:

| Control-plane concept | Agent Bridge concept |
| --- | --- |
| cluster | local development environment |
| namespace | workspace, repository, or swarm |
| pod | agent session or tightly scoped cooperating group |
| job | bounded work item |
| API server | Agent Bridge daemon |
| durable state | event journal plus Turso projections |
| kubelet/runtime adapter | Pi or other harness adapter |
| scheduler | work claiming and peer recruitment |
| controller | reconciliation of desired and observed swarm state |
| service discovery | actor identities, aliases, and selectors |
| service mesh | messaging, policy, identity, and telemetry |
| admission policy | tool preflight and destructive-command protection |
| lease | temporary path or task ownership |
| liveness/readiness | heartbeat and active/waiting/blocked state |
| audit log | causal provenance ledger |
| human administrator | root authority and final override |

The most valuable idea is reconciliation:

```text
desired work + policy
        versus
live agents + mutations + conflicts + results
        yields
spawn, claim, release, review, interrupt, or retire actions
```

The reconciliation machinery should be deterministic where possible. LLMs can propose decisions; the daemon should validate and record state transitions. Agents are not fungible replicas: replacing a session may discard substantial context, so restart and rescheduling policies must account for context value.

## Flat swarm manager

A future swarm manager could organize temporary groups of peer sessions around a mission without imposing a permanent hierarchy.

Possible minimal resources:

```go
type Swarm struct {
    ID          string
    WorkspaceID string
    Mission     string
    Members     []string
    State       string
    CreatedBy   string
}

type WorkItem struct {
    ID           string
    SwarmID      string
    Description  string
    Owner        string
    State        string
    Dependencies []string
    Scope        []string
}
```

Potential behavior:

- agents atomically self-claim unblocked work
- abandoned work becomes available after actor expiry
- any peer can ask another peer for help or review
- collisions cause negotiation rather than silent overwrite
- blocked work can trigger a peer recruitment request
- risky changes can trigger an independent review request
- excess peers can retire after the mission converges
- the human can inspect, redirect, override, or terminate any part of the swarm

Dynamic scaling should have explicit limits and policy. It must not permit recursive, unbounded agent spawning.

## Native control center proof of concept

The first visible experiment should probably be a Pi-native control center rather than a browser dashboard. Running `/bus` with no arguments could open an overlay showing:

```text
Agent Control Center

Peers
  @walkie   active    agent-bridge   jj:tqzzsvxo
  @talkie   waiting   agent-bridge   jj:kpxlomnr

Live work
  @walkie   editing   internal/state/engine.go
  @talkie   reviewing docs/vision.md

Collisions
  jj.ts · @walkie <-> @talkie · negotiating

Enter talk · Space select · H huddle · R refresh · Esc close
```

A deliberately small formation flow could be exposed as either an `H` action or:

```text
/bus form @walkie,@talkie "Investigate the failing provenance tests"
```

For a first proof of concept, formation would only:

1. create a durable huddle/swarm identifier and mission;
2. record selected peer membership;
3. deliver the mission to every peer;
4. show the group in the control center; and
5. preserve direct peer-to-peer messaging.

It would not yet add task scheduling, roles, autoscaling, or a generic resource API.

A useful one-hour implementation slice would be:

1. `workspace.snapshot` returning scoped actors, active intents, open collisions, and huddles;
2. a `/bus` TUI overlay over that snapshot;
3. Enter-to-talk using the existing talk composer;
4. minimal huddle formation and durable fan-out; and
5. a three-Pi-session demonstration.

## Deferred session awakening

Agent Bridge currently provides durable mail but cannot wake an offline harness session to answer a coordination request. Explore an explicit, policy-controlled `agent-bridge awaken` capability:

- an agent or human requests that a known, recently registered session resume and inspect a bounded message or question;
- the harness adapter, not the daemon, decides whether a dormant session can be resumed, restarted, or must be reported unavailable;
- every request, adapter outcome, and resulting session generation is recorded as provenance; and
- the capability must not become arbitrary process control, hidden prompt injection, or autonomous/unbounded agent spawning.

The first experiment should only target one local Pi session with an explicit user-approved policy, a timeout, and a deterministic unavailable result. Cross-machine wake, cost/budget policies, and automatic recruitment remain later work.

## Deferred harness-managed worker identities

Background workers launched by a harness should eventually be registered as ordinary Agent Bridge actors rather than reporting only to their parent process. This enables durable, ordered peer messaging, WorkUnit participation, collision delivery, and checkpoints between sibling workers without making the parent a mandatory relay.

Parent/child launch provenance must be first-class rather than inferred from parent tool calls, timestamps, paths, or later child registration. The authoritative relation should be `parent actor -> launch -> child actor -> optional WorkUnit`, represented with normalized UUID BLOB columns:

```text
actor_launches
  launch_uuid BLOB PRIMARY KEY
  parent_actor_uuid BLOB NOT NULL
  child_actor_uuid BLOB
  work_unit_uuid BLOB
  harness_job_id TEXT
  state TEXT NOT NULL
  requested_at DATETIME NOT NULL
  attached_at DATETIME
  terminated_at DATETIME
```

Possible journal events are `actor.launch_requested`, `actor.launch_attached`, and `actor.launch_terminated`. A stable launch UUID must make retries idempotent and connect a child that registers after the spawn request. The parent relation records causal provenance, not permanent hierarchy or authority; once attached, the child is an ordinary equal participant and directly addressable peer.

The harness must still own lifecycle policy: it grants a worker a canonical session UUID, constrained capabilities, repository/workspace scope, and the explicit launch provenance link. Workers must not gain recursive spawning or unrestricted process control merely by becoming addressable peers. The first experiment should register two local bounded workers, attach each to its launch and WorkUnit, allow direct `bridge_message`, and prove mailbox replay and shutdown behavior.

## Deferred Pi hierarchy context cue

Pi should make the selected coordination hierarchy visible in prompt context without injecting a large project plan on every turn. A compact harness-authored cue could include:

```text
Direction: <uuid> · <objective>
WorkUnit: <uuid> · <objective>
JJ: <working/materialized change identities>
Latest checkpoint: <kind/status/boundary>
Relevant files: <bounded mutation paths>
```

This context must be derived from authoritative selection, WorkUnit/JJ relations, and checkpoint evidence—not inferred from the latest timestamps or model prose. It should refresh when selection, JJ identity, checkpoint boundary, or compaction changes, and remain small enough that detailed rollups are fetched lazily. The cue explains the mental model `Direction -> WorkUnits -> JJ changes -> checkpoints/mutations -> files` while keeping lifecycle state, VCS state, and evidence boundaries distinct.

## Context management

Swarm context should not mean copying every transcript to every peer. That would waste tokens, leak irrelevant information, and create anchoring.

Prefer layered, scoped context:

- **Mission:** stable objective and acceptance criteria
- **Constraints:** explicit policy/spec clauses that reviewers can cite
- **Work item:** local objective, scope, dependencies, and owner
- **Workspace feed:** compact relevant events since the agent's last boundary
- **Artifacts:** paths, hashes, diffs, test results, or provenance references
- **Messages:** targeted peer communication with durable acknowledgement
- **Handoff summary:** bounded context for an agent taking over work
- **Compaction summary:** durable continuity after transcript compression

Large artifacts should be referenced rather than copied through several agents. Provenance should connect a result to the mission, work item, originating agent, tool activity, and source artifacts.

Context delivery should be event- and relevance-driven. A peer editing an unrelated module should not receive every message. Collision participants, reviewers, and dependent work owners should receive the strongest signals.

## Sidecars

Reviewer, tester, security, and spec-guardian "sidecars" are primarily a collaboration pattern:

```text
peer subscribes to or accepts review work
builder sends a focused review request
reviewer inspects referenced artifacts
reviewer returns findings or a semantic interrupt
```

Avoid creating a large sidecar subsystem before this pattern needs additional machinery. A specialized peer plus structured message conventions may be enough.

## CRDT and collaborative-buffer future

Do not build a CRDT inside Agent Bridge. If editors such as Zed expose shared-buffer presence or operation streams, Agent Bridge could consume those as higher-fidelity observations:

```text
collaborative buffer engine -> edit presence/operations -> bridge policy and signals
```

The editor should own synchronized text. Agent Bridge should own actor identity, intent, policy, semantic conflict detection, negotiation, and provenance.

## Suggested sequence when revisited

1. Native `/bus` control center over current workspace state.
2. Durable huddles with missions and peer membership.
3. Shared work items with atomic self-claiming and dependency unblocking.
4. Deterministic reconciliation for expiry, abandoned work, completion, and bounded recruitment requests.
5. Structured review/handoff messages and scoped context packets.
6. Policy-controlled dynamic peer spawning in the Pi adapter.
7. Richer editor/LSP/collaborative-buffer observations when external integrations make them practical.

## Explicit non-goals for early versions

- Kubernetes-compatible APIs, YAML, or CRDs
- cloud control plane or distributed consensus
- permanent supervisor-agent hierarchy
- generic project management
- mandatory manual leases
- shared global transcript
- hard locks for ordinary edits
- agent reputation scores
- autonomous unbounded spawning
- implementing a CRDT or collaborative editor

## Open questions

- Is a swarm explicit, or is every workspace with multiple active peers already an implicit swarm?
- Should huddle membership require acceptance, or is delivery enough for the first version?
- Which decisions belong to deterministic controllers versus contextual LLM judgment?
- What is the smallest useful work-item model that does not become project management?
- How should context value affect replacement, retirement, and reassignment?
- When may recruitment happen automatically, and what budget or human approval is required?
- Which workspace events deserve context injection versus UI-only visibility?
