# Roadmap

## Product direction

Agent Bridge is a local coordination control plane for independently launched coding agents. Near-term work should strengthen the Pi vertical slice, preserve deterministic provenance as truth, and add semantic understanding only when concurrent work makes its cost useful.

Checkpoints, WorkUnits, the local Direction core, generic ticket context, explicit launch provenance, and the initial Watchman external-change substrate are implemented. The next product lane is deterministic workflow safety—mutation admission, observed-baseline protection, and controlled escalation to WTF workspaces—while evidence-backed checkpoints and provenance remain the source of truth. Semantic extraction stays optional enrichment rather than coordination authority.

## Landed foundation

These roadmap foundations are implemented in the current daemon and Pi adapter:

- [x] Local Go daemon with Unix-socket JSON RPC as coordination authority (`internal/server`, `internal/state`, `internal/protocol`).
- [x] Append-only, fsynced journal with replay and crash-tail recovery (`internal/store`).
- [x] Durable ordered mailboxes with explicit acknowledgement and idempotent sends.
- [x] Actor registration, heartbeat leases, canonical session-UUID identities, aliases, capabilities, and Git/JJ context.
- [x] Repository/workspace/directory authority scopes.
- [x] Automatic Pi tool-intent reporting and exact-path collision detection.
- [x] Collision lifecycle: `open -> negotiating -> yielded -> resolved`.
- [x] Turso provenance projection with mutation snapshots, session events, turn/compaction context, and deterministic queries.
- [x] Pi provenance tools, mailbox delivery, collision UI, and reconnect/re-register behavior.
- [x] Immutable evidence-backed checkpoint claims with asserted/verified/failed/blocked status.
- [x] Durable multi-participant WorkUnits with lifecycle, selection, replay, and ordered checkpoint history.
- [x] Deployment integrity checks through `agent-bridge doctor` and `agent-bridge version`.
- [x] Strict local/CI quality workflow with mise, Taskfile, gofumpt, golangci-lint, race tests, shuffled tests, and govulncheck.

## Phase 0 — Cleanup and canonicalization

This is the immediate prerequisite and may be performed independently from semantic-index work.

Goals:

- [x] Keep `/home/andrew/Desktop/personal/AgentBridge` as the only product source.
- [x] Remove stale terminology, compatibility residue, and undocumented prototype assumptions.
- [x] Make source, installed adapter, installed skill, binary, and runtime versions inspectable.
- [x] Add a deployment/version or `doctor` workflow that detects drift.
- [x] Keep the daemon, Pi adapter, and skill tests green under the strict quality gate.
- [x] Preserve the product boundary documented in [the retrospective](retrospective.md).

Exit condition: a contributor can identify, test, install, and verify the canonical daemon and Pi adapter without guessing which copy is authoritative.

## Phase 1 — Agent-authored checkpoints ✅

**Status: complete.** Checkpoints are immutable, replayable, evidence-linked boundaries with structured claims and normalized provenance.

Build checkpoints first as a deterministic, standalone provenance substrate. A checkpoint may be declared by an agent or human and does not require a WorkUnit at creation. Conceptually, a WorkUnit owns or links to its child checkpoints; WorkUnits are simply a later composition layer that adds mutable objectives, lifecycle state, and top-level ordering. The coordination journal remains authoritative; checkpoint records are immutable evidence boundaries.

### Checkpoint declaration

An agent can explicitly declare an internal checkpoint at a meaningful stopping point, and a human can declare one through the same API. The declaration source should be recorded as `agent` or `human`; automatic system boundaries can be added later without changing the model.

### Checkpoint identity and evidence

Every immutable checkpoint should be tied to deterministic evidence:

- [x] repository and workspace UUIDs;
- [x] actor address and session generation;
- [x] journal start/end sequence;
- [x] Pi turn or compaction boundary when available;
- [x] Git HEAD and optional JJ change/commit identity;
- [x] mutation, message, collision, and test-result references; and
- [x] optional `work_unit_uuid` linkage; and
- [x] agent- or human-authored metadata at that boundary.

The declarer may explicitly say, “this is a good stopping point; checkpoint this.” Automatic boundaries such as settlement, compaction, handoff, and shutdown can be added later, but must use the same immutable model.

### Evidence-backed claims

A checkpoint boundary and a claim made at that boundary are distinct. Authored metadata such as “tests pass” is a claim, not evidence. Agent Bridge should preserve structured claims and require them to cite deterministic checkpoint evidence before presenting them as verified.

The implemented normalized projection maps each claim to the evidence rows that support it:

```text
checkpoint_claims
  checkpoint_id TEXT NOT NULL REFERENCES checkpoint_requests(id)
  ordinal INTEGER NOT NULL
  claim_kind TEXT NOT NULL
  statement TEXT NOT NULL
  status TEXT NOT NULL
  PRIMARY KEY (checkpoint_id, ordinal)

checkpoint_claim_evidence
  checkpoint_id TEXT NOT NULL
  claim_ordinal INTEGER NOT NULL
  evidence_kind TEXT NOT NULL
  evidence_ordinal INTEGER NOT NULL
  PRIMARY KEY (checkpoint_id, claim_ordinal, evidence_kind, evidence_ordinal)
```

`checkpoint_claim_evidence` should reference the corresponding normalized `checkpoint_evidence` row rather than copy its identifier into JSON. Initial claim kinds may include `implementation`, `test`, `build`, `runtime`, `review`, `decision`, and `blocked`. Initial statuses may include `asserted`, `verified`, `failed`, and `blocked`.

Defensive evidence rules:

- a test, build, or runtime claim cannot be `verified` without a captured execution or test-result reference and successful outcome;
- a review claim cites the reviewer result and reviewed boundary;
- an implementation claim cites mutation evidence plus an observed repository/JJ boundary;
- a decision or note may cite its journal range and relevant messages; and
- missing evidence downgrades a claim to `asserted` rather than rejecting the immutable checkpoint or silently treating prose as fact.

The daemon or adapter should derive relevant evidence from the authoritative actor/scope/journal range whenever possible. Prompting should require agents to distinguish what they changed, what they directly verified, what remains asserted, and what failed. Model-authored summaries never upgrade their own verification status.

### Exact duplicate policy

Do not add SimHash, embedding similarity, or model-based semantic deduplication to checkpoint identity. Exact retries use a stable caller declaration or operation ID and must be idempotent; conflicting reuse is rejected. Distinct checkpoint IDs remain distinct immutable events even when their prose is similar.

Better boundary guidance should prevent accidental duplicates at the source: declare one checkpoint per meaningful stopping point, reuse the same declaration ID when retrying a lost response, and do not emit another checkpoint merely to restate an unchanged claim. UI may surface exact structured duplicates as a warning, but must not merge or discard them automatically.

### WorkUnits and ordering come later

A future WorkUnit will compose checkpoints with an objective, optional policy/context/acceptance/scope, equal participants, mutable lifecycle state, and handoff metadata. The top-level workflow will order WorkUnits; checkpoints do not define workflow order. Do not build a separate semantic-processing queue yet. Any durable queue for extraction or execution can later be derived from workflow ordering rather than becoming an independent source of truth.

### Derived enrichment

Semantic claims, model summaries, and extraction runs are optional derived data. They may be regenerated, versioned, or discarded without changing the checkpoint or observed provenance. No model availability or semantic processor may sit on the coordination path.

Exit condition: a fixture session can create and replay immutable agent- and human-declared checkpoints, preserve evidence links, and query the latest checkpoint deterministically. Evidence references and authored metadata are projected into relational tables; UUID references use 16-byte BLOB storage.

### Immediate implementation slice

- [x] add the minimal checkpoint protocol and declaration path;
- [x] capture agent/human declaration source and immutable evidence identity;
- [x] project deterministic evidence references and latest-checkpoint queries; and
- [x] add replay, idempotency, and immutability tests.

WorkUnit composition and lifecycle transitions are complete in Phase 2. Atomic mutation admission, path leases, and observed-baseline protection remain later safety work.

## Phase 2 — WorkUnit orchestration ✅

**Status: complete.** Multiple equal participants can share a durable goal, contribute ordered evidence-backed checkpoints, replay lifecycle state, and coordinate through durable mailbox/collision flows. Discovery, notification, and richer lifecycle UX are later enhancements rather than completion blockers.

A WorkUnit is the mutable, durable representation of a goal. It is the change-safe equivalent of a TODO: it carries the objective, acceptance criteria, lifecycle, participants, and links to immutable progress evidence. A checkpoint is not a TODO or mutable task record; it is an immutable evolution-log boundary describing what was true at a meaningful point while pursuing the WorkUnit.

### Relational model

The initial projection should use normalized relationships rather than storing checkpoint or actor lists in JSON:

```text
work_units
  work_unit_uuid BLOB PRIMARY KEY
  repository_uuid BLOB NOT NULL
  workspace_uuid BLOB NOT NULL
  objective TEXT NOT NULL
  acceptance_criteria TEXT
  context TEXT
  state TEXT NOT NULL
  created_by BLOB NOT NULL
  created_at DATETIME NOT NULL
  updated_at DATETIME NOT NULL
  completed_at DATETIME

work_unit_actors
  work_unit_uuid BLOB NOT NULL REFERENCES work_units(work_unit_uuid)
  actor_uuid BLOB NOT NULL
  joined_at DATETIME NOT NULL
  left_at DATETIME
  participation_state TEXT NOT NULL
  PRIMARY KEY (work_unit_uuid, actor_uuid)

checkpoint_requests
  ...
  work_unit_uuid BLOB REFERENCES work_units(work_unit_uuid)
```

All UUIDs remain canonical strings at protocol boundaries and exactly 16-byte `BLOB` values in SQLite/Turso. WorkUnit participants are equal collaborators in the first version; there is no distinguished owner. `created_by` records provenance, not continuing authority.

A WorkUnit does not contain a serialized checkpoint list. A checkpoint optionally names exactly one WorkUnit when declared, and its immutable association cannot be reassigned. Standalone checkpoints remain valid provenance but are not retroactively adopted by a WorkUnit in the first version. The ordered checkpoint history is queried by `work_unit_uuid` and journal sequence.

### Lifecycle state and journal events

Lifecycle state describes the current coordination condition:

```text
proposed -> active <-> blocked -> verified -> completed
      \-------------------------------------> abandoned
```

`created` and `updated` are journal events, not lifecycle states. The authoritative event vocabulary should initially include:

```text
work_unit.created
work_unit.updated
work_unit.actor_joined
work_unit.actor_left
work_unit.transitioned
checkpoint.requested
```

Every mutation records its actor, time, previous value or state where applicable, and resulting value or state. The append-only coordination journal remains authoritative; `work_units` and `work_unit_actors` are deterministic mutable projections rebuilt from those events.

Lifecycle mutation authorization, ownership policy, human approval, and future RBAC are explicitly deferred. The first slice enforces identity, scope, participation, legal transitions, idempotency, and auditability without attempting to settle the final authority model.

### Relationship to JJ changes

A WorkUnit is semantic coordination state, not a synonym for a JJ change. One WorkUnit may begin inside shared `@`, span many checkpoints, and eventually produce zero, one, or several JJ changes. Several actors may co-author the same WorkUnit, while separate virtual WorkUnits may temporarily coexist in one physical working-copy change.

A later normalized `work_unit_changes` relation may record working, materialized, or follow-up JJ changes. The first WorkUnit slice should only preserve JJ identities through checkpoint evidence and must not require VCS history shaping.

### Initial WorkUnit slice

- [x] add WorkUnit protocol types and journal events;
- [x] project `work_units` and equal-participant `work_unit_actors` tables;
- [x] add `/work <objective>` creation and current-WorkUnit selection in the Pi adapter;
- [x] attach newly declared checkpoints to the current WorkUnit;
- [x] support explicit lifecycle transitions and participant join/leave events; and
- [x] query a WorkUnit with its participants and ordered checkpoint history.

Exit condition met: multiple actors can share a durable goal, contribute immutable checkpoints, observe the same replayed lifecycle state, and complete or abandon the WorkUnit without treating Pi turns or JJ snapshots as semantic task boundaries.

### Coordination stress acceptance

The WorkUnit slice should include a disposable multi-agent fixture that verifies both semantic orchestration and lower-level coordination:

- several actors register and join one WorkUnit as equal participants;
- disjoint backend/frontend work proceeds concurrently in one physical JJ `@`;
- each meaningful component boundary produces an ordered checkpoint;
- a later QA participant may contradict or supersede an unsupported claim with stronger evidence without mutating history;
- repeated daemon restart and journal replay preserve participants, lifecycle, mailbox state, and checkpoint order;
- two actors declaring the same canonical path produce exactly one open collision and durable collision mail for both;
- unacknowledged mail survives replay, direct participant communication advances negotiation, and acknowledgement clears delivery;
- yield and resolution remain explicit state transitions; and
- the final WorkUnit verification cites integrated test, build, and runtime evidence rather than trusting component summaries.

The initial local stress fixture demonstrated the shape successfully across NestJS/SQLite backend work, SolidJS frontend work, an independent QA pass, repeated full journal replays, and an exact-path mailbox collision. It also exposed two required safeguards: metadata-only “tests pass” claims are not verified evidence, and semantically duplicate checkpoints with different IDs remain visible rather than being similarity-merged.

Top-level workflow ordering and WorkUnit dependencies follow after this slice; they should be represented relationally rather than inferred from checkpoint time.

## Phase 3 — Direction orchestration

**Status: local core landed; dependency/readiness and richer rollups remain.** Direction creation, lifecycle, session selection, WorkUnit attachment and nesting, repository-aware status rollups, and bounded non-Pi worker controls are implemented.

A Direction is a durable project above WorkUnits. It coordinates a coherent larger outcome that may be decomposed, reprioritized, and carried by different agents over time.

### Project, issue, sub-issue, and evidence mapping

The model should borrow the useful shape of Linear without copying its product vocabulary or treating checkpoints as tasks:

```text
Linear concept     Agent Bridge concept
project            Direction
issue              WorkUnit
sub-issue          child WorkUnit
activity/evidence  checkpoint and provenance
```

A possible initiative or portfolio layer sits above Directions and remains deliberately unnamed and deferred. Directions are not recursively overloaded to represent both initiatives and projects. WorkUnits may be nested when a bounded issue needs smaller independently executable sub-issues.

```text
Direction
  ├── WorkUnit
  │     ├── child WorkUnit
  │     │     └── checkpoints
  │     └── checkpoints
  └── WorkUnit
        └── checkpoints
```

Checkpoints remain immutable evidence beneath WorkUnits and never become mutable issue records.

### Organizing hierarchy and JJ changes

The practical operator hierarchy is:

```text
Direction
  -> WorkUnits
    -> zero or more JJ changes
      -> checkpoints and mutation evidence
        -> observed file changes
```

A Direction composes the repository-scoped WorkUnits required for one integrated outcome. A WorkUnit composes the JJ changes that materialize that issue over time; it is not required to map one-to-one to a change. Checkpoints are immutable evidence boundaries over the mutations, tests, messages, and observed file changes that led toward or validated those JJ changes. They do not own files or replace JJ history.

The eventual `work_unit_changes` relation should make the WorkUnit-to-JJ association explicit, including working, materialized, and follow-up changes. Checkpoint evidence should retain JJ identity plus normalized mutation/file references so operators can move from strategic outcome down to the exact observed changes without inferring hierarchy from timestamps or prose.

### Direction responsibilities

A Direction should carry:

- a project objective, success criteria, constraints, and strategic context;
- mutable lifecycle state and separately derived operational health;
- participants and optional human-authored coordination policy;
- directly contained and nested WorkUnits;
- WorkUnit dependencies, priorities, and readiness;
- explicit decisions that explain decomposition or replanning; and
- provenance-backed completion evidence rolled up from all descendant WorkUnits.

It should answer project-level questions such as:

- What integrated outcome are we building, and why?
- Which issues and sub-issues are ready, active, blocked, or converging?
- Which agents and machines are contributing to each branch of work?
- What evidence satisfied a dependency or completion criterion?
- How and why did the project plan change?
- Does the integrated result satisfy the Direction rather than only its individual WorkUnits?

### Hypothetical relational model

Directions and WorkUnit relationships should be normalized rather than stored as child UUID lists in JSON:

```text
directions
  direction_uuid BLOB PRIMARY KEY
  objective TEXT NOT NULL
  success_criteria TEXT
  constraints TEXT
  context TEXT
  state TEXT NOT NULL
  created_by BLOB NOT NULL
  created_at DATETIME NOT NULL
  updated_at DATETIME NOT NULL
  completed_at DATETIME

direction_actors
  direction_uuid BLOB NOT NULL REFERENCES directions(direction_uuid)
  actor_uuid BLOB NOT NULL
  joined_at DATETIME NOT NULL
  left_at DATETIME
  participation_state TEXT NOT NULL
  PRIMARY KEY (direction_uuid, actor_uuid)

work_units
  ...
  direction_uuid BLOB REFERENCES directions(direction_uuid)
  parent_work_unit_uuid BLOB REFERENCES work_units(work_unit_uuid)

work_unit_dependencies
  predecessor_uuid BLOB NOT NULL REFERENCES work_units(work_unit_uuid)
  successor_uuid BLOB NOT NULL REFERENCES work_units(work_unit_uuid)
  dependency_kind TEXT NOT NULL
  required INTEGER NOT NULL
  PRIMARY KEY (predecessor_uuid, successor_uuid)
```

The safest first implementation gives each WorkUnit at most one Direction and one parent WorkUnit. Those associations are established explicitly rather than inferred from actors, timestamps, checkpoints, or repository location. The daemon must reject WorkUnit containment cycles and invalid dependency cycles where the selected dependency kind requires an acyclic graph.

Containment and execution order are separate concepts. A child WorkUnit decomposes its parent; a dependency controls readiness. Neither relationship should be encoded as a serialized list or inferred from checkpoint order.

### Lifecycle, health, and convergence

Direction lifecycle may begin with:

```text
draft -> active <-> paused -> converging -> verified -> completed
                 \-------------------------> abandoned
```

Lifecycle is explicit coordination state. Operational health is a derived read model such as:

```text
on_track | at_risk | blocked | conflicted
```

A Direction must not complete automatically merely because every WorkUnit is terminal. `converging` represents the important interval in which individually completed issues are integrated and checked against project-level success criteria. Verification and completion should cite deterministic descendant checkpoints, tests, decisions, and integration evidence.

### Pi Direction workflow

The Pi extension could expose a conversation-local project selection workflow:

```text
/direction <objective>       create and select a Direction
/direction                   show the selected Direction and compact rollup
/direction list              list relevant active Directions
/direction use <uuid>        select an existing Direction
/work <objective>            create a WorkUnit under the selected Direction
/work child <objective>      create a child of the selected WorkUnit
```

Selection is convenience state, not coordination truth. It must be scoped to the actor session and validated by the daemon. Stale selections must be cleared or rejected deterministically. The daemon remains authoritative for Direction identity, WorkUnit containment, participants, lifecycle, and readiness. Agents must never attach work by guessing the latest-created Direction or WorkUnit.

A compact Direction rollup should favor actionable project state over a large injected plan: objective, ready and blocked WorkUnits, active participants, open collisions, latest checkpoints, and unmet success criteria. Detailed provenance remains available lazily.

### Multi-repository and eventual multi-machine scope

A project-sized Direction may still span multiple repositories and machines. It therefore must not have one mandatory `repository_uuid` or `workspace_uuid` column as its identity scope. Repository, workspace, and machine participation should eventually be represented through normalized scope relations.

This capability should be staged:

1. one Direction coordinating WorkUnits in one local repository;
2. one Direction spanning several local repositories and workspaces;
3. synchronized Direction state across several machines; and
4. explicit authority, conflict, and offline-reconciliation policy for federated mutation.

Multi-machine operation is not merely a Turso-sync toggle. Before it can become authoritative, Agent Bridge needs stable machine identity, authenticated actors, deterministic ordering or conflict resolution, explicit write authority, and observable offline behavior. Provenance may replicate before coordination authority does.

### Direction event vocabulary

The append-only journal remains authoritative. A possible initial vocabulary is:

```text
direction.created
direction.updated
direction.actor_joined
direction.actor_left
direction.transitioned
direction.decision_recorded
work_unit.direction_assigned
work_unit.child_created
work_unit.dependency_added
work_unit.dependency_removed
```

Projection tables provide current state and fast rollups. Historical events preserve who changed the project plan, what changed, and which evidence justified the decision. Derived health, readiness, summaries, and semantic claims may be regenerated and must never become the source of truth.

### Direction implementation slices

1. add local, single-repository Direction creation and session-scoped `/direction` selection;
2. attach WorkUnits to one Direction and support cycle-safe child WorkUnits;
3. add explicit WorkUnit dependencies and deterministic readiness computation;
4. add convergence and Direction-level verification backed by checkpoint evidence;
5. add multi-repository scope relations and cross-repository rollups; and
6. design multi-machine authority and reconciliation only after local orchestration is proven.

### Todo dogfood follow-up sequence

The [cross-repository Go/Svelte Todo dogfood](todo-direction-dogfood.md) validated the hierarchy and evidence model while exposing operational gaps. Treat the following as current roadmap sequencing rather than leaving them only in the future parking lot.

Immediate follow-up:

- [x] add replay-safe Direction lifecycle transitions and compact Pi lifecycle controls;
- [x] add deterministic `direction.status` rollup across attached repository-scoped WorkUnits;
- [x] expose each rolled-up WorkUnit's effective repository/workspace root and scope kind so a parent-directory integration scope is explicit;
- [x] provide a bounded non-Pi worker CLI for identity context, mailbox, test result, checkpoint, and lifecycle operations without raw JSON-RPC; and
- [x] retain checkpoints as immutable evidence boundaries rather than treating them as subtasks.

Deferred after the immediate operability slice:

- [x] first-class `parent actor -> launch -> child actor -> optional WorkUnit` provenance with a stable launch UUID and harness attachment events;
- [ ] warn when mutations occur in a scope related to an active WorkUnit but lack WorkUnit context;
- [ ] correlate instrumented creation of brand-new paths by matching active create intent with `Before.Exists=false` before Watchman classifies it as unknown;
- [ ] retire Watchman workspace watches after no active actor remains, with a bounded grace period;
- [ ] remove no-op legacy `activity.*` compatibility RPCs after deployed adapters no longer call them;
- [ ] include actor aliases, roles, liveness, and recent activity in Direction rollups;
- [ ] include open questions and message summaries in Direction status;
- [ ] add explicit normalized WorkUnit-to-JJ-change relations;
- [ ] inject a compact Pi hierarchy cue: `Direction -> WorkUnit -> JJ changes -> checkpoint/mutations -> files`; and
- [ ] add dependencies and deterministic readiness only after local rollup and evidence behavior remain stable under dogfood.

Exit condition for the local slice: several agents can coordinate one project-shaped Direction containing issues and sub-issues, inspect deterministic readiness and blockers, and verify the integrated outcome from descendant checkpoint evidence without confusing project state, executable work, VCS state, or provenance boundaries.

## Phase 4 — Generic ticket context ✅

**Status: complete.** Protocol normalization, replay-safe Direction and WorkUnit updates, immutable checkpoint tickets, relational projections, and the Pi `bridge_ticket` workflow are implemented. Provider-specific connectivity remains deferred.

Agent Bridge provides a low-friction local place to retain ticket context without choosing Linear, GitHub, Jira, or any other tracker as its model. This phase is deliberately **local only**: tracker connectivity, OAuth, webhooks, remote mutation, synchronization, and cloud projection are deferred.

### Simple local contract

Directions, WorkUnits, and checkpoints each optionally carry `tickets`: an ordered JSON array of zero or more JSON object maps. A map has **no required keys**. It may be as small as:

```json
[{"reference": "https://tracker.example/items/42", "label": "capture foundation"}]
```

or use an entirely different shape:

```json
[{"source": "a human handoff", "work_item": 42, "repository": "owner/repo"}]
```

The daemon stores and replays the canonical JSON exactly as local ticket context; it does not infer provider, ticket kind, identity, lifecycle, ownership, or remote authority from map keys. Empty or absent is normal. The initial implementation validates only that the value is bounded JSON containing an array of object maps and canonicalizes object-key order for deterministic idempotency/replay.

This payload is local annotation, not a remote-identity store. If a future adapter needs a canonical remote UUID for lookup, reconciliation, or mutation, that adapter must put it in its own normalized 16-byte-BLOB mapping table rather than treating an arbitrary JSON value as authoritative identity. Do not put credentials, raw API responses, transcripts, or secrets in ticket maps.

### Pi interaction

Expose one generic `bridge_ticket` tool and compact selected Direction/WorkUnit commands. The tool accepts a target plus a JSON array and does not require users or models to name a provider. Its description should tell the model: when a user supplies ticket context, offer to record it; report it as stored only after the daemon succeeds. Checkpoint tickets are supplied with the immutable checkpoint declaration.

Example interaction:

```text
User: Track this against the capture-foundation ticket.
Agent: I can record that ticket context on the current WorkUnit. [bridge_ticket succeeds]
Agent: Stored the ticket context on WorkUnit <uuid>.
```

The extension must never silently fetch from or mutate a tracker. An agent may preserve a URL, key, title, or arbitrary user-provided fields, but remote access remains outside this phase.

### Durable model and events

Use `tickets_json TEXT NOT NULL DEFAULT '[]'` on the `directions` and `work_units` projections, and add canonical `tickets` to the immutable checkpoint request/event and `checkpoint_requests` projection. Direction and WorkUnit ticket replacement is an explicit journaled update containing before/result values; checkpoint tickets cannot be edited after declaration. Projection rebuild must reconstruct the same canonical JSON.

All normal Agent Bridge UUID relationships remain validated at protocol boundaries and stored as 16-byte BLOB values. The ticket JSON is intentionally opaque annotation and does not replace any relational UUID identity.

### Implementation slices

1. **Local substrate:** protocol validation/canonicalization; Direction and WorkUnit ticket update events; immutable checkpoint ticket payload; projections, replay, idempotency, and BLOB-regression tests.
2. **Pi vertical slice:** `bridge_ticket` create/list/replace/clear behavior plus minimal tool guidance and compact status display.
3. **Dogfood:** record a ticket from two different tracker-shaped inputs, restart/replay, and demonstrate that no network access is required.
4. **Later adapters:** only after local dogfood identifies a real need, add optional provider-specific import/projection adapters behind this generic local field. Their remote UUID mappings, credentials, retry queues, and privacy policy remain separate from ticket context.

### Exit condition

A human can mention any ticket-shaped context naturally; an agent can store it on the intended Direction, WorkUnit, or checkpoint without requiring a provider-specific key or remote access; and replay returns the same local ticket maps. Agent Bridge remains authoritative for local coordination regardless of external tracker availability.

## Phase 5 — External-change provenance and safety work carried forward

**Status: initial unknown-actor, Watchman transport/baseline, continuity, and external-observation substrate landed.** Full stress acceptance and conservative correlation hardening remain active; observed-baseline write protection and destructive-operation admission have not landed.

Continue this lane in this order:

```text
unknown-actor substrate
  -> Watchman external-change provenance
  -> observed-baseline protection
  -> destructive-operation admission and audited override
  -> richer collision evidence and adapter authorization
```

Watchman is a conservative filesystem wake-up source, never authorship authority. Blocking policy should consume its reconciled, replay-tested facts only after continuity and failure behavior are proven.

### Phase 5.0 — Unknown actor identity and addressability

Add a deterministic, workspace-scoped `unknown` actor kind before ingesting external changes:

```text
actor_kind      agent | unknown
addressable     boolean
presence_kind   lease | synthetic
```

Rules:

- existing Pi actors remain addressable, lease-backed `agent` sessions;
- each workspace receives one canonical RFC 4122 version-5-compatible unknown actor UUID derived from the workspace UUID and a fixed `unknown-external` namespace label;
- unknown actors have synthetic presence and are never active or addressable;
- unknown actors cannot send or receive messages, own leases, join collision negotiation, declare overrides, or be selected through aliases; and
- every persisted actor reference remains a validated canonical UUID stored as exactly a 16-byte `BLOB`.

Backfill existing actors as agent/addressable/lease-backed. Reject invalid enum combinations at replay and registration. Add engine tests for every forbidden unknown-actor operation and projection tests asserting BLOB type and length for every new UUID relation.

Exit condition: replay deterministically reconstructs the same non-addressable unknown actor for a workspace, and no coordination API can treat it as a live peer.

### Phase 5.1 — Watchman transport and coverage

Use Watchman as the preferred event-driven recursive source. Do not add a steady polling fallback. If Watchman is missing or unhealthy, keep coordination available, mark external provenance as degraded, and surface the condition through `doctor` and status.

Implement one persistent Watchman client connection that can own multiple Agent Bridge subscriptions. The smallest spike may use `watchman -j --server-encoding=json -p`; a direct socket/BSER client can replace it only if it materially improves lifecycle handling. Start a workspace subscription when its first addressable actor registers, retain it through a bounded idle grace period, and reconstruct prior baselines when it becomes active again. Synthetic unknown actors do not keep watches alive; explicitly pinned always-watched workspaces can be a later configuration. For each active workspace:

1. call `watch-project` with its absolute workspace root;
2. retain the returned `watch` root and optional `relative_path`;
3. use `relative_root` in queries/subscriptions and reconstruct canonical workspace paths without `realpath` or symlink traversal;
4. create a uniquely named subscription tied to the client connection;
5. request at least `name`, `exists`, `type`, `size`, and modification-time metadata; Agent Bridge remains responsible for SHA-256 snapshots;
6. retain the last opaque Watchman clock as a cursor, never as an event identity or timestamp; and
7. unsubscribe only Agent Bridge's subscription on shutdown—never delete a shared Watchman watch owned by other tools.

Respect existing `.watchmanconfig` and global Watchman policy; do not rewrite project configuration. Record effective coverage and surface `ignore_dirs`, unsupported/network filesystems, poison errors, and configuration changes that require rewatch/restart. Watchman does not follow symlink targets; Agent Bridge snapshots must continue to use `lstat`/`O_NOFOLLOW` semantics. Watchman/inotify supplies no process, terminal, TTY, or human identity. Privileged fanotify, Audit, or eBPF enrichment is outside this slice and could only add optional evidence later.

`agent-bridge doctor` should report executable path/version, connection health, watched roots, effective relative roots/exclusions, last clock, last event time, recrawl/poison warning, and whether provenance continuity is current, reconciling, or degraded.

Exit condition: fixture and real-Watchman tests can establish, reconnect, and tear down multiple workspace subscriptions without duplicate Agent Bridge subscriptions, path-scope escapes, or changes to user Watchman configuration.

### Phase 5.2 — Baseline bootstrap, continuity, and replay

Watchman is a wake-up and opaque-cursor source, not snapshot, wall-clock, or authorship authority. Maintain a journal-derived latest baseline for each `(workspace_uuid, canonical_path)`. Baseline updates become visible only after the corresponding Agent Bridge event is durable.

Bootstrap/reconnect algorithm:

1. restore the latest baselines and Watchman cursor from journal replay/projection;
2. establish `watch-project` and obtain a synchronized Watchman clock;
3. enumerate current covered paths and take safe Agent Bridge snapshots through a bounded background worker pool, applying the configured hash-size ceiling and recording metadata-only baselines where hashing is intentionally unavailable;
4. query/subscribe `since` the pre-enumeration clock so changes racing the crawl are not lost;
5. establish initial baselines without inventing historical mutations when no prior baseline exists; and
6. compare against persisted baselines on restart, emitting an observed-during-downtime interval only for actual snapshot differences.

Handle `is_fresh_instance`, recrawl warnings, invalid clocks, subscription cancellation, Watchman restart, poison, and Agent Bridge downtime as explicit continuity boundaries:

- journal `watch.continuity_lost` with the last good clock and reason;
- run a full baseline reconciliation;
- compare real snapshots rather than trusting Watchman's recrawl result, which may mark most files changed;
- emit external observations only for actual before/after differences;
- label their time as an observation interval when the exact change time is unknowable; and
- journal `watch.continuity_restored` with the new cursor and coverage.

Never convert a fresh-instance result into one unknown mutation per existing file. A first-ever baseline has no attributable “before” state.

Exit condition: restart, recrawl, clock invalidation, and subscription-cancellation fixtures converge to the real filesystem state without missed actual differences or fabricated authorship.

### Phase 5.3 — External-change events and conservative correlation

Add authoritative journal events and normalized projections such as:

```text
external_change.observed
watch.continuity_lost
watch.continuity_restored
```

An external observation records a UUID, unknown actor UUID, repository/workspace UUIDs, observation interval, continuity state, change kind, before/after `FileSnapshot`, Watchman cursor evidence, and related intent IDs. Store UUID relations in 16-byte `BLOB` columns; use relational path/evidence rows rather than UUID lists in JSON.

A practical first projection is:

```text
external_changes
  external_change_uuid BLOB PRIMARY KEY
  unknown_actor_uuid BLOB NOT NULL
  repository_uuid BLOB NOT NULL
  workspace_uuid BLOB NOT NULL
  observed_start TEXT NOT NULL
  observed_end TEXT NOT NULL
  continuity_state TEXT NOT NULL
  watchman_clock TEXT
  event_sequence INTEGER NOT NULL

external_change_paths
  external_change_uuid BLOB NOT NULL
  path TEXT NOT NULL
  change_kind TEXT NOT NULL
  before_json TEXT NOT NULL
  after_json TEXT NOT NULL
  PRIMARY KEY (external_change_uuid, path)

external_change_intents
  external_change_uuid BLOB NOT NULL
  intent_id TEXT NOT NULL
  correlation_kind TEXT NOT NULL
  PRIMARY KEY (external_change_uuid, intent_id)

workspace_file_baselines
  workspace_uuid BLOB NOT NULL
  path TEXT NOT NULL
  snapshot_json TEXT NOT NULL
  event_sequence INTEGER NOT NULL
  PRIMARY KEY (workspace_uuid, path)
```

`workspace_file_baselines` is a rebuildable projection, not an independent source of truth. Watchman subscription/clock health may be projected for status, but the journaled continuity events remain authoritative.

Coalesce noisy notifications per canonical path, then snapshot after Watchman subscription settlement plus a bounded Agent Bridge debounce; Watchman does not provide a portable write-close contract. Infer only file-state transitions—created, modified, deleted, or type-changed. Treat rename as delete/create unless deterministic file identity proves more. This is coalesced state provenance, not a syscall audit log: a transient write that returns to the same snapshot before observation may produce only a wake-up/no-op and must not be reported as a content transition. Do not store file contents by default.

A Watchman notification is explained by an instrumented intent only when all high-confidence facts agree:

- exact canonical workspace path;
- the intent's durable before state matches the current persisted baseline;
- the intent has completed or settled enough to provide an after snapshot;
- the intent's after snapshot exactly matches the newly observed filesystem state; and
- no intervening durable observation contradicts that transition.

Buffer events overlapping active intents until `intent.end` or a bounded timeout. Timing overlap, process lifetime, an active human editor, or Watchman state metadata alone never proves authorship. If several intents could explain the same transition, record ambiguous-known correlation; if the final state differs from the instrumented after snapshot, record unknown or mixed evidence rather than suppressing it.

Notify active actors in the workspace initially, then narrow by path/WorkUnit scope when richer scope exists. The message must say “unattributed external change observed,” include path/change kind/bounded snapshot evidence and continuity confidence, and instruct agents to re-read before writing. Unknown actors never receive collision or mailbox messages.

Stress with real filesystem writers:

- Neovim headless and manual atomic saves;
- `sed -i`, `perl -pi`, Python `Path.write_text`, and formatter rewrites;
- `printf >>`, truncate, `dd conv=notrunc`, and interrupted/slow writes;
- create/delete/copy/move and temp-file rename over target;
- symlink creation/replacement and file/directory type changes;
- rapid repeated writes and generated-tree bursts; and
- known Pi edit/write/bash mutations that must correlate rather than duplicate as unknown.

Exit condition: unexplained saved changes eventually produce one conservative durable observation, known instrumented final states do not produce duplicate unknown attribution, and mixed states remain explicitly ambiguous.

### Phase 5.4 — Observed-baseline preflight protection

Build protection from explicit read observations and the baseline projection, not from Watchman timing. Add a `file.observed` event containing actor UUID/generation, workspace/path, observation kind, snapshot, tool/turn evidence, and journal sequence.

For Pi, observe successful `read` tool executions through generic `tool_call` plus `tool_result`, and only claim a whole-file observed hash when the returned content is complete and can be verified against a safe current snapshot. Partial/truncated reads may establish range evidence later but must not masquerade as a whole-file baseline.

Attach the latest actor observation to mutation preflight:

- exact-text `edit` retains its built-in compare-and-swap behavior and still checks existence/type changes;
- whole-file `write` compares its expected observed snapshot to the current durable/filesystem baseline;
- no known baseline initially warns and allows ordinary writes, while policy may be tightened after measuring false positives;
- a high-confidence mismatch blocks and emits a stale-baseline signal with the intervening known/unknown evidence; and
- arbitrary or broad shell writers cannot claim baseline safety and escalate to workspace policy.

Pi's `tool_call` hook can block, but preflight is not an OS-level transaction against uninstrumented external writers. Document the remaining race and do not claim linearizable file writes.

Exit condition: an instrumented whole-file write based on a file another actor changed since observation is blocked before execution, while fresh exact-text edits and disjoint writes remain low-friction.

### Phase 5.5 — Destructive-operation admission and human overrides

Evolve `intent.begin` into an atomic policy decision:

```text
grant | wait | warn | block
```

Classify restore/reset/clean/delete and workspace-wide VCS operations by blast radius. Parse only command forms that can be classified safely; shell metacharacters, compound commands, unknown flags, or unbounded paths become broad/unknown scope rather than receiving optimistic parsing.

At minimum cover `rm`, recursive moves/deletes, `git restore/checkout/reset/clean/stash`, and JJ restore/abandon/working-copy or operation-log transitions documented in [the JJ-native workflow roadmap](workflow-roadmap.md). The daemon evaluates active actors/intents, workspace scope, external observations since the actor baseline/checkpoint, and operation ownership before deciding.

A human override is a separate durable event, not a boolean on the blocked attempt. It must include human actor UUID, blocked decision/event ID, exact operation fingerprint and scope, reason, expiry or one-shot use, and resulting outcome. Reject reuse for changed arguments, paths, workspace, or generation. Unknown actors and unbound clients cannot override. The daemon owns authorization and audit regardless of the presenting adapter.

Exit condition: destructive shared-workspace operations cannot execute through the Pi adapter without a deterministic grant or matching one-shot human override, and replay explains both the original block and override use.

### Phase 5.6 — Richer collision evidence and adapter authorization

Add richer evidence incrementally without replacing exact-path facts:

1. directory/path-prefix overlap for recursive or generated operations;
2. configured generated-output and lockfile relationships;
3. normalized WorkUnit scope overlap; and
4. optional LSP symbol/range relationships with source/version metadata.

Store collision evidence as normalized rows with evidence kind, paths/symbols, confidence, and source version. Generated/symbol evidence may warn or escalate but must not silently become authorship or semantic truth.

Separate adapter-declared capabilities from daemon-effective authorization. Validate the Phase 5.0 registration credential (or a later authenticated persistent connection), reject cross-actor mutation/message attempts, and authorize methods by actor kind plus effective capabilities such as:

```text
session.register  mailbox.receive  message.send
external.observe   file.observe     file.preflight
human.override    collision.block  turn.steer
```

Owner-only socket permissions remain the local transport boundary, but capability claims alone do not grant authority. Every denied privileged attempt and effective-policy change is auditable.

Exit condition: future adapters can participate at an explicitly bounded compatibility level without impersonating another actor or acquiring human/destructive authority by self-declaration.

### Phase 5 research basis

Implementation should verify the installed versions rather than freezing assumptions. Primary references used for this plan:

- [Watchman `watch-project`](https://facebook.github.io/watchman/docs/cmd/watch-project), [`subscribe`](https://facebook.github.io/watchman/docs/cmd/subscribe), [clocks](https://facebook.github.io/watchman/docs/clockspec), [queries](https://facebook.github.io/watchman/docs/cmd/query), [configuration](https://facebook.github.io/watchman/docs/config), [query synchronization](https://facebook.github.io/watchman/docs/cookies), [installation](https://facebook.github.io/watchman/docs/install), and [recrawl/poison troubleshooting](https://facebook.github.io/watchman/docs/troubleshooting)
- installed Pi extension and RPC documentation for blocking `tool_call`, `tool_execution_*`, `sendMessage`, settlement, RPC UI degradation, and strict JSONL framing
- [the canonical prototype retrospective](retrospective.md) and [the JJ-native workflow roadmap](workflow-roadmap.md)

## Deferred — Bounded session communication scopes

Do not make subagent messaging RBAC a current blocker; it is a low-priority 1–2% optimization. Later, allow a launching parent or WorkUnit policy to assign each session an enforced `talk_list` (or equivalent normalized communication scope) that limits which actors it may address.

Support configurable profiles rather than one hard-coded hierarchy: parent and siblings, parent only, no outbound messaging, or an explicit actor/scope allowlist. The daemon—not agent instructions or adapter claims—must enforce the effective scope on `message.send`, reject stale or out-of-scope recipients, and audit policy assignment, changes, and denials. Parent/child and sibling membership must come from explicit launch or WorkUnit relationships, never aliases, timestamps, or inferred activity. Keep unrestricted peer communication as the existing behavior until this is justified by a concrete isolation or supervision need.

## Deferred — Contention-activated semantic enrichment

Do not implement active-actor smart triggers, Spark budgeting, catch-up indexing, or automatic semantic extraction as part of the checkpoint substrate. Revisit these only after agent-authored checkpoints and top-level workflow ordering are useful on their own.

If semantic enrichment is later activated by contention, it must remain a derived, bounded, evidence-citing read model and must never replace agent-authored task state or coordination truth.

## Deferred — Lazy semantic retrieval

Do not add semantic retrieval or prompt injection yet. Existing provenance queries, explicit checkpoint reads, and local Pi reasoning are sufficient for the first checkpoint/work-unit workflow. Add compact retrieval packets only after there is demonstrated demand for attribution, handoff, recovery, or coordination explanations.

## Deferred — Continuous semantic supervisor

Do not yet build a continuously steering reviewer that emits scope, faithfulness, test-quality, or replan signals.

Revisit it only after:

- semantic checkpoints are measurably useful;
- extraction quality and evidence citation are understood;
- false-positive and interruption budgets exist;
- deterministic safety policy is mature; and
- the human can inspect and override every semantic intervention.

## Explicit non-goals for the current roadmap

- model calls during single-agent serial work;
- full-transcript indexing by default;
- treating Spark output as coordination truth;
- injecting summaries into every agent turn;
- autonomous semantic blocking;
- broad cross-harness expansion before the Pi workflow is solid;
- cloud upload of provenance or semantic checkpoints by default; and
- a standalone second chat UI disconnected from the existing Herdr and Zed surfaces.
