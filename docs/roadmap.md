# Roadmap

## Product direction

Agent Bridge is a local coordination control plane for independently launched coding agents. Near-term work should strengthen the Pi vertical slice, preserve deterministic provenance as truth, and add semantic understanding only when concurrent work makes its cost useful.

The checkpoint and WorkUnit substrate is complete. The next major capability is **Direction orchestration**: project-sized coordination over issues and sub-issues, while evidence-backed checkpoints and deterministic provenance remain the source of truth. Semantic extraction stays optional enrichment rather than coordination authority.

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

A Direction is a durable project above WorkUnits. It coordinates a coherent larger outcome that may be decomposed, reprioritized, and carried by different agents over time. This phase is intentionally hypothetical until the WorkUnit substrate demonstrates which project-level coordination queries are genuinely useful.

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

Exit condition for the local slice: several agents can coordinate one project-shaped Direction containing issues and sub-issues, inspect deterministic readiness and blockers, and verify the integrated outcome from descendant checkpoint evidence without confusing project state, executable work, VCS state, or provenance boundaries.

## Phase 4 — Linear cloud projection and shared agent memory

This phase follows local Direction orchestration. It projects selected Agent Bridge Directions, WorkUnits, checkpoints, and evidence-backed claims into Linear so teammates and Linear Agent can share durable project memory without requiring each coding agent to call Linear MCP directly.

Linear is an external collaboration projection, not the coordination authority. Agent Bridge's local journal, normalized provenance, checkpoint identity, and lifecycle remain canonical. Linear unavailability, authentication expiry, rate limits, or content divergence must never block local checkpoint creation or WorkUnit progress.

### Why Linear fits

The useful mapping is:

```text
Agent Bridge          Linear
Direction             project
WorkUnit               issue or sub-issue
current Direction      project document and project status
current WorkUnit       one managed issue rollup comment
checkpoint             immutable issue/project comment or project status update
checkpoint claim       structured Markdown claim row
checkpoint evidence    bounded references, outcomes, hashes, and external links
```

Linear's current MCP server technically supports finding, creating, and updating projects, issues, comments, and documents over authenticated remote MCP. Agent Bridge deliberately uses only the bounded lookup/link and projection capabilities described here; it never invokes project or issue creation. The available comment contract can create or update comments on issues, projects, initiatives, documents, milestones, and status updates. Project documents provide Markdown project memory, while project and initiative status updates provide a visible project-level progress stream with health.

This also makes the projection useful inside Linear itself: Linear Agent reasons over issues, projects, comments, activity history, and documents. A standardized Agent Bridge projection therefore becomes shared human and agent memory rather than an opaque integration side channel.

### Background shim, not prompt-visible MCP

Individual agents should not receive Linear MCP tools or schemas in their normal prompt. The daemon owns a background `LinearProjection` adapter with a small Agent Bridge-specific control surface:

```text
linear.link
linear.status
linear.retry
linear.pause
linear.reconcile
```

The adapter owns MCP connection management, authentication state, retries, formatting, deduplication, and remote identifier mapping. Pi surfaces only compact projection health and explicit human controls. No model call is required to format routine checkpoint exports.

The remote MCP transport is currently authenticated Streamable HTTP. OAuth 2.1 is the normal interactive path; bearer tokens and permission-scoped Linear API keys are also supported. Credentials must remain in OS credential storage or the MCP client's protected auth store, never in the Agent Bridge journal, provenance database, checkpoint metadata, or prompts. Each Linear workspace requires an explicit authentication context.

Projection is opt-in per Direction. Read-only and disabled modes should remain available. Agent Bridge must not infer consent from generic environment variables or silently upload local provenance.

A permanent product invariant is that Linear projects and issues are created manually outside Agent Bridge. The integration is projection-only onto explicitly linked existing Linear structure: a human links a Direction to an existing Linear project and each WorkUnit to an existing issue or sub-issue. Agent Bridge may create its managed comments and Direction memory document under those linked resources, but it never creates Linear projects, issues, or sub-issues. Missing links pause projection and surface a setup action rather than guessing or creating work.

### Dual representation: immutable history plus mutable rollup

A single constantly edited comment is insufficient because it destroys checkpoint history. A comment per checkpoint without a rollup is also inconvenient for humans. Use both:

1. **Immutable checkpoint entry.** Export each selected checkpoint once as a new issue or project comment. Never rewrite it after successful reconciliation. Corrections create a new comment that references the prior checkpoint/comment.
2. **Mutable rollup.** Maintain one managed comment on every linked WorkUnit issue containing current objective, lifecycle, participants, latest verified checkpoint, blockers, and next step.
3. **Direction memory document.** Maintain one project document containing the Direction objective, success criteria, constraints, WorkUnit graph/rollup, integration decisions, and the Agent Bridge projection format version.
4. **Direction status stream.** Export meaningful Direction convergence/verification boundaries as new project status updates rather than continuously rewriting one status message.

The immutable checkpoint comment preserves causality. The managed rollup and project document make the latest state readable without reconstructing the entire thread.

### Standard checkpoint comment format

Use deterministic Markdown generated from relational data, not model-authored prose:

```markdown
<!-- agent-bridge:v1 checkpoint:<checkpoint-id> -->
## Agent Bridge checkpoint · verified

**WorkUnit:** `<work-unit-uuid>`

**Boundary:** journal `120..146` · checkpoint `<checkpoint-id>`

**Declared by:** agent · 2026-08-22T18:08:20Z

### Claims
| Status | Kind | Statement |
| --- | --- | --- |
| verified | test | API and frontend suites pass |
| failed | test | strict repository lint baseline is not clean |

### Evidence
- test result `…` · passed · `go test ./...`
- test result `…` · failed · `./scripts/quality.sh`
- JJ change/commit and Git HEAD when available

_Projected by Agent Bridge. Local journal identity remains authoritative._
```

Default exports include claims, outcomes, bounded command labels, content hashes, VCS identities, and safe external links. They must not include raw transcripts, complete command output, absolute local paths, secrets, private mailbox bodies, or full mutation snapshots. Uploading richer evidence is a separate explicit policy.

### Exact identity and deduplication

Do not use semantic similarity or SimHash. Every exported object carries a machine-readable marker containing the format version and canonical Agent Bridge identity. Exact reconciliation uses that marker plus the local mapping table.

A lost MCP response is resolved by listing comments and searching for the exact marker before retrying creation. Once a checkpoint comment is reconciled, Agent Bridge stores its Linear comment UUID and content SHA-256. Similar prose under a different checkpoint identity remains a distinct event.

Linear issue identifiers such as `CAS-123` are convenient aliases, not canonical identity. Linear entity UUIDs cross the MCP boundary as strings and are persisted locally as 16-byte `BLOB` values under the repository UUID rules.

### Local projection model

```text
linear_connections
  connection_uuid BLOB PRIMARY KEY
  linear_workspace_uuid BLOB NOT NULL
  mode TEXT NOT NULL
  created_at DATETIME NOT NULL
  paused_at DATETIME

linear_direction_links
  direction_uuid BLOB PRIMARY KEY
  connection_uuid BLOB NOT NULL
  project_uuid BLOB NOT NULL
  document_uuid BLOB
  rollup_comment_uuid BLOB

linear_work_unit_links
  work_unit_uuid BLOB PRIMARY KEY
  direction_uuid BLOB NOT NULL
  issue_uuid BLOB NOT NULL
  rollup_comment_uuid BLOB

linear_checkpoint_exports
  checkpoint_id TEXT PRIMARY KEY
  target_kind TEXT NOT NULL
  target_uuid BLOB NOT NULL
  comment_uuid BLOB
  content_sha256 TEXT NOT NULL
  state TEXT NOT NULL
  attempts INTEGER NOT NULL
  last_attempt_at DATETIME
  exported_at DATETIME
  last_error TEXT

linear_projection_outbox
  operation_uuid BLOB PRIMARY KEY
  aggregate_kind TEXT NOT NULL
  direction_uuid BLOB
  work_unit_uuid BLOB
  checkpoint_id TEXT
  operation_kind TEXT NOT NULL
  state TEXT NOT NULL
  attempts INTEGER NOT NULL
  available_at DATETIME NOT NULL
  created_at DATETIME NOT NULL
```

Keep foreign identifiers relational. Do not store Linear project, issue, comment, document, or status-update UUIDs inside JSON payloads when columns can represent them.

### Durable export flow

```text
local journal append
  -> local state/provenance apply
  -> relational projection outbox row
  -> background MCP worker
  -> exact remote-marker reconciliation
  -> Linear create/update operation
  -> local export mapping + content hash
  -> projection success event
```

Requirements:

- enqueue in the same local transaction/projection boundary as the source event;
- retry with bounded exponential backoff and jitter;
- serialize updates per remote project/issue/comment target;
- make create operations recoverable after a lost response through marker lookup;
- keep immutable checkpoint exports append-only;
- use exact-text patch/update operations for managed documents where supported;
- expose queue depth, oldest age, last success, last error, auth state, and paused/degraded state;
- allow explicit retry and pause without deleting local evidence; and
- journal projection results without turning Linear responses into coordination authority.

### Initial direction of synchronization

Start one-way: Agent Bridge to Linear.

Linear comments written by teammates are valuable shared memory, but should initially be observed rather than automatically mutating local Direction or WorkUnit state. A later inbound event can record:

```text
external.linear_observed
external.linear_comment_observed
external.linear_issue_changed
external.linear_project_changed
```

Inbound observations may generate a proposal, mailbox notification, or explicit reconciliation task. They must not silently transition a WorkUnit, alter acceptance criteria, replace checkpoint claims, or resolve collisions.

For the first inbound slice, bounded polling through MCP is sufficient. Linear's public API also supports webhooks for issues, comments, documents, projects, and project updates; webhook ingestion can replace polling when multi-machine deployment and authenticated public ingress are justified.

### Managed-content divergence

Before updating a rollup comment or project document, compare the last exported content hash with the current Linear content.

- unchanged managed content: update normally;
- teammate edited managed content: stop automatic overwrite and surface a reconciliation task;
- checkpoint comment edited or deleted remotely: preserve local truth, record divergence, and do not silently recreate or overwrite without policy;
- issue moved between projects/teams: retain identity by Linear UUID and refresh its observed placement;
- remote object inaccessible: mark projection degraded without deleting the local link.

Human-authored Linear discussion remains human-authored. Agent Bridge should own only clearly marked generated regions or comments.

### Permissions and privacy

Linear permissions apply to MCP operations. Private teams and projects can expose sensitive data through API credentials, webhooks, and integrations, so each Direction link needs an explicit target workspace/project and export policy.

Recommended defaults:

- projection disabled until explicitly linked;
- least-privilege MCP/API authorization, without Linear's `Create issues` permission where API-key scopes permit;
- an adapter-level method allowlist that forbids project/issue/sub-issue creation even when the credential is more powerful;
- no mutation of issue/project core fields beyond Agent Bridge-owned comments, documents, and status projections;
- no transcript or raw-output upload;
- no private mailbox-body upload;
- no automatic export from repositories marked local-only;
- clear per-Direction indicator of what is projected;
- human-visible identity for the integration/app user where Linear supports it; and
- audit events for link, unlink, pause, retry, divergence, and remote deletion.

A future Agent Bridge Linear app user could provide clearer authorship than a personal user token. Linear agents/app users can participate in comments and projects under scoped workspace permissions, but app installation, delegation, RBAC, and teammate identity mapping are later product/security work.

### Linear Agent and Loops opportunities

Once the projection format is stable, Linear Agent can summarize or reason over the managed project document and checkpoint comments using normal Linear context. Team guidance or shared skills can teach the standardized marker and claim vocabulary.

Linear Loops could later react to project or issue conditions and use MCP connectors, but Loops are not required for Agent Bridge projection. They are plan/credit/permission dependent and should remain optional automation above the deterministic export layer.

A reverse integration is also possible later: expose a read-only Agent Bridge MCP server to Linear Agent so Linear can request bounded local provenance packets. That path requires explicit machine reachability, authorization, redaction, and user presence policy; it is not part of the initial cloud projection.

### Implementation slices

1. define and fixture-test the Markdown projection format and exact marker parser;
2. add Linear connection/link/outbox/export mapping tables;
3. build a capability-bounded MCP client adapter with protected credentials and projection health;
4. link one local Direction to one existing Linear project explicitly;
5. explicitly link each WorkUnit to an existing Linear issue/sub-issue and maintain its managed rollup comment;
6. export immutable evidence-backed checkpoint comments with exact reconciliation;
7. maintain a Direction project document and export Direction status updates;
8. add degraded/offline/auth-expired UX, retries, pause, and divergence handling;
9. dogfood with one non-sensitive project and compare local replay against Linear reconstruction; and
10. add inbound observations through bounded polling, with webhooks deferred until public ingress is warranted.

### Exit condition

A linked local Direction can project multiple WorkUnits and evidence-backed checkpoints into one Linear project without exposing raw sensitive provenance. Teammates and Linear Agent can inspect current rollups and immutable checkpoint history. Replaying the local journal reconstructs the same export outbox and remote mappings; duplicate exports, lost responses, expired auth, remote edits, and Linear downtime do not corrupt or block local coordination.

### Research basis

- [Linear MCP server](https://linear.app/docs/mcp): hosted Streamable HTTP MCP, OAuth 2.1/bearer/API-key authentication, read-only option, and issue/project/comment tooling.
- [Linear API and Webhooks](https://linear.app/docs/api-and-webhooks): GraphQL mutations and webhooks for issues, comments, documents, projects, and project updates.
- [Linear project overview](https://linear.app/docs/project-overview): project descriptions, resources, documents, milestones, and inline comments.
- [Linear Agent](https://linear.app/docs/linear-agent): workspace reasoning over projects, issues, comments, activity history, and documents; comments and project documents are useful shared agent context.
- Current Linear MCP tool contracts additionally confirm updateable comments across issues/projects/initiatives/documents/status updates, project documents with atomic exact-text patches, and project/initiative status updates with health.

## Phase 5 — Human presence, external provenance, and safety work carried forward

This lane may proceed alongside the checkpoint and WorkUnit substrates. Implement it in this order:

```text
actor and live-feed substrate
  -> Zed ACP human/multiplayer facade
  -> Watchman external-change provenance
  -> observed-baseline protection
  -> destructive-operation admission and audited override
  -> richer collision evidence and adapter authorization
```

The ordering is intentional. ACP needs honest human/session identity and an ordered live feed. Watchman needs persisted baselines and a non-addressable unknown actor. Blocking policy should consume those facts only after their replay and failure behavior are tested.

### Phase 5.0 — Actor kinds, addressability, and ordered live events

The current implementation treats an actor address as a live session UUID. Extend that model minimally before adding ACP or Watchman:

```text
actor_kind      agent | human | unknown
addressable     boolean
presence_kind   lease | synthetic
```

Initial rules:

- `agent` actors are harness sessions with heartbeat leases and mailboxes.
- `human` actors are adapter-backed sessions. The first ACP version uses a configured canonical human UUID and increments its generation on reconnect; Zed's opaque ACP session ID is adapter metadata, not the canonical actor UUID.
- `unknown` is a canonical RFC 4122 version-5-compatible deterministic UUID derived from the workspace UUID and a fixed `unknown-external` namespace label, has synthetic presence, and is never active or addressable.
- The ACP facade is a transport, not the author of relayed human or Herdr-agent events.
- An unknown actor cannot send or receive messages, own leases, join collision negotiation, declare overrides, or be selected through aliases.
- `Address` and every persisted actor reference must validate as canonical UUIDs at the protocol boundary and be stored as exactly 16-byte `BLOB` values. Do not silently convert arbitrary strings to blobs.
- Before accepting human-authored events, bind requests to the registered actor UUID and generation and reject mismatched sender/actor fields. Current Pi and Go clients open one socket per call, so do not assume connection affinity: either migrate them to authenticated persistent connections or, preferably for the first slice, return an ephemeral registration credential that every actor-scoped request must carry. Keep that credential memory-only, rotate it on registration/restart, and never journal or project it. Full method-level capability policy follows in Phase 5.9, but transport-level anti-impersonation is a Phase 5.0 prerequisite.

The first migration may add `actor_kind`, `addressable`, and `presence_kind` to the existing `actors` projection because the canonical actor UUID remains the session address. Backfill existing rows as `agent`, addressable, lease-backed. Reject invalid enum combinations at replay and registration rather than guessing. Add explicit engine tests for every forbidden unknown-actor operation and projection tests asserting `typeof(column) = 'blob' AND length(column) = 16` for every new UUID-bearing column. If concurrent independent ACP presences for one human become necessary, introduce a normalized principal-to-presence relation rather than placing presence UUIDs in JSON.

Real-time consumers also need an ordered daemon feed. Add a cursor-based subscription or bounded long-poll API over journal sequence with these properties:

- durable events replay from `after_sequence` before live delivery;
- every frame includes journal sequence and repository/workspace scope;
- reconnect resumes without duplication or reordering;
- ephemeral high-rate presence updates use a separate connection-local sequence and are never presented as durable provenance;
- projection-backed payloads include the projection watermark and cannot be presented as newer than their evidence; and
- slow consumers are disconnected with an explicit resumable cursor rather than causing coordination-path backpressure.

Do not stream full transcripts, reasoning, or unbounded terminal output. Durable activity boundaries may cite bounded output/test evidence; token-by-token output and cursor-like presence are ephemeral.

Exit condition: a replay fixture can represent addressable agent and human sessions plus a deterministic non-addressable unknown actor, and a reconnecting subscriber receives a causally ordered, deduplicated durable feed followed by live events.

### Phase 5.1 — Zed ACP contract spike

Build a disposable protocol spike before the product facade. Use a custom Zed External Agent launched through `agent_servers` and ACP v1 over stdio newline-delimited JSON-RPC: one encoded protocol message per line and no non-ACP output on stdout. Verify behavior against a pinned Zed version and save fixture transcripts for:

1. `initialize` capability negotiation and `clientInfo` contents;
2. `session/new`, `session/load` or the explicit decision not to advertise loading, and absolute `cwd` handling;
3. `session/prompt` completion and cancellation;
4. `session/update` message chunks and tool-call updates with `locations`;
5. updates sent while no prompt request is active;
6. reconnect/process death and duplicate session creation; and
7. Zed's ACP log output through `dev: open acp logs`.

Stock ACP provides no authenticated human identity, native multi-participant identity, cursor/selection stream, or arbitrary editor-buffer edit notification. Configure the human UUID and display name in Agent Bridge-owned local configuration; never derive identity from `clientInfo`, a display name, or the active OS user. Custom `_...` methods and `_meta` are allowed by ACP but must not become required unless Zed advertises matching support.

The idle-update experiment is a release gate. If stock Zed does not render `session/update` notifications outside an active prompt, document that limitation and use explicit refresh/replay while pursuing a Zed-side capability. Do not keep a fake prompt or tool call open indefinitely merely to obtain a spinner-backed event channel.

Exit condition: checked-in protocol fixtures state exactly which standard ACP messages Zed accepts, when it renders them, and which desired multiplayer behaviors require Zed-specific support.

### Phase 5.2 — Zed ACP human and Herdr multiplayer facade

Use the ACP process as an Agent Bridge facade, not as an agent runtime:

```text
Herdr-hosted Pi agents -> Agent Bridge daemon -> ACP facade -> Zed Agent Panel
            shared workspace files <--------------------------> Zed editor
```

Herdr remains the runtime and supervisory UI. For each ACP session the facade should:

1. resolve `cwd` through repository/workspace scope and register the configured human actor generation;
2. subscribe to the ordered Agent Bridge feed from its journal cursor;
3. translate an explicit `@alias message` or structured command into a durable human-authored `message.send`;
4. poll the human mailbox and acknowledge only after the facade has durably advanced its delivery cursor and successfully serialized the ACP update; ACP provides no proof that Zed rendered a notification, so delivery remains at-least-once across process failure;
5. label relayed events with the original actor alias and UUID in visible text and `_meta`, never the facade identity;
6. map live tool activity to ACP `tool_call`/`tool_call_update`, using globally unique per-session tool-call IDs and absolute path/optional line `locations`;
7. render mutations, collisions, checkpoints, WorkUnits, and bounded diffs/evidence as compact updates without exposing stored secrets or full transcripts; and
8. return deterministic `end_turn`, `cancelled`, and error outcomes for every `session/prompt`.

Start read-only except for human messaging and explicit checkpoint/WorkUnit actions. Do not proxy model execution, filesystem writes, or shell commands through this facade. ACP `fs/read_text_file` can expose unsaved editor contents only when an agent explicitly requests a read; it is not a general Zed edit-event stream. A manual Zed save therefore remains unknown unless a future Zed adapter emits a causal save event.

Required acceptance tests:

- a Zed-authored message is journaled under the human UUID;
- a relayed Herdr mutation retains the Herdr agent UUID;
- an active ACP session does not cause a manual file save to be attributed to the human;
- duplicate prompt retries do not duplicate durable messages;
- reconnect resumes from the last acknowledged journal/mailbox cursor;
- projection lag cannot reorder the displayed feed; and
- malformed/out-of-scope paths are rejected without terminating the daemon.

Exit condition: Andrew can open a Zed workspace, see and address active Herdr agents in a labeled coordination feed, and contribute durable human-authored messages/checkpoints while all agent runtimes remain independently hosted in Herdr.

### Phase 5.3 — Agent presence and follow-along

Add a bounded adapter event vocabulary for live activity:

```text
activity.started
activity.updated      ephemeral and rate-limited
activity.completed
presence.focused      ephemeral path + optional line/symbol
```

The Pi adapter can source these from documented `tool_execution_start/update/end`, `tool_call`, `tool_result`, turn hooks, and `agent_settled`. Persist start/completion boundaries and evidence references; rate-limit or drop intermediate updates under load. A tool-reported path/line is logical focus, not a literal editor cursor.

ACP tool locations can enable Zed follow-along for current paths and lines. Literal cursors, selections, unsaved buffer operations, or participant avatars require an explicit Zed client/extension capability and must retain the originating human or agent UUID. Never infer a cursor from the most recently modified file.

Exit condition: several Herdr agents can produce simultaneously visible, correctly labeled path/tool/activity streams in Zed without adding high-rate noise to the journal or model context.

### Phase 5.4 — Watchman transport and coverage

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

### Phase 5.5 — Baseline bootstrap, continuity, and replay

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

### Phase 5.6 — External-change events and conservative correlation

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

Buffer events overlapping active intents until `intent.end` or a bounded timeout. Timing overlap, process lifetime, an active ACP human, or Watchman state metadata alone never proves authorship. If several intents could explain the same transition, record ambiguous-known correlation; if the final state differs from the instrumented after snapshot, record unknown or mixed evidence rather than suppressing it.

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

### Phase 5.7 — Observed-baseline preflight protection

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

### Phase 5.8 — Destructive-operation admission and human overrides

Evolve `intent.begin` into an atomic policy decision:

```text
grant | wait | warn | block
```

Classify restore/reset/clean/delete and workspace-wide VCS operations by blast radius. Parse only command forms that can be classified safely; shell metacharacters, compound commands, unknown flags, or unbounded paths become broad/unknown scope rather than receiving optimistic parsing.

At minimum cover `rm`, recursive moves/deletes, `git restore/checkout/reset/clean/stash`, and JJ restore/abandon/working-copy or operation-log transitions documented in [the JJ-native workflow roadmap](workflow-roadmap.md). The daemon evaluates active actors/intents, workspace scope, external observations since the actor baseline/checkpoint, and operation ownership before deciding.

A human override is a separate durable event, not a boolean on the blocked attempt. It must include human actor UUID, blocked decision/event ID, exact operation fingerprint and scope, reason, expiry or one-shot use, and resulting outcome. Reject reuse for changed arguments, paths, workspace, or generation. Unknown actors and unbound clients cannot override. Zed ACP may present the decision, but the daemon owns authorization and audit.

Exit condition: destructive shared-workspace operations cannot execute through the Pi adapter without a deterministic grant or matching one-shot human override, and replay explains both the original block and override use.

### Phase 5.9 — Richer collision evidence and adapter authorization

Add richer evidence incrementally without replacing exact-path facts:

1. directory/path-prefix overlap for recursive or generated operations;
2. configured generated-output and lockfile relationships;
3. normalized WorkUnit scope overlap; and
4. optional LSP symbol/range relationships with source/version metadata.

Store collision evidence as normalized rows with evidence kind, paths/symbols, confidence, and source version. Generated/symbol evidence may warn or escalate but must not silently become authorship or semantic truth.

Separate adapter-declared capabilities from daemon-effective authorization. Validate the Phase 5.0 registration credential (or a later authenticated persistent connection), reject cross-actor mutation/message attempts, and authorize methods by actor kind plus effective capabilities such as:

```text
session.register  mailbox.receive  message.send
activity.report   file.observe     file.preflight
human.override    collision.block  turn.steer
```

Owner-only socket permissions remain the local transport boundary, but capability claims alone do not grant authority. Every denied privileged attempt and effective-policy change is auditable.

Exit condition: future adapters can participate at an explicitly bounded compatibility level without impersonating another actor or acquiring human/destructive authority by self-declaration.

### Phase 5 research basis

Implementation should verify the installed versions rather than freezing assumptions. Primary references used for this plan:

- [Zed External Agents](https://zed.dev/docs/ai/external-agents)
- [ACP overview](https://agentclientprotocol.com/protocol/v1/overview), [transports](https://agentclientprotocol.com/protocol/v1/transports), [session setup](https://agentclientprotocol.com/protocol/v1/session-setup), [prompt turns](https://agentclientprotocol.com/protocol/v1/prompt-turn), [tool calls](https://agentclientprotocol.com/protocol/v1/tool-calls), [filesystem methods](https://agentclientprotocol.com/protocol/v1/file-system), and [extensibility](https://agentclientprotocol.com/protocol/v1/extensibility)
- [Watchman `watch-project`](https://facebook.github.io/watchman/docs/cmd/watch-project), [`subscribe`](https://facebook.github.io/watchman/docs/cmd/subscribe), [clocks](https://facebook.github.io/watchman/docs/clockspec), [queries](https://facebook.github.io/watchman/docs/cmd/query), [configuration](https://facebook.github.io/watchman/docs/config), [query synchronization](https://facebook.github.io/watchman/docs/cookies), [installation](https://facebook.github.io/watchman/docs/install), and [recrawl/poison troubleshooting](https://facebook.github.io/watchman/docs/troubleshooting)
- installed Pi extension and RPC documentation for blocking `tool_call`, `tool_execution_*`, `sendMessage`, settlement, RPC UI degradation, and strict JSONL framing
- [the canonical prototype retrospective](retrospective.md) and [the JJ-native workflow roadmap](workflow-roadmap.md)

## Deferred — ACP golden-path collision handling

Once the Zed ACP buffer bridge, observed baselines, and deterministic mutation admission are proven, make collaborative reconciliation the golden path for ordinary file contention. This changes collision behavior without replacing Agent Bridge authority:

```text
same-buffer overlap
  -> current compatible edits       -> apply collaboratively
  -> stale but uniquely rebasable   -> rebase and apply
  -> concurrent compatible edits    -> serialize through the editor
  -> ambiguous/destructive/semantic -> negotiate, block, or ask the human
```

Agent Bridge remains authoritative for actor identity, buffer observations, expected baselines, admission ordering, ownership, durable evidence, and escalation. ACP is the shared-buffer transport and editor actuator. Zed cursor/tool-location UI is a rebuildable projection and never becomes authorship or policy truth.

The daemon should return a structured contention outcome such as:

```text
auto_merged | rebased | serialized | waiting | negotiating | blocked | resolved
```

For an ACP-backed exact edit, do not emit a stop-and-talk collision merely because another actor touched the same path. First verify the observed buffer version/hash, attempt the exact edit against the current buffer, and admit it when the target remains unique and peer work is preserved. Journal the decision, participating actors, baseline/version evidence, transformed ranges where available, and resulting snapshot hash without storing buffer contents by default.

Escalate to the durable collision lifecycle when reconciliation is ambiguous, ranges overlap incompatibly, a whole-file/destructive operation would discard work, generated/runtime resources conflict, a semantic disagreement remains despite a technically mergeable result, ACP is unavailable, or Watchman observes an unattributed state transition. Shell, Git/JJ, database, process, and cross-workspace conflicts remain Agent Bridge policy even when file buffers are collaborative.

Prerequisites:

1. a validated always-on editor bridge with bounded `fs/read_text_file` and `fs/write_text_file` routing;
2. explicit actor observations and stale-baseline protection;
3. atomic daemon admission and one-shot human override;
4. buffer/range/version evidence sufficient to prove peer work was preserved; and
5. measured false-merge, rebase-failure, negotiation, and fallback rates.

Exit condition: two instrumented actors can edit one Zed buffer without routine interruption or silent overwrite, while every non-mechanical conflict still produces a replayable Agent Bridge decision and safe escalation.

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
- broad cross-harness expansion beyond the bounded Zed ACP bridge before the Pi workflow is solid;
- cloud upload of provenance or semantic checkpoints by default; and
- a standalone second chat UI disconnected from the existing Herdr and Zed surfaces.
