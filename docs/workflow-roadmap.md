# JJ-native workflow roadmap

## Status and relationship to the primary roadmap

This is the secondary workflow roadmap for making Agent Bridge and Pi feel native to Jujutsu rather than treating JJ as safer Git. It chronicles the research and design decisions behind the workflow, then turns them into an implementation sequence.

The canonical product roadmap remains [roadmap.md](roadmap.md). This document is narrower:

- same-workspace multi-agent execution;
- semantic JJ change boundaries;
- snapshots and recovery;
- mutation admission and stale-write protection;
- handoff and work ownership; and
- the shrinking boundary of Workspace Forge/WTF.

Promote work from this document into the primary roadmap as implementation commitments become concrete.

## Research contact

The Pi session that produced the initial research and this roadmap is currently reachable through Agent Bridge as:

```text
name / alias: @chronicler
canonical:    @01a02750-a75e-7a0f-b121-ab1df9786dbd
JJ workspace: default
JJ change:    mospuyszsrtrtzvnlyrrkvspkoyzxyyw
```

The canonical session UUID address remains unambiguous while the session is known to Agent Bridge. The alias is workspace-scoped and useful while this actor is active. Future agents should prefer the canonical address when recording durable references.

Research artifacts are retained locally under:

```text
.yosoi/research/20260822T024029Z-jujutsu-vcs-workflows-for-autonomous-coding-agents-parallel-work/
```

The packet contains the full findings, source map, MVP plan, rollout plan, limitations, search results, and fetched evidence.

## Executive direction

The desired workflow is:

```text
parallel agent
  -> Agent Bridge observes and admits work
  -> agent works in the current JJ workspace when safe
  -> Agent Bridge coordinates ownership, snapshots, handoffs, and recovery
  -> WTF creates another workspace only when physical isolation is required
```

Workspace Forge should become an **isolation actuator selected by Agent Bridge policy**, not the default coordination model.

JJ supplies:

- a working-copy commit;
- stable change identity across rewrites;
- mutable local history;
- first-class conflicts;
- an operation log; and
- multiple working copies over one repository.

Agent Bridge supplies what JJ does not:

- actor identity;
- live intent;
- per-tool causality;
- ownership and leases;
- mutation admission;
- messaging and negotiation;
- semantic work units; and
- provenance-aware recovery.

Together they can support agents behaving like coordinated processes in one development environment instead of isolated jobs that must always be merged later.

## Research chronicle

### 1. The initial problem

The original Pi operating rule was conservative but shallow:

- run `jj status` before editing;
- do not create a change per Pi turn;
- ask before `restore` or `abandon`; and
- report status and diff statistics afterwards.

That added ceremony without exploiting JJ's model. It did not help Pi decide when to create a change, how to preserve experiments, how to shape history, how to recover safely, or how to coordinate multiple agents sharing the repository.

### 2. Official JJ invariants

Research against the official JJ documentation established these invariants:

1. The working copy is a commit, with one working-copy commit per workspace.
2. Most JJ commands snapshot changed working-copy contents before operating.
3. A change ID identifies a logical change as it evolves; commit IDs identify particular rewritten instances.
4. The operation log records repository views, including workspace working-copy pointers.
5. JJ accepts lock-free concurrent repository operations and merges divergent operation views.
6. This repository-level concurrency does not serialize simultaneous writes to the same physical files.
7. Rewriting a change checked out by another workspace can make that working copy stale.
8. Conflicted commits are valid repository states; command success does not guarantee a conflict-free result.
9. Operation-log undo, revert, and restore are local and recoverable, but they affect shared repository state rather than one agent's private history.
10. Pushes, untracked-file deletion, processes, databases, package scripts, and other external effects remain outside JJ rollback.

Primary sources:

- <https://docs.jj-vcs.dev/latest/working-copy/>
- <https://docs.jj-vcs.dev/latest/operation-log/>
- <https://docs.jj-vcs.dev/latest/technical/concurrency/>
- <https://docs.jj-vcs.dev/latest/conflicts/>
- <https://docs.jj-vcs.dev/latest/git-compatibility/>

### 3. Community JJ-agent patterns

The strongest recurring pattern was **snapshot often, curate later**:

- force snapshots after meaningful agent activity;
- preserve execution history without making each tool call a semantic commit;
- use change IDs for durable references;
- shape verified work with `describe`, `split`, `squash`, and `rebase`; and
- protect live workspaces from cross-workspace rewrites.

Notable prior art:

- `jj-sensei`: version-matched knowledge, live status, serialized post-tool snapshots, `evolog` reconstruction, guarded recovery, and workspace-aware immutability. <https://github.com/msmorgan/jj-sensei>
- `agentic-jj`: a detailed catalog of LLM failure modes around editor prompts, revsets, stale hashes, squash direction, peer snapshots, and operation-log restore. <https://github.com/ulisten-ai/agentic-jj>
- `jujutsu-skill`: describe-first and atomic-change guidance. <https://github.com/danverbraganza/jujutsu-skill>
- `jj-hunk`: stable, non-interactive hunk IDs for agent-driven split, commit, and squash. <https://github.com/laulauland/jj-hunk>
- `jj-kata`: guarded `start -> refresh -> integrate -> drop` workspace lifecycle. <https://github.com/msmorgan/jj-kata>
- `workspace-jj`: wave-based fan-out, review gates, and change-ID fan-in. <https://github.com/muloka/claude-plugins/tree/main/plugins/workspace-jj>

`jj-sensei` is especially relevant, it has a MIT liscense!

### 4. Same-workspace prior art

Most coding-agent products default to worktrees because coordination is weak:

- Claude and Cursor isolate parallel sessions in separate checkouts.
- Claude Agent Teams still advises avoiding same-file edits because teammates can overwrite one another.
- MCP Agent Mail adds identities, messaging, advisory reservations, and TTLs, but agents must participate voluntarily.
- a File Lock Coordinator prototype transparently serializes edits in hooks without spending model context.

GitButler demonstrates the complementary direction: multiple active logical branches in one shared working directory, with direct file/hunk routing and worktrees reserved for incompatible runtime or checkout state.

Sources:

- <https://code.claude.com/docs/en/worktrees>
- <https://code.claude.com/docs/en/agent-teams>
- <https://cursor.com/docs/configuration/worktrees>
- <https://docs.gitbutler.com/ai-agents/parallel-agents>
- <https://mcpagentmail.com/showcase>
- <https://github.com/KasparOrange/file-lock-coordinator>

The opportunity for Agent Bridge is to make same-workspace coordination automatic and causal rather than voluntary.

## The decisive JJ constraint

One physical JJ workspace has one physical working-copy commit, `@`.

Multiple agents in the same directory therefore co-author the same `@`. They cannot independently run `jj new`, `jj edit`, `jj prev`, `jj next`, broad `restore`, or similar commands and assume those transitions are private. Those commands can change the tree and working-copy pointer beneath every peer.

The Agent Bridge model must therefore be:

```text
JJ repository
  └── physical JJ workspace and shared @
      └── Agent Bridge work units
          ├── objective and acceptance criteria
          ├── participating actors
          ├── path/directory/symbol scope
          ├── mutation and read evidence
          ├── tests and decisions
          ├── handoff state
          └── eventual JJ change/hunk assignment
```

Two useful modes follow:

### Co-author mode

Several agents deliberately contribute to one semantic JJ change. The change belongs to the mission or work unit, not one session.

### Virtual work-unit mode

Agents own separate logical work while their edits are simultaneously present in shared `@`. Agent Bridge preserves ownership and evidence until the work can be partitioned into JJ changes at a synchronized boundary.

A work unit spans Pi turns. A Pi turn, tool call, snapshot, description, and semantic change are distinct concepts.

## Current implementation baseline

The current Pi adapter and daemon already:

- register sessions and heartbeat leases;
- report Git repository/worktree identity plus JJ workspace/change identity;
- refresh VCS identity every ten seconds;
- use `jj --ignore-working-copy` for passive, non-snapshotting inspection;
- record mutation intent before recognized tools execute;
- record file metadata and hashes before and after mutations;
- record turn, compaction, session, collision, and mutation provenance;
- provide immutable checkpoints, WorkUnits, Directions, and local ticket context;
- preserve explicit parent/launch/child provenance and optional WorkUnit attachment;
- observe conservative external changes through Watchman;
- detect recent exact-path intent overlap;
- push durable collision messages; and
- support `open -> negotiating -> yielded -> resolved` collision state.

The current adapter does not yet:

- force JJ snapshots after edits;
- record operation IDs or evolution-log checkpoints;
- track file reads and observed baseline hashes;
- block a stale whole-file write;
- serialize exact-path mutation attempts;
- classify most mutating JJ commands;
- associate WorkUnits with normalized JJ change relations;
- synchronize workspace-wide JJ transitions; or
- provide provenance-aware rewind.

Most importantly, `intent.begin` currently opens and reports a collision but does not stop both tool calls from executing. Collision detection is preflight observation, not yet admission control.

Relevant implementation surfaces:

```text
packages/pi-extension/index.ts       lifecycle hooks, preflight, delivery, UI
packages/pi-extension/intent.ts      mutation classification
packages/pi-extension/jj.ts          passive JJ identity inspection
packages/pi-extension/provenance.ts  file snapshots and hashes
packages/pi-extension/protocol.ts    adapter protocol types
internal/state/engine.go              actors, intents, collisions, messages
internal/state/scope.go               repository/workspace authority
internal/server/server.go             RPC boundary
internal/provenance/db.go             deterministic read model
docs/vcs.md                           current VCS identity contract
skills/agent-bridge/                  lazy operational guidance
```

## Ownership boundaries

### Base prompt owns norms

Keep always-on instructions short:

- JJ changes represent semantic outcomes, not turns.
- Shared workspace state is not private.
- Proceed autonomously with verified, local, recoverable work.
- Do not rewrite or restore peer-owned work.
- Coordinate when Agent Bridge reports contention.
- Remote publication and external destructive effects remain explicit boundaries.

The prompt should not contain a full JJ manual, command recipes, lease schemas, or recovery runbooks.

### Skills own operational knowledge

A lazy JJ/Agent Bridge skill should own:

- installed-version command lookup;
- non-interactive command patterns;
- semantic boundary heuristics;
- stale/divergent/conflict diagnosis;
- safe solo and shared-repository recovery;
- change shaping and `jj-hunk` recipes;
- ownership, handoff, and rewind workflows; and
- workspace escalation criteria.

### Pi extension owns harness mechanics

The adapter should own:

- session observation;
- mutation and read preflight;
- compact status UI;
- model-transparent wait/block behavior;
- debounced JJ snapshots;
- conversation-bound checkpoint UX;
- JJ command classification; and
- delivery of policy evidence and peer messages.

### Go daemon owns truth and policy

The daemon should remain authoritative for:

- atomic admission decisions;
- path/directory/work-unit leases;
- ownership and handoff state;
- active actors per workspace and JJ change;
- collision lifecycle;
- checkpoint identities;
- rollback authorization;
- durable audit/provenance; and
- contention-activated semantic claims as a derived read model.

## JJ operation policy

Reversibility is necessary but not sufficient. Classify operations by blast radius.

### Autonomous after referent verification

When no peer state is endangered:

- bounded status, log, diff, and show;
- snapshots;
- `new` and `describe` at semantic boundaries;
- path-based split, squash, and restore;
- local rebase or abandon of clearly owned mutable changes;
- forward repair changes; and
- updating descriptions after reading the existing description.

### Agent Bridge preflight or coordination required

When peers share the workspace or repository:

- any command changing shared `@` or its tree;
- `new`, `edit`, `prev`, or `next` while peers are active;
- rewrite of another workspace's working-copy change or ancestry;
- restore, abandon, squash, or rebase over peer-owned evidence;
- generated outputs, lockfiles, broad formatters, and dependency installation;
- whole-file writes based on stale observations; and
- unknown or broad shell mutation scope.

### Human-root or explicit external boundary

- push and remote bookmark updates;
- `--ignore-immutable`;
- global operation-log restore;
- workspace-directory deletion;
- destructive filesystem operations with unknown/untracked state;
- external database/process/runtime side effects; and
- bypassing Agent Bridge admission.

`jj op restore` must never be the normal agent rewind mechanism. Even `jj undo` and `jj op revert` become unsafe when operations from multiple actors or workspaces are interleaved. Prefer selective tree restoration or forward repair.

## Deferred workflow implementation phases

The phases below describe later JJ/safety integration. They do not supersede the immediate checkpoint substrate in the primary roadmap; checkpoint declaration and deterministic evidence come first.

### Workflow Phase 0 — Guidance and observability

Goals:

- replace the old “JJ is safer Git” rule with semantic-boundary guidance;
- add compact active workspace/change/peer state to Pi UI;
- classify JJ commands without enforcing policy;
- explain why a command would be solo-safe, shared-sensitive, or external; and
- preserve current passive `--ignore-working-copy` polling.

Exit condition: agents understand that `@` is shared workspace state and can see active peers without loading a large manual into every turn.

### Workflow Phase 1 — Atomic mutation admission

Change `intent.begin` from collision reporting into an atomic decision:

```text
grant | wait | warn | block
```

Requirements:

- first exact-path writer receives a lease;
- a second active mutation on that path waits or blocks before execution;
- leases end on `tool_result`, cancellation, session death, or TTL expiry;
- decisions and overrides are journaled;
- disjoint-file edits remain silent and fast; and
- collision negotiation remains available when judgment is required.

Exit condition: two Pi agents cannot silently execute simultaneous writes to one path.

### Workflow Phase 2 — Observed-baseline protection

Restore the strongest lesson from the first prototype without reviving watcher-based authority:

1. record the hash an actor read;
2. attach the expected baseline to mutation preflight;
3. compare it to the current file state;
4. distinguish known peer writes from unexplained changes; and
5. block high-confidence stale overwrite attempts.

Built-in exact-text `edit` already provides partial compare-and-swap semantics because the old text must match uniquely. Whole-file `write` needs an explicit baseline check. Arbitrary shell writers require conservative classification or workspace escalation.

Exit condition: a whole-file write based on content another actor has since changed cannot silently overwrite it.

### Workflow Phase 3 — JJ execution checkpoints

Keep passive identity polling separate from explicit snapshots.

At debounced boundaries such as:

- `agent_settled`;
- pre-compaction;
- collision resolution;
- accepted handoff;
- meaningful test-batch completion; and
- session shutdown;

perform:

1. acquire a short daemon workspace lock;
2. force a JJ working-copy snapshot;
3. record operation ID, change ID, commit ID, actor generation, turn, workspace, journal range, mutation references, and test evidence; and
4. suppress duplicate UI/context output if state did not change.

A checkpoint preserves execution state. It does not create a semantic boundary by itself.

Exit condition: every meaningful settled interval can be tied to deterministic Agent Bridge evidence and a JJ snapshot without one commit per tool or turn.

### Workflow Phase 4 — Semantic work units and synchronized changes

Add a minimal durable resource:

```text
work_unit {
  id
  objective
  acceptance
  repository_id
  workspace_id
  jj_change_id
  actors
  scopes
  state
  evidence_refs
}
```

Lifecycle:

```text
proposed -> active -> blocked/yielded -> verified -> completed
                         \-> handed_off
                         \-> abandoned
```

Rules:

- the work unit, not the Pi turn, owns the semantic change;
- multiple actors can co-author one work unit;
- separate logical work can coexist as virtual work units inside shared `@`;
- `jj new` in a shared workspace is a synchronized transition after active mutations settle; and
- the daemon updates all actors and emits the new workspace/change identity.

Exit condition: agents can naturally continue, complete, or hand off semantic work without making VCS boundaries chat-turn boundaries.

### Workflow Phase 5 — Deterministic change shaping

Provide high-level, structured VCS operations instead of requiring models to choreograph raw history surgery.

Candidate `jj_change` capabilities:

- inspect current semantic work units;
- list stable file/hunk IDs;
- materialize a work unit into a JJ change;
- split selected files or hunks;
- squash a verified follow-up into its semantic parent;
- preserve or explicitly replace descriptions;
- verify end-state trees after rewrites; and
- refuse operations touching live peer ownership.

Evaluate `jj-hunk` behind an adapter for stable non-interactive hunk selection. Query installed-version JJ documentation rather than freezing command assumptions in the prompt.

Exit condition: an agent can turn mixed shared `@` work into reviewable semantic changes through bounded, validated operations.

### Workflow Phase 6 — Handoff and Bridge-aware rewind

Handoff should be explicit and durable:

```text
handoff.propose
  -> handoff.accept
  -> ownership transfer
  -> evidence packet delivered
  -> provenance remains connected
```

A handoff packet should include:

- objective and acceptance criteria;
- current paths/work units;
- open collisions;
- latest mutation and checkpoint references;
- current JJ identity;
- latest compaction/settlement summary; and
- recommended tests or next actions.

Safe rewind flow:

1. choose a conversation/provenance checkpoint;
2. compute mutations to invert;
3. detect later foreign mutations on affected paths or hunks;
4. selectively restore or apply an inverse only when peer work is preserved;
5. otherwise block and negotiate or produce a reviewable patch;
6. record the recovery as a new forward operation; and
7. navigate conversation state only after code restoration succeeds, unless the human chooses conversation-only rewind.

Exit condition: an agent can recover its own work without global operation-log surgery or silent removal of later peer edits.

### Workflow Phase 7 — Shrink the WTF boundary

Initially select a shared workspace only when:

- Agent Bridge is healthy;
- all writers are instrumented;
- tasks share a compatible baseline;
- filesystem/runtime state can be shared;
- scopes are disjoint or deliberately collaborative; and
- mutations can pass admission and baseline checks.

Escalate to a WTF/JJ workspace when:

- agents need different checkout states;
- agents are attempting competing solutions;
- dependencies, ports, databases, caches, dev servers, or generated outputs conflict;
- mutation scope is broad or unclassifiable;
- an external tool bypasses hooks;
- Agent Bridge is unavailable; or
- risky experimentation needs a physical boundary.

Then progressively add resource leases for:

- ports;
- dev servers;
- test runners;
- databases;
- caches;
- package installs;
- generators; and
- build outputs.

Each resource made safely shareable removes another reason to create a workspace.

Exit condition: WTF is invoked because policy found a real isolation requirement, not merely because a second agent exists.

### Workflow Phase 8 — Optional semantic enrichment (deferred)

This phase is not a prerequisite for the core work-unit/checkpoint workflow. If revisited later:

- keep agent- and human-authored work-unit state authoritative;
- treat model extraction as an optional sidecar;
- cite deterministic journal/checkpoint evidence;
- keep enrichment bounded and regenerable; and
- never treat model output as coordination truth.

Do not add contention admission, smart triggers, extraction queues, or retrieval injection until the deterministic workflow has demonstrated a need for them.

## Shared workspace versus WTF decision table

| Condition | Shared workspace | WTF/JJ workspace |
| --- | --- | --- |
| Same baseline, disjoint instrumented files | preferred | unnecessary |
| Same semantic change, coordinated contributors | preferred | optional |
| Same file, serialized edits with fresh baselines | possible | safer fallback |
| Same file, competing approaches | avoid | preferred |
| Different dependency or checkout state | avoid | required |
| Shared dev server and compatible app state | preferred | unnecessary |
| Separate ports/databases/caches required | future resource policy | preferred today |
| Broad formatter or generator | only with declared scope/lease | preferred today |
| Arbitrary external editor or unclassified shell writer | conservative | preferred |
| Bridge unavailable | unsafe default | preferred |
| Destructive experiment or global recovery | avoid | preferred plus human review |

## Verification strategy

Build deterministic replay and integration fixtures for:

- disjoint same-workspace edits;
- simultaneous exact-path edits;
- stale whole-file writes;
- an agent dying while holding a lease;
- collision negotiation, yield, and resolution;
- shared-workspace `jj new` while peers are active;
- rewriting another workspace's working-copy change;
- stale workspace detection;
- conflict-producing JJ operations that still return success;
- safe selective rewind with no later peer edit;
- blocked rewind with a later peer edit;
- handoff across compaction;
- Agent Bridge unavailable and WTF escalation; and
- external/unattributed changes.

Useful product metrics:

- stale writes prevented;
- false-positive blocks;
- wait and negotiation duration;
- collisions found before versus after mutation;
- shared-workspace completions versus WTF escalations;
- rollback attempts blocked due to later foreign work;
- stale/divergent workspace incidents;
- commands and tokens required per shaped semantic change; and
- semantic extraction cost only while contention is active.

## Risks and guardrails

### Repository concurrency is not filesystem concurrency

JJ can safely merge divergent repository operations, but two processes can still race on one physical file. Agent Bridge admission is mandatory for a shared-workspace default.

### Recovery can be reversible and still be wrong

Operation-log surgery may be recoverable while disrupting every active workspace. Prefer selective forward repair and make global restore a human-root operation.

### Hash-only provenance cannot yet invert arbitrary edits

Safe hunk-level rewind needs richer range/hunk evidence or privacy-reviewed patch capture. Do not imply exact recovery before the evidence exists.

### Arbitrary tools bypass harness attribution

Filesystem observation should provide conservative external-change evidence, never fabricated authorship. Unknown broad writers should cause escalation.

### Shared filesystem does not imply shared runtime compatibility

Ports, databases, caches, generated outputs, and package installations remain independent isolation dimensions.

### Prompt bloat is not enforcement

Put stable norms in the prompt, recipes in skills, mechanics in the adapter, and authoritative policy in the daemon.

## Immediate next implementation slice

The checkpoint, WorkUnit, Direction, launch-provenance, and external-observation foundations now exist. The next disciplined workflow slice is **atomic mutation admission**, followed by **observed-baseline protection**:

1. change `intent.begin` into an atomic `grant | wait | warn | block` decision;
2. serialize overlapping exact-path mutation attempts while leaving disjoint work silent;
3. expire leases on tool completion, cancellation, actor death, or bounded TTL;
4. record complete file-read observations without treating partial reads as whole-file baselines;
5. block high-confidence stale whole-file writes; and
6. fixture-test both same-workspace success and escalation to a WTF workspace.

This safety slice is the prerequisite for making shared-workspace execution the normal multi-agent default. WTF integration may begin with capability discovery and explicit isolation requests, but policy must continue to choose physical isolation whenever writers are uninstrumented, scopes are broad, or runtime resources conflict.
