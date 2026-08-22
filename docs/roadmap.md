# Roadmap

## Product direction

Agent Bridge is a local coordination control plane for independently launched coding agents. Near-term work should strengthen the Pi vertical slice, preserve deterministic provenance as truth, and add semantic understanding only when concurrent work makes its cost useful.

The next major capability is **contention-activated semantic indexing**: cheap, fast Codex Spark jobs turn bounded session and provenance slices into evidence-linked structured claims while two or more agents are active in the same repository.

## Landed foundation

These roadmap foundations are implemented in the current daemon and Pi adapter:

- [x] Local Go daemon with Unix-socket JSON RPC as coordination authority (`internal/server`, `internal/state`, `internal/protocol`).
- [x] Append-only, fsynced journal with replay and crash-tail recovery (`internal/store`).
- [x] Durable ordered mailboxes with explicit acknowledgement and idempotent sends.
- [x] Actor registration, heartbeat leases, canonical `harness:session` identities, aliases, capabilities, and Git/JJ context.
- [x] Repository/workspace/directory authority scopes.
- [x] Automatic Pi tool-intent reporting and exact-path collision detection.
- [x] Collision lifecycle: `open -> negotiating -> yielded -> resolved`.
- [x] Turso provenance projection with mutation snapshots, session events, turn/compaction context, and deterministic queries.
- [x] Pi provenance tools, mailbox delivery, collision UI, and reconnect/re-register behavior.
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

## Phase 1 — Semantic checkpoint substrate

Build semantic indexing as an asynchronous read model. The coordination journal remains authoritative; model output is derived and may be regenerated.

### Checkpoint identity

Every checkpoint should be tied to deterministic evidence:

- [ ] repository and workspace IDs;
- [ ] actor address and session generation;
- [ ] journal start/end sequence;
- [ ] Pi turn or compaction boundary when available;
- [ ] Git HEAD and optional JJ change/commit identity;
- [ ] mutation, message, collision, and test-result references; and
- [ ] extractor model, prompt, and schema versions.

### Structured output

Spark should propose bounded semantic claims such as:

- [ ] objective;
- [ ] decisions and rationale;
- [ ] constraints;
- [ ] completed work;
- [ ] affected modules;
- [ ] abandoned approaches;
- [ ] unresolved questions; and
- [ ] handoff context.

Each claim should cite deterministic evidence and carry confidence. It must never overwrite observed provenance or be presented as ground truth.

### Durable queue

Use a retry-safe queue keyed by approximately:

```text
repository_id
+ actor
+ session_generation
+ journal_end_sequence
+ checkpoint_kind
```

Requirements:

- [ ] idempotent enqueue and completion;
- [ ] bounded payloads rather than full transcripts;
- [ ] incremental indexing from the prior checkpoint;
- [ ] retry/error visibility;
- [ ] concurrency and budget limits;
- [ ] no coordination-path dependency on model availability; and
- [ ] a model-runner boundary so Codex Spark is the first worker, not daemon infrastructure.

Exit condition: a fixture session can be checkpointed, processed asynchronously by a fake or Spark worker, stored with evidence links, and deterministically queried.

## Phase 2 — Contention-activated indexing

Semantic indexing should consume no model tokens for ordinary serial work.

### Admission policy

```text
fewer than 2 active actors in a repository
  deterministic provenance only
  no Spark jobs

2 or more active actors in a repository
  activate semantic indexing
  perform bounded catch-up
  index settled work incrementally

repository returns below 2 active actors
  flush a final checkpoint
  stop after a grace period
```

Use canonical `repository_id`, not cwd-string equality. Heartbeat-expired sessions do not count toward activation.

### Scope-sensitive behavior

For actors in the same repository but separate workspaces, index lightweight semantic coordination facts:

- objectives and current JJ changes;
- decisions and constraints;
- modules or behavior under change;
- handoffs and peer messages; and
- possible duplicated or incompatible work.

For actors sharing one physical workspace, additionally prioritize:

- exact file mutations;
- stale baselines;
- destructive operations;
- active collisions; and
- yield/resolution state.

### Smart triggers

Do not invoke Spark after every tool event. Debounce and batch work around:

- `agent_settled`;
- collision open or resolution;
- peer message or handoff;
- context compaction;
- meaningful test-batch completion;
- significant JJ mutation boundaries;
- accumulated unindexed-event thresholds; and
- session shutdown.

When a second actor activates indexing, backfill only:

1. the latest checkpoint or compaction for each active actor;
2. subsequent mutations and test results;
3. current Git/JJ identity and declared intent; and
4. unresolved messages and collisions.

Exit condition: one actor working alone causes zero Spark jobs; adding a second actor in the same repository produces bounded catch-up and incremental checkpoints; returning to solo mode stops jobs after a final flush.

## Phase 3 — Lazy semantic retrieval

Once checkpoints exist, local Pi agents already provide the interactive reasoning layer. Avoid building a second conversational product.

Expose compact deterministic queries such as:

```text
explain this session
explain since compaction
explain this JJ change
what decisions led here
what does another active actor need me to know
```

The Agent Bridge skill should lazily load semantic checkpoint tools only for attribution, recovery, handoff, or coordination questions. Answers should combine:

1. observed provenance;
2. evidence-linked extracted claims; and
3. local Pi reasoning over the retrieved packet.

Do not inject the full semantic index into every prompt. Deliver a compact packet only when a collision, handoff, relevant shared-module change, explicit query, or materially changed shared context requires it.

Exit condition: any local Pi agent can explain a session or JJ change from a small retrieved packet without another Spark call and without loading the semantic schema into unrelated turns.

## Phase 4 — Safety work carried forward

Develop these alongside or after the checkpoint substrate, without reviving watcher-based attribution as authority:

- conservative external-change awareness;
- stale-baseline preflight protection;
- workspace-aware admission for restore/reset/clean/delete operations;
- explicit human overrides with audit events;
- richer directory/generated-output/symbol collision evidence; and
- capability and authorization policy for future adapters.

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
- cross-harness expansion before the Pi workflow is solid;
- cloud upload of provenance or semantic checkpoints by default; and
- a second chat UI for explanations Pi can already provide.
