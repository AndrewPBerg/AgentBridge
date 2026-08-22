# Roadmap

## Product direction

Agent Bridge is a local coordination control plane for independently launched coding agents. Near-term work should strengthen the Pi vertical slice, preserve deterministic provenance as truth, and add semantic understanding only when concurrent work makes its cost useful.

The next major capability is an **agent-authored checkpoint and work-unit substrate**: agents can declare meaningful stopping points, preserve evidence-linked task state, and later organize that work into an explicitly ordered workflow. Semantic extraction is optional enrichment, not coordination truth.

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

## Phase 1 — Agent-authored checkpoints

Build checkpoints first as a deterministic, standalone provenance substrate. A checkpoint may be declared by an agent or human and does not require a WorkUnit at creation. Conceptually, a WorkUnit owns or links to its child checkpoints; WorkUnits are simply a later composition layer that adds mutable objectives, lifecycle state, and top-level ordering. The coordination journal remains authoritative; checkpoint records are immutable evidence boundaries.

### Checkpoint declaration

An agent can explicitly declare an internal checkpoint at a meaningful stopping point, and a human can declare one through the same API. The declaration source should be recorded as `agent` or `human`; automatic system boundaries can be added later without changing the model.

### Checkpoint identity and evidence

Every immutable checkpoint should be tied to deterministic evidence:

- [ ] repository and workspace UUIDs;
- [ ] actor address and session generation;
- [ ] journal start/end sequence;
- [ ] Pi turn or compaction boundary when available;
- [ ] Git HEAD and optional JJ change/commit identity;
- [ ] mutation, message, collision, and test-result references; and
- [ ] optional `work_unit_id` linkage; and
- [ ] agent- or human-authored metadata at that boundary.

The declarer may explicitly say, “this is a good stopping point; checkpoint this.” Automatic boundaries such as settlement, compaction, handoff, and shutdown can be added later, but must use the same immutable model.

### WorkUnits and ordering come later

A future WorkUnit will compose checkpoints with an objective, optional policy/context/acceptance/owner/scope, mutable lifecycle state, and handoff metadata. The top-level workflow will order WorkUnits; checkpoints do not define workflow order. Do not build a separate semantic-processing queue yet. Any durable queue for extraction or execution can later be derived from workflow ordering rather than becoming an independent source of truth.

### Derived enrichment

Semantic claims, model summaries, and extraction runs are optional derived data. They may be regenerated, versioned, or discarded without changing the checkpoint or observed provenance. No model availability or semantic processor may sit on the coordination path.

Exit condition: a fixture session can create and replay immutable agent- and human-declared checkpoints, preserve evidence links, and query the latest checkpoint deterministically.

### Immediate implementation slice

1. add the minimal checkpoint protocol and declaration path;
2. capture agent/human declaration source and immutable evidence identity;
3. project deterministic evidence references and latest-checkpoint queries; and
4. add replay, idempotency, and immutability tests.

WorkUnit composition, `/work <objective>`, lifecycle transitions, and top-level ordering follow after the checkpoint substrate. Atomic mutation admission, path leases, and observed-baseline protection remain later safety work.

## Phase 2 — Safety work carried forward

Develop these alongside or after the checkpoint substrate, without reviving watcher-based attribution as authority:

- conservative external-change awareness;
- stale-baseline preflight protection;
- workspace-aware admission for restore/reset/clean/delete operations;
- explicit human overrides with audit events;
- richer directory/generated-output/symbol collision evidence; and
- capability and authorization policy for future adapters.

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
- cross-harness expansion before the Pi workflow is solid;
- cloud upload of provenance or semantic checkpoints by default; and
- a second chat UI for explanations Pi can already provide.
