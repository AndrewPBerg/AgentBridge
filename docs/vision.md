# Agent Bridge

## Summary

Agent Bridge is ambient coordination infrastructure for concurrent coding agents. It gives agents OS-like awareness of one another without requiring every agent to manually call a coordination tool.

The core idea: agents should not treat a repository as inert files. They should treat it as a shared operating environment with other active actors, live intent, soft ownership, and semantic interrupts.

Agent Bridge provides:

- presence: which agents are active
- intent: what each agent is trying to do
- leases: which files/modules/tasks are currently hot
- events: what changed, who changed it, and why
- signals: semantic interrupts such as pause, replan, yield, review, or block
- shared context: task specs, constraints, and acceptance criteria

## Problem

Today, multiple agents working in the same repository are mostly blind to each other.

Common failures:

- Agent B overwrites Agent A's unstaged changes.
- Agent B stashes, resets, reformats, or rewrites files without knowing another agent is active.
- Agents duplicate work because they cannot see each other's goals.
- Git detects conflicts only after damage has already happened.
- Review happens post-hoc instead of during trajectory drift.
- Coordination relies on the human remembering which terminal owns which task.

The filesystem and git are insufficient as coordination layers. They provide persistence and conflict detection, but not live intent, presence, or semantic negotiation.

## Design Principle

Coordination should be ambient, not agent-managed.

Bad model:

```text
agent remembers to call bridge.claim_files()
agent remembers to poll bridge.status()
agent remembers to release files
```

Better model:

```text
runtime/editor/filesystem observes agent activity
bridge infers or records active work
other agents receive automatic signals before destructive action
```

The agent should not have to remember to be polite. The environment should make unsafe concurrency visible, interruptible, and hard to do accidentally.

## Analogy

```text
filesystem    = memory
git           = persistence/versioning
agent bridge  = scheduler + signals + presence + soft locks
Pi/Zed/IDE    = UI/runtime surfaces
```

Agent Bridge is closer to `dbus` plus Unix signals plus file leases than to a normal CLI tool.

## Goals

- Support multiple concurrent agents in the same repo or related worktrees.
- Detect conflicts before destructive edits occur.
- Make active work visible across terminals, editors, and agent sessions.
- Allow semantic push signals into agent workflows.
- Preserve human control and override ability.
- Keep coordination automatic by default.
- Work with existing tools through filesystem/git/process observation.

## Non-Goals

- Not a replacement for git.
- Not a hard distributed lock manager by default.
- Not a full project management system.
- Not an agent memory product.
- Not primarily a reviewer agent, though review sidecars can use it.
- Not dependent on a single editor or agent runtime.

## Core Concepts

### Agent

A process or session performing work in a repository.

Examples:

- Pi coding agent session
- Codex session
- Claude Code session
- editor-integrated agent
- human editor session, optionally represented as an actor

Agent metadata:

```json
{
  "agent_id": "pi-7f3a",
  "kind": "pi",
  "repo": "/home/andrew/project",
  "worktree": "/home/andrew/project",
  "branch": "feature/auth-refactor",
  "task": "Refactor auth boundary without regex heuristics",
  "started_at": "2026-06-21T22:00:00Z"
}
```

### Presence

A live record that an agent is active.

Presence should expire automatically via heartbeat timeout. Crashed sessions should not leave permanent locks.

### Intent

A short declaration of what the agent is trying to accomplish.

Intent can be:

- explicitly supplied by the user/runtime
- inferred from prompts, branch names, issue IDs, or changed files
- updated over time

Intent is not a guarantee. It is coordination context.

### Lease

A soft claim over files, directories, symbols, modules, tests, or tasks.

Examples:

```json
{
  "lease_id": "lease-123",
  "agent_id": "pi-7f3a",
  "scope": { "type": "path", "value": "src/auth/**" },
  "mode": "write",
  "strength": "soft",
  "reason": "Refactoring auth boundary",
  "expires_at": "2026-06-21T22:20:00Z"
}
```

Lease types:

- `read`: agent is inspecting
- `write`: agent is editing
- `review`: agent is reviewing changes
- `test`: agent is running or modifying tests

Lease strengths:

- `observed`: inferred from file access/edit events
- `soft`: warn on conflict
- `hard`: block unless human overrides

Default should be `observed` or `soft`, not hard.

### Event

An append-only fact about the shared environment.

Examples:

```text
agent.started
agent.heartbeat
agent.intent.updated
file.opened
file.modified
file.saved
git.diff.changed
git.branch.changed
lease.created
lease.conflict_detected
signal.sent
signal.acknowledged
agent.finished
```

Events are durable enough for debugging but not necessarily permanent project history.

### Signal

A semantic interrupt delivered to an agent, runtime, editor, or human.

Signals are the push side of Agent Bridge.

Examples:

```text
SIGWARN       non-blocking warning
SIGYIELD      another agent owns this scope; pause or pick different work
SIGREPLAN     current trajectory conflicts with shared intent/spec
SIGASK        human decision needed
SIGBLOCK      do not proceed without override
SIGMERGE      upstream/local changes need reconciliation
SIGFAITH      implementation technically passes but violates stated intent
SIGKILLSEM    terminate current trajectory/session as last resort
```

Signals should include evidence:

```json
{
  "signal": "SIGYIELD",
  "to": "codex-a91c",
  "from": "agent-bridge",
  "severity": "high",
  "reason": "src/auth/session.ts is currently leased for write by pi-7f3a",
  "evidence": [
    "pi-7f3a modified src/auth/session.ts 42s ago",
    "lease reason: Refactoring auth boundary"
  ],
  "recommended_action": "pause and ask user, or work in a separate worktree"
}
```

## Push-Pull Model

### Push

The bridge asynchronously emits signals when it detects risk.

Examples:

- An agent begins editing a file leased by another agent.
- A command attempts `git reset --hard` while another agent has uncommitted changes.
- A diff changes files outside declared task scope.
- A sidecar reviewer detects spec drift.

### Pull

Agents and tools can query current bridge state.

Examples:

```text
Who is active?
What files are hot?
Can I edit src/auth/session.ts?
What changed since I started?
Are there unresolved signals?
```

Pull is useful, but push is the distinctive primitive. The system should not depend on agents remembering to poll.

## Architecture

```text
                 ┌──────────────────┐
                 │  Pi / Agent UI    │
                 └────────┬─────────┘
                          │ signals/status
┌──────────────┐   ┌───────▼────────┐   ┌─────────────────┐
│ filesystem   │──▶│ Agent Bridge    │◀──│ editor adapters │
│ watchers     │   │ daemon          │   └─────────────────┘
└──────────────┘   └───────┬────────┘
                           │
┌──────────────┐           │ events/signals
│ git watchers │──────────▶│
└──────────────┘           │
                           ▼
                   ┌──────────────┐
                   │ event log     │
                   │ state store   │
                   └──────────────┘
```

### Daemon

Local per-user daemon responsible for:

- ingesting events
- maintaining presence/leases/state
- detecting conflicts
- routing signals
- exposing local API/socket

Suggested transport:

- Unix domain socket for local control/API
- append-only JSONL event log for debugging
- SQLite for current state and recent history

### Watchers

Sources of ambient information:

- filesystem events: inotify/fanotify
- git state: branch, index, worktree diff
- process tree: commands run by agent sessions
- editor buffers: optional richer integration
- agent runtime hooks: optional direct integration

### Adapters

Adapters integrate specific environments:

- Pi extension
- VS Code/Zed/Neovim plugin
- shell wrapper
- Codex/Claude Code session shim
- git hook integration

Adapters should improve fidelity but not be required for basic operation.

## Conflict Detection

### Syntactic Conflicts

Detected from paths and git state:

- two agents edit same file
- one agent edits generated file another is regenerating
- one agent runs destructive git command while others are dirty
- branch/worktree changes underneath active agent

### Semantic Conflicts

Detected from intent, ownership, and optional code analysis:

- two agents modify same module through different files
- implementation violates task constraints
- tests are updated to fit bad behavior
- agent edits outside declared scope
- duplicate work on same issue

Semantic detection should be advisory at first. False positives must be cheap to override.

## Safety Model

Default posture: warn and interrupt, do not hard-block.

Escalation ladder:

1. observe only
2. warn
3. ask for acknowledgment
4. require human override
5. block command/edit
6. semantic kill

Hard blocking should be reserved for high-confidence destructive actions, such as:

- `git reset --hard`
- deleting files with active external leases
- force-checkout over another agent's changes
- overwriting files modified after session start

## Example Flows

### Same File Conflict

1. Agent A edits `src/auth/session.ts`.
2. Bridge observes write and creates/refreshes soft lease.
3. Agent B opens and modifies same file.
4. Bridge sends `SIGYIELD` to Agent B.
5. Agent B queues message: `Another agent is actively editing this file. Pause, coordinate, or override?`

### Destructive Git Command

1. Agent B attempts `git stash --all` or `git reset --hard`.
2. Shell/runtime adapter reports command before execution.
3. Bridge sees Agent A has dirty changes in same repo.
4. Bridge emits `SIGBLOCK`.
5. Command is blocked unless human explicitly overrides.

### Spec Sidecar

1. User starts task with spec: `No hardcoded heuristics; preserve parser abstraction.`
2. Code-only reviewer sidecar watches diff snapshots.
3. Sidecar detects implementation that avoids regex but hardcodes parser branches.
4. Sidecar emits `SIGFAITH` through Agent Bridge.
5. Main agent receives interruption: `Stop and replan: current diff technically avoids regex but violates parser abstraction.`

## MVP

### Phase 1: Local Event Bus + File Leases

- Local daemon.
- Per-repo presence registry.
- Filesystem watcher for modified files.
- Git dirty-state watcher.
- Soft file leases inferred from edits.
- CLI/API to inspect active agents and hot files.
- Signals: `SIGWARN`, `SIGYIELD`, `SIGBLOCK`.

MVP commands:

```sh
agent-bridge status
agent-bridge events
agent-bridge watch /path/to/repo
agent-bridge register --agent pi --task "Refactor auth"
agent-bridge signal <agent-id> SIGYIELD --reason "file leased"
```

### Phase 2: Pi Extension

- Auto-register Pi sessions.
- Display active leases/signals in UI.
- Queue monitor messages into the conversation.
- Warn before edits or dangerous commands.
- Let human override with explicit action.

### Phase 3: Semantic Sidecars

- Code-only reviewer sidecar.
- Diff/spec evaluator.
- Symbol/module lease inference.
- `SIGFAITH`, `SIGREPLAN`, `SIGASK`.

### Phase 4: Cross-Tool Ecosystem

- Zed/VS Code/Neovim adapters.
- Codex/Claude Code shims.
- Worktree-aware coordination.
- Shared project dashboards.

## API Sketch

### Register Agent

```http
POST /agents
{
  "kind": "pi",
  "repo": "/home/andrew/project",
  "worktree": "/home/andrew/project",
  "task": "Implement CAS-216 auth refactor"
}
```

### Emit Event

```http
POST /events
{
  "type": "file.modified",
  "agent_id": "pi-7f3a",
  "repo": "/home/andrew/project",
  "path": "src/auth/session.ts"
}
```

### Query State

```http
GET /repos/:repo_id/state
```

Response:

```json
{
  "agents": [],
  "leases": [],
  "signals": [],
  "dirty_files": []
}
```

### Subscribe to Signals

```text
SUBSCRIBE /signals?agent_id=pi-7f3a
```

Could be implemented with Unix socket streaming, SSE, WebSocket, or MCP notifications depending on host runtime.

## Data Model

Minimum tables:

- `agents`
- `heartbeats`
- `events`
- `leases`
- `signals`
- `repos`
- `overrides`

All records should include:

- timestamp
- repo/worktree identity
- actor/agent identity when known
- source adapter
- confidence when inferred

## UX Requirements

- Low-friction: starts automatically with agent/editor/runtime.
- Visible: user can see who owns what.
- Explainable: every signal includes evidence.
- Overrideable: human remains final authority.
- Non-annoying: avoid interrupt spam through debounce and severity thresholds.
- Crash-safe: stale leases expire.
- Privacy-conscious: do not broadcast secrets or full file contents by default.

## Open Questions

- Should leases be repo-global, worktree-local, or both?
- How should human editor activity be represented?
- What is the minimum reliable identity model for agents?
- Can command blocking be implemented safely across shells/runtimes?
- How much semantic analysis belongs in the daemon versus sidecars?
- Should the event log store diffs, hashes, or only paths?
- What is the right override UX for high-confidence destructive actions?

## Risks

- False positives interrupt flow and users disable it.
- Hard locks create deadlocks or reduce agent autonomy.
- Weak identity makes attribution unreliable.
- Editor/runtime adapters fragment behavior.
- Overly broad event capture creates privacy/security risk.
- Agents may learn to satisfy bridge checks while still violating intent.

## Guiding Philosophy

Agent Bridge is not about making agents obedient through another checklist. It is about giving agents a shared operating environment.

Humans already coordinate through awareness: open files, active branches, conversation, visible diffs, and social norms. Agents need machine-native equivalents.

The bridge should make concurrency legible:

```text
who is here?
what are they doing?
what do they own?
what changed?
what should stop now?
```

Once those primitives exist, review agents, spec monitors, semantic kill switches, and collaborative IDE workflows become natural extensions rather than separate hacks.

## Current Runtime Direction

The likely implementation shape is:

```text
agent-bridge/                 Go daemon + CLI
  cmd/agent-bridge/            user-facing CLI
  cmd/agent-bridged/           daemon entrypoint, if split
  internal/workspace/          repo/workspace buses
  internal/events/             append-only event ingestion
  internal/leases/             file/symbol/task leases
  internal/signals/            semantic signal routing
  internal/git/                git/worktree observation
  internal/watch/              filesystem/process watchers
  internal/protocol/           JSON protocol over Unix socket
  packages/pi-extension/       TypeScript Pi adapter
```

### Why Go

Go is the default choice for the daemon because Agent Bridge is mostly a local systems coordination problem, not a frontend problem.

Go is good for:

- long-running local daemons
- many concurrent watchers via goroutines
- channels/event loops that match the bus mental model
- subprocess supervision
- Unix sockets
- file watching
- simple static binary distribution
- predictable operational behavior

The phrase "virtual process management" should not be overinterpreted. The useful thing is simpler: Go makes it easy to run lots of lightweight concurrent tasks that behave like little supervised actors:

```text
workspace actor
  ├─ fs watcher goroutine
  ├─ git watcher goroutine
  ├─ heartbeat expiry goroutine
  ├─ signal router goroutine
  ├─ socket client goroutines
  └─ event persistence goroutine
```

This maps cleanly onto the Agent Bridge model.

### Why TypeScript Still Matters

Pi is TypeScript-native, so the Pi integration should be a TypeScript extension.

The extension should be thin:

- register Pi session presence on `session_start`
- unregister/expire on `session_shutdown`
- intercept edit/write/bash tool calls before execution
- report tool calls/results to the Go daemon
- receive bridge signals over a socket
- inject interrupts with Pi's steering/follow-up message APIs
- display status/widgets in the Pi UI

The TypeScript layer should not own coordination truth. It should adapt Pi to the bridge.

### Not Shell, Not Rust First

Shell is useful for spikes and wrappers, but a bad fit for a persistent bus with state, sockets, watchers, and conflict policy.

Rust would be excellent for correctness and systems-level control, but it is probably slower to iterate on for this specific product. Agent Bridge needs design discovery more than maximum performance.

C is unnecessary unless this becomes a kernel/filesystem experiment.

## Bus Topology

Agent Bridge should not start as a machine-global bus that tries to understand everything. Coding agents work inside projects. The primary coordination unit should be the workspace.

```text
user registry / global discovery
  └─ workspace bus: git repo root or explicit workspace root
       └─ session endpoints: pi, codex, claude, opencode, editor, human
```

### User Registry

A small user-level registry tracks active workspace buses.

Responsibilities:

- discover active workspaces
- route clients to the right workspace socket
- clean up dead workspace daemons
- answer global questions like "what agents are active anywhere?"

It should not own detailed repo coordination.

### Workspace Bus

The workspace bus is the source of truth for a repo or project directory.

Responsibilities:

- active agents
- current intent per agent
- file/symbol leases
- dirty files
- recent diffs
- semantic signals
- conflict detection
- overrides

Workspace root resolution:

1. explicit `--workspace <path>`
2. git root from `git rev-parse --show-toplevel`
3. nearest `.agent-bridge-root` marker
4. current working directory

Never silently choose `$HOME` as a workspace root. That would create a giant accidental bus.

### Session Endpoint

Each agent/editor session is an endpoint attached to a workspace bus.

Session endpoints are ephemeral. They heartbeat, receive signals, and expose their capabilities.

Example capabilities:

```json
{
  "agent_id": "pi-7f3a",
  "can_receive_steer": true,
  "can_block_tool_calls": true,
  "can_block_shell_commands": true,
  "can_show_ui": true,
  "can_apply_patches": false
}
```

## Deep-Dive Idea Bank

This section is intentionally expansive. Many ideas here are too much for v1, but they define the larger design space.

Use the tags:

- `NOW`: plausible early product feature
- `NEXT`: likely useful after the daemon/adapter exists
- `LATER`: bigger feature, needs adoption
- `MOONSHOT`: weird but strategically interesting
- `DANGER`: powerful but easy to make annoying or unsafe

### 1. Agent Air Traffic Control

`NEXT`

Treat each workspace like controlled airspace.

Agents file a lightweight flight plan:

```text
agent: pi-7f3a
mission: refactor auth parser
expected paths: src/auth/**, tests/auth/**
altitude: high-risk refactor
runway: branch feature/auth-parser
```

The bridge acts like a tower:

- warns about intersecting flight paths
- asks an agent to hold short before editing hot files
- reroutes agents to separate modules
- declares temporary no-fly zones around fragile files
- escalates when an agent flies outside its declared mission

The point is not rigid locking. It is shared situational awareness.

### 2. Semantic Signals as First-Class OS Signals

`NOW`

Unix signals are too low-level for agent behavior. Agent Bridge should define semantic signals that describe intent-level interruptions.

Possible signal vocabulary:

```text
SIGOBSERVE    FYI only; update your mental model
SIGWARN       low-risk issue detected
SIGYIELD      stop touching this scope; another actor is active
SIGREBASE     your baseline is stale; reconcile before proceeding
SIGMERGE      merge/review another actor's changes
SIGASK        ask the human before choosing
SIGREPLAN     stop implementation and make a new plan
SIGFAITH      current approach violates the spirit of the spec
SIGSCOPE      you are editing outside declared task scope
SIGTEST       tests are insufficient or suspiciously tailored
SIGDANGER     command/edit is destructive
SIGBLOCK      bridge/runtime blocked action pending override
SIGKILLSEM    terminate current trajectory/session
```

Signals should be structured objects, not strings only:

```json
{
  "type": "SIGFAITH",
  "severity": "high",
  "confidence": 0.82,
  "scope": ["src/parser/*", "tests/parser/*"],
  "claim": "The implementation avoids regex but hardcodes parser cases, violating the abstraction requirement.",
  "evidence": [
    { "path": "src/parser/router.ts", "line": 81, "summary": "new branch checks literal provider names" },
    { "path": "tests/parser/router.test.ts", "summary": "test mirrors literals instead of exercising parser contract" }
  ],
  "recommended_action": "pause and replan before further edits"
}
```

### 3. File Leases That Feel Like Editor Presence

`NOW`

IDE collaboration works because presence is visible. Agent Bridge should make agent presence around files feel similarly obvious.

A file can be:

```text
cold        no known activity
warm        recently read/opened
hot         recently modified
claimed     explicitly leased
contested   two agents are active nearby
quarantined blocked pending human review
```

This could appear in a Pi widget:

```text
Agent Bridge: auth/session.ts HOT by pi-7f3a · parser/** CLAIMED by codex-a91c
```

This is social coordination translated into machine state.

### 4. Symbol-Level Leases

`NEXT`

Path-level ownership is crude. Agents often conflict semantically without editing the same file.

Example:

- Agent A edits `AuthSession` in `src/auth/session.ts`.
- Agent B edits `createSession()` tests in `tests/auth/session.test.ts`.
- Agent C edits docs for the same behavior.

A symbol lease could track:

```json
{
  "scope": {
    "type": "symbol",
    "language": "typescript",
    "name": "AuthSession.create",
    "definition": "src/auth/session.ts:42"
  }
}
```

This needs language-aware indexing. It can start by integrating external tools later rather than being core to v1.

### 5. Intent Diff

`NEXT`

Normal diff asks: what code changed?

Intent diff asks: how does observed work differ from declared intent?

Inputs:

- user objective
- agent plan
- changed files
- patch summary
- tests changed
- bridge events

Output:

```text
Declared intent: preserve parser abstraction, no heuristics.
Observed diff: added provider-specific branches in parser router.
Mismatch: satisfies literal "no regex" constraint while violating abstraction goal.
Signal: SIGFAITH
```

This is the philosophical heart of the reviewer/monitor idea.

### 6. Spec Ledger / Constitution

`NEXT`

Each workspace or task can have an active spec ledger:

```yaml
objective: Refactor parser without changing behavior.
constraints:
  - no hardcoded provider-specific branches
  - no regex-based parsing
  - preserve public API
  - tests must exercise behavior, not implementation details
acceptance:
  - existing tests pass
  - parser contract tests cover new path
  - no unrelated formatting churn
```

The bridge should treat this as a living constitution.

Agents and sidecars can cite ledger clauses when emitting signals:

```text
SIGFAITH: violates constraints[0] "no hardcoded provider-specific branches"
```

This avoids vague reviewer comments.

### 7. Bridge as Immune System

`MOONSHOT`

Model the workspace as an organism and agents as cells/processes. The bridge is the immune system.

Immune responses:

- inflammation: mark area hot/contested
- antibodies: add targeted tests around risky behavior
- quarantine: isolate suspicious diff
- fever: increase review strictness under repeated failures
- apoptosis: terminate a bad trajectory
- memory: remember past failure patterns for this repo/agent/model

This sounds ridiculous, but it creates useful product language:

```text
quarantine this diff
raise immune response on auth module
this agent has triggered three faithfulness antibodies
```

### 8. Quarantine Diffs

`NEXT` / `DANGER`

Instead of only warning, the bridge could quarantine untrusted changes.

Modes:

- normal: edits write directly to worktree
- quarantine: edits go to a shadow patch
- review: human/sidecar approves patch into worktree
- reject: patch is discarded

This prevents agents from dirtying the real repo when confidence is low.

Implementation options:

- git worktree per agent
- overlay filesystem
- patch capture from tool calls
- editor buffer isolation

This is powerful but likely too heavy for v1.

### 9. Copy-on-Write Agent Sandboxes

`MOONSHOT`

Every agent thinks it is editing the repo, but it is actually editing a private overlay. The bridge can merge, reject, or compare overlays.

Benefits:

- no accidental overwrites
- easy A/B comparison between agents
- agents can race on different solutions
- human can promote winning diff

Problems:

- hard to integrate with arbitrary tools
- filesystem overlays are OS-specific
- test/build behavior may differ subtly
- user mental model may get confusing

This is agent-native branching beyond git.

### 10. Tournament Mode

`MOONSHOT`

Run multiple agents against the same task in isolated sandboxes, then have the bridge compare outputs.

Flow:

1. User gives spec.
2. Bridge spawns 3 agents in separate worktrees/overlays.
3. Agents cannot see each other's work initially.
4. Bridge runs tests/static checks/spec review.
5. Reviewer sidecar ranks diffs.
6. Human chooses or asks bridge to synthesize.

This turns multi-agent parallelism from chaos into controlled competition.

### 11. Agent Pair Programming / Huddles

`LATER`

When conflict is detected, agents can be forced into a short huddle.

Example signal:

```text
SIGHUDDLE: pi-7f3a and codex-a91c are editing the same abstraction. Summarize your intent in <=5 bullets and wait.
```

The bridge then collects:

- Agent A intent
- Agent B intent
- conflicting scopes
- proposed division of labor

Then either:

- asks human
- assigns ownership
- tells one agent to yield
- creates two separate worktrees

This is social coordination for machines.

### 12. Repo Weather Map

`NEXT`

A visual or CLI dashboard that shows repo activity as weather.

```text
agent-bridge radar

src/auth/          STORM    3 agents, 2 contested files
src/parser/        HOT      spec monitor active
src/ui/            CLEAR
migrations/        FROZEN   destructive edits blocked
```

This is not just cute. It gives humans instant situational awareness across five terminals.

### 13. Heat-Based Scheduling

`LATER`

Agents can be nudged toward cold areas.

If two agents are about to work on `src/auth`, the bridge can suggest:

```text
Auth module is hot. Available cold tasks:
- update docs for parser API
- add tests for token refresh
- inspect failing lint in src/ui
```

This turns bridge from passive guard into scheduler.

### 14. Agent Reputation / Trust Scores

`LATER` / `DANGER`

Track how often an agent/model/session causes useful vs harmful events.

Signals:

- caused overwrite warning
- ignored SIGYIELD
- produced reverted diff
- passed review first try
- triggered SIGFAITH
- edited outside scope

Use reputation to tune policy:

```text
trusted agent: warn only
unknown agent: require ack for hot files
reckless agent: quarantine risky edits
```

Danger: this can become noisy pseudoscience. It should be evidence-based and local, not a moral score.

### 15. Capability-Based Agent Permissions

`NEXT`

Agents should not all have equal authority.

Capabilities:

```text
can_read_repo
can_write_files
can_run_tests
can_run_network
can_modify_git_index
can_run_destructive_git
can_edit_protected_paths
can_override_leases
can_emit_blocking_signals
```

A reviewer sidecar might emit `SIGFAITH` but not edit code.
A builder agent might edit code but not override conflicts.
A human is root.

This is Unix permissions translated to agents.

### 16. Protected Paths and Sacred Files

`NOW`

Some paths deserve special policy:

```yaml
protected:
  - .env*
  - package-lock.json
  - migrations/**
  - infra/prod/**
  - generated/**
  - src/auth/**
```

Policy examples:

- `.env*`: never read or write without explicit human confirmation
- lockfiles: warn on unexpected churn
- migrations: require human confirmation
- generated files: route edits to source generator
- auth/security: stricter review threshold

This can be useful before any fancy semantic monitor exists.

### 17. Destructive Command Firewall

`NOW`

The bridge should classify shell commands before they run.

High-risk examples:

```text
git reset --hard
git checkout -- .
git clean -fdx
git stash --all
rm -rf
find . -delete
chmod -R
mv src src_old
npm install with lifecycle scripts
```

The Pi adapter can block tool calls. Shell adapters can wrap commands. The daemon supplies policy.

The key rule:

> A destructive command in a shared workspace is not local. It is an event affecting every active agent.

### 18. Git Operation Negotiation

`NEXT`

Before git operations, agents should negotiate with the bridge.

Examples:

- `checkout`: would this remove another agent's files?
- `stash`: whose changes are being stashed?
- `pull/rebase`: will this invalidate another agent's baseline?
- `commit`: should this include only this agent's leased files?

Possible command:

```sh
agent-bridge git preflight reset --hard
```

But adapters should make this automatic where possible.

### 19. Agent Black Box / Flight Recorder

`NEXT`

Every session should leave a small flight recorder:

- started/stopped
- declared task
- files touched
- signals received
- signals ignored/acknowledged
- commands run
- final diff summary

This is not full transcript storage. It is operational history.

Useful after a bad event:

```text
Why did auth/session.ts get overwritten?
```

Bridge can answer:

```text
codex-a91c edited file 38s after pi-7f3a lease; received SIGYIELD; no ack; then ran git checkout -- src/auth/session.ts; blocked? false because shell adapter absent.
```

### 20. Blame Before Damage

`NEXT`

Git blame is after-the-fact. Bridge blame should be live.

For any dirty line/file:

```text
who touched this?
when?
under what task?
was there an active signal?
which session owns it?
```

This would make concurrent agent work debuggable.

### 21. Event-Sourced Workspace State

`NOW`

The bridge should store append-only events and derive current state.

Benefits:

- easy debugging
- replayable bugs
- deterministic conflict detector tests
- sidecars can subscribe at any time
- state corruption can be rebuilt

Storage:

```text
~/.agent-bridge/events/<workspace-hash>.jsonl
~/.agent-bridge/state/<workspace-hash>.sqlite
```

Events should avoid secrets and full file contents by default.

### 22. CRDT / Shared Buffer Future

`MOONSHOT`

Zed-style collaboration points toward shared buffers, not just file saves.

If editor adapters expose buffer-level operations, the bridge can detect conflicts before disk writes:

```text
agent A is editing function body lines 40-80
human cursor is in same function
agent B attempts rewrite of whole file
```

This moves from filesystem coordination to live collaborative editing.

Potential backend concepts:

- CRDT operations
- operational transforms
- LSP ranges
- editor cursor/selection presence

Not v1, but strategically important.

### 23. LSP-Aware Bridge

`LATER`

The bridge could subscribe to language-server facts:

- symbol definitions
- references
- diagnostics
- rename operations
- code actions
- compile errors

Then it can reason about semantic blast radius:

```text
Agent A changed interface PaymentProvider.
Bridge warns agents editing StripeProvider, BillingService, and payment tests.
```

This turns file leases into dependency leases.

### 24. SUPP / Code-Aware Context Adapter

`NEXT`

A code-aware adapter could summarize touched symbols, dependencies, and diff risk.

Use cases:

- infer semantic leases from changed files
- explain why two agents conflict despite different paths
- produce concise evidence for signals
- identify missing tests based on dependency graph

The daemon should not need to implement all language intelligence itself. It should allow analyzers to publish events.

### 25. Agent Bridge as Blackboard Architecture

`LATER`

Classic AI systems used a blackboard: independent specialists read/write shared facts.

Agent Bridge can become a blackboard for coding:

```text
builder agent posts diff
reviewer posts concern
tester posts failing case
security sidecar posts risk
human posts decision
scheduler posts ownership update
```

The bridge does not need to be smart. It needs to make facts durable, visible, and routable.

### 26. Sidecar Ecosystem

`NEXT`

Sidecars are specialized agents/processes that subscribe to bridge events and emit signals.

Examples:

- code-only spec reviewer
- test adequacy reviewer
- security reviewer
- dependency-risk reviewer
- migration reviewer
- performance regression watcher
- generated-file guardian
- docs consistency checker
- flaky-loop detector

Sidecars should declare:

```json
{
  "sidecar": "spec-reviewer",
  "subscribes": ["git.diff.changed", "spec.updated"],
  "emits": ["SIGFAITH", "SIGREPLAN", "SIGASK"],
  "can_block": false
}
```

### 27. The Spec God Sidecar

`NEXT`

A sidecar whose only job is preserving the user's real objective.

It should ignore agent transcript at first and inspect only:

- spec ledger
- code diff
- tests
- repo conventions
- changed-file scope

Its job:

```text
Did the implementation remain faithful to the objective, or did it merely satisfy literal words?
```

Signals:

- `SIGFAITH`: betrayed intent
- `SIGREPLAN`: design is wrong, stop digging
- `SIGASK`: ambiguity requires human decision
- `SIGTEST`: tests are laundering bad behavior

### 28. Test Laundering Detector

`NEXT`

Agents often make tests pass by adapting tests to bad behavior.

Bridge/sidecar can flag suspicious patterns:

- tests assert implementation details instead of behavior
- deleted tests near modified code
- snapshots updated without explanation
- coverage moved away from changed behavior
- test names do not match assertions
- new tests mirror hardcoded branches

This is a concrete instance of faithfulness monitoring.

### 29. Scope Creep Detector

`NOW`

Given a task intent and touched files, detect when the agent wanders.

Example:

```text
Task: fix parser edge case.
Touched: parser, auth, theme, package manager config.
Signal: SIGSCOPE
```

This can start simple and become semantic later.

### 30. Agent Mutex With Graceful Yield

`NOW`

Instead of blocking writes outright, use graceful yield protocol:

1. Agent B wants hot file.
2. Bridge sends `SIGYIELD`.
3. Agent B must choose:
   - wait
   - ask human
   - request handoff
   - override with reason
4. Override is logged.

This keeps human control while preventing silent clobbering.

### 31. Handoff Protocol

`NEXT`

Agents should be able to hand off work explicitly.

```text
agent A: I own src/auth/** for parser refactor.
agent A -> agent B: handoff tests/auth/** after commit abc123.
agent B: accepted.
bridge: transfers lease.
```

This enables workflows like:

- builder -> tester
- tester -> fixer
- reviewer -> builder
- human -> agent

### 32. Work Stealing

`MOONSHOT`

If an agent is idle or blocked, it can ask for available work.

Bridge can suggest tasks based on:

- cold files
- failing tests
- TODOs
- open review comments
- stale leases
- sidecar signals

This turns Agent Bridge into a lightweight local scheduler.

### 33. Agent Market / Bidding

`MOONSHOT`

Agents bid on tasks/files based on capability and context.

```text
Task: fix failing auth test
pi: bid 0.82, already has context
codex: bid 0.61, can run isolated test
claude: bid 0.74, strong reviewer
```

The bridge assigns or recommends work.

This is probably overkill, but it points toward multi-agent resource allocation.

### 34. The Human as Root User

`NOW`

The human must remain the ultimate authority.

Principles:

- every hard block has an override
- every override has a reason
- signals explain evidence
- sidecars cannot silently take over
- bridge state is inspectable
- bridge can be disabled per workspace

Agent Bridge should increase human leverage, not create another opaque boss.

### 35. Interrupt Budget

`NEXT`

Too many warnings kill trust. The bridge needs an interrupt budget.

Policy:

- group related warnings
- suppress repeated low-confidence warnings
- escalate repeated ignored warnings
- prefer one high-quality `SIGREPLAN` over ten nags
- let user tune sensitivity by workspace/mode

Modes:

```text
quiet       only destructive blocks
normal      conflicts + high-confidence spec drift
strict      scope/spec/test warnings
paranoid    protected paths and all semantic concerns
```

### 36. Semantic Kill Switch

`DANGER`

`SIGKILLSEM` should mean: terminate the current trajectory, not necessarily the process.

Possible actions:

- stop current tool execution
- inject steering message
- force replan before further edits
- block writes until human ack
- snapshot and quarantine diff
- shut down session only as last resort

This is safer than Unix `SIGKILL` because the target is behavior, not just process lifetime.

### 37. Agent Timeouts and Stuckness

`NEXT`

Agents get stuck in loops:

- rerunning same failing test
- applying same patch repeatedly
- reading same file without progress
- making broad rewrites after failures

Bridge can detect loop signatures and emit:

```text
SIGREPLAN: same command failed 4 times with no diff improvement.
```

This uses process/tool events more than code diff.

### 38. Diff Debt Meter

`NEXT`

Track how large/risky a diff becomes relative to task size.

Signals:

```text
small task, huge diff
many unrelated files
tests deleted
lockfile churn
generated files modified
format-only churn mixed with behavior change
```

This gives the bridge a way to say, "this is getting out of hand."

### 39. Merge Court

`MOONSHOT`

When agents conflict, the bridge convenes a merge court.

Participants:

- builder agent
- conflicting agent
- reviewer sidecar
- test sidecar
- human judge

Court packet:

- competing diffs
- stated intents
- test results
- risk summary
- recommended resolution

The human can decide quickly because the bridge prepared the evidence.

### 40. Agent Treaty Files

`LATER`

A repo can define collaboration policy in versioned config:

```yaml
# .agent-bridge.yml
workspace:
  mode: normal
protected:
  - .env*
  - infra/prod/**
leases:
  default_ttl: 20m
  hard:
    - migrations/**
sidecars:
  spec_reviewer: enabled
  test_laundering: warn
signals:
  SIGFAITH: require_ack
  SIGBLOCK: require_human
```

This is like `.editorconfig`, but for agents.

### 41. Agent-Readable Social Feed

`NEXT`

Agents need a concise feed of relevant workspace events.

Example:

```text
Recent workspace events:
- pi-7f3a claimed src/auth/** for parser refactor.
- codex-a91c modified tests/auth/session.test.ts.
- spec-reviewer emitted SIGTEST on current diff.
- human overrode SIGYIELD for docs/auth.md.
```

Adapters can inject this into context at useful boundaries, but should keep it compact.

### 42. Push Without Context Pollution

`NOW`

Signals should not always become giant chat messages. Delivery levels:

- status only
- UI notification
- queued custom message
- user-style steering message
- blocking modal
- command cancellation

The bridge should separate signal existence from context injection.

### 43. Privacy Levels

`NOW`

Events need privacy classes:

```text
public-local   safe metadata: path, event type, timestamp
sensitive      command names, branch names, issue IDs
secret         env vars, file contents, tokens, prompts
forbidden      .env contents, credentials, private keys
```

Default bridge events should store paths/hashes/summaries, not full file contents.

### 44. Secret Tripwires

`NOW`

The bridge should watch for accidental secret exposure:

- agent reads `.env`
- agent tries to print token-like strings
- agent writes secrets into logs/tests/docs
- command dumps environment

Signal:

```text
SIGDANGER: potential secret exposure; block output or require human review.
```

### 45. Network / Package Install Guard

`NEXT`

Agents running installs or fetching third-party code create security risk.

Bridge can require review for:

- package lifecycle scripts
- remote curl/bash patterns
- new binary downloads
- OAuth/token scopes
- postinstall hooks
- chmod/sudo usage

This fits the broader capability model.

### 46. Local-Only by Default

`NOW`

Agent Bridge should be local-first.

Default:

- Unix socket only
- owner-only permissions
- no cloud sync
- no remote telemetry
- no prompt upload

Remote/team mode can exist later, but local trust is the starting point.

### 47. Workspace Modes

`NEXT`

Different projects need different strictness.

Modes:

```text
solo          mostly silent; protect dangerous commands
pair          human + one agent; show presence and spec drift
swarm         many agents; leases and conflict signals strict
review        sidecars active; builder edits limited
competition   isolated worktrees/overlays per agent
production    protected paths hard-blocked
```

Mode controls default policy.

### 48. Agent Bridge DevTools

`NEXT`

A CLI/TUI for debugging the bridge itself:

```sh
agent-bridge status
agent-bridge radar
agent-bridge events --since 10m
agent-bridge leases
agent-bridge signals
agent-bridge explain <signal-id>
agent-bridge replay <event-log>
agent-bridge doctor
```

The daemon needs excellent introspection or users will not trust it.

### 49. Replayable Conflict Tests

`NOW`

Because events are append-only, conflict policy can be tested with replay files.

Example test fixture:

```text
agent A starts
agent A modifies file X
agent B starts
agent B attempts git reset --hard
expect SIGBLOCK
```

This makes the coordination brain testable without real agents.

### 50. Bridge as Agent Operating System

`MOONSHOT`

The maximal vision: Agent Bridge becomes a local operating layer for agents.

It provides:

- identity
- presence
- permissions
- signals
- scheduling
- shared memory
- event logs
- leases
- task routing
- review circuits
- safety policy

Agents become less like isolated terminals and more like processes in a shared OS.

The repo/workspace is the process namespace. The human is root. The bridge is init, scheduler, signal bus, and audit log.

## Near-Term Product Slice

The first useful version should be boring compared to the idea bank.

Build this first:

1. Go daemon with one workspace bus per repo/root.
2. Unix socket JSON protocol.
3. Agent registration and heartbeat.
4. File modification watcher.
5. Soft leases inferred from writes.
6. Pi TypeScript extension that registers sessions.
7. Pi tool-call interception for `write`, `edit`, and dangerous `bash`.
8. `SIGYIELD`, `SIGWARN`, and `SIGBLOCK`.
9. `agent-bridge status` and `agent-bridge events`.
10. Stale lease expiry.

Then add:

1. Spec ledger.
2. Code-only diff reviewer sidecar.
3. `SIGFAITH`, `SIGSCOPE`, `SIGREPLAN`.
4. Protected path policy.
5. Cross-agent dashboard/radar.

Do not start with overlays, CRDTs, agent markets, or autonomous scheduling. They are useful north stars, not the first bridge.
