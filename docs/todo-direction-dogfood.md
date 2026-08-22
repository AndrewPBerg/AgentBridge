# Agent Bridge Direction dogfood report

## Result

Direction `f609d91e-89c1-462d-aedd-2e73c0b23306` produced a working Todo application across two independent Git repositories:

- Go `net/http` API with embedded Turso/SQLite persistence;
- Svelte/TypeScript/Vite frontend;
- component checks and production build;
- real API plus built frontend exercised through system Chromium/CDP; and
- CRUD, all/active/completed filters, validation, API error behavior, reload persistence, and API restart persistence.

The disposable application repositories were removed after the exercise. Durable Agent Bridge WorkUnits, checkpoints, test results, and this report preserve the relevant outcome and lessons.

## WorkUnits and evidence

- Backend `3546c188-d653-4727-befe-f61e500008b2`: completed; verified checkpoint `todo-api-checkpoint-20260822`.
- Frontend `15d48ab6-4e58-4a5c-94bd-d20345bf2451`: completed; verified checkpoint `4c1a40e9-42fc-4491-a097-0ab42681c6ab`.
- Frontend follow-up `89fee022-8f10-4412-821c-42cb3ea51211`: completed; verified checkpoint `d5af80e2-0d8a-4e76-a091-4f725cd0f6d3`.
- Integration QA `880f6600-b595-4cbb-802e-997b2d91e5a0`: completed; corrected runtime checkpoint `todo-integration-corrected-checkpoint-20260822`.

## Corrected QA finding

The first QA run reported an active-filter bug. That conclusion was invalid: the CDP harness searched for `button[aria-pressed="active"]` and `button[aria-pressed="completed"]`, but `aria-pressed` is boolean. JavaScript evaluation exceptions were also ignored, so the absent-button failure was hidden.

The corrected harness selected filters by accessible text and failed on CDP exception details. The frontend follow-up also changed `load` to accept the requested filter directly, which was a valid defensive improvement, but the initial report was not valid evidence of an application defect.

## Product findings

### 1. Harness worker identity must be first-class

Spawned Luna sessions automatically registered under actors different from the actors assigned to their WorkUnits. Workers could manually submit checkpoints under assigned UUIDs, but automatic mutation provenance remained attached to the harness actors and lacked WorkUnit linkage.

Required future relation: `parent actor -> launch -> child actor -> optional WorkUnit`, with stable launch UUID and harness attachment events.

### 2. Direction rollup was missing

Before the follow-up slice, `direction.get` returned only the Direction record. Operators needed every WorkUnit UUID and separate queries to understand cross-repository state. A compact `direction.status` rollup and Pi lifecycle/status commands were implemented as follow-up work.

### 3. Direction lifecycle was inert

The Direction remained `draft` while several WorkUnits were active or complete. Replay-safe lifecycle transitions and Pi controls were implemented as follow-up work.

### 4. Assigned actors and actual editors can diverge

WorkUnit membership showed synthetic assigned actors while mutation timelines showed harness-created worker actors. Actor aliases, launch provenance, and unattached-activity warnings are needed.

### 5. Raw JSON-RPC was too prominent

Non-Pi workers had to construct raw JSON for mailbox polling, test results, checkpoints, and transitions. A bounded `agent-bridge worker` CLI was added for status, poll/send/ack, test recording, evidence-backed checkpoints, and lifecycle transitions.

### 6. Integration scope was awkward

The integrated issue spanned two child Git repositories, while WorkUnits require one repository/workspace scope. QA used the parent directory authority scope. Direction status now exposes effective roots and scope kinds so this is explicit rather than looking like an unexplained third repository issue.

### 7. Checkpoints worked well when evidence was explicit

Repository component checkpoints and the corrected integration checkpoint clearly separated claims from persisted test/runtime evidence. The model worked as an immutable boundary, not as a subtask.

## Immediate improvements completed during dogfood

- replay-safe Direction lifecycle and Pi `start|pause|converge|verify|complete|abandon` controls;
- deterministic `direction.status` across repositories with WorkUnit/checkpoint/evidence/readiness rollup;
- effective repository/workspace root and scope-kind labels, including parent-directory integration scope;
- bounded non-Pi `agent-bridge worker` CLI for status, mailbox poll/send/ack, test results, checkpoints, and WorkUnit transitions;
- source-daemon lifecycle/status journey, worker CLI live journey, strict Go quality, Pi checks, and corrected Chromium integration journey; and
- Todo Direction advanced from draft through active/converging/verified to completed after four descendant WorkUnits completed.

## Additional Watchman dogfood

The exercise also revealed two Watchman issues:

- generated `node_modules/.vite` cache files produced noisy external-change notifications; and
- Watchman initially classified a known `functions.edit` before its `intent.end`, producing an unattributed notification.

The deployed fixes exclude generated dependency/cache paths and perform a bounded exact-path active-intent wait followed by re-snapshot. The proof produced one expected unknown event for an uninstrumented creation and no unknown event for the subsequent instrumented modification. Instrumented creation of a brand-new path remains a known gap because Watchman has no prior baseline; the follow-up must correlate an active create intent whose `Before.Exists` is false before classifying the path as unknown.

## Deferred improvements

- first-class harness launch provenance and worker identity attachment;
- automatic warning for mutations related to an active WorkUnit but lacking WorkUnit context;
- Watchman correlation for instrumented creation of brand-new paths with no prior baseline;
- actor aliases/roles and recent-activity summaries in Direction rollup;
- Direction message/open-question summaries;
- explicit WorkUnit-to-JJ change relations;
- compact Pi prompt cue for `Direction -> WorkUnit -> JJ changes -> checkpoint/mutations -> files`; and
- dependencies and deterministic readiness after local rollup behavior is proven.
