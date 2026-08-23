# Generic ticket context implementation plan

**WorkUnit:** `0ec39226-cb2f-4083-bcff-ac559e6533e6`

## Decision

Replace the proposed normalized, provider-aware external-reference model with simple local ticket context. A Direction, WorkUnit, or checkpoint carries `tickets`: a bounded JSON array of object maps. Maps have no required keys and may represent any tracker, handoff, or human note. Agent Bridge does not inspect map keys to infer provider, ownership, lifecycle, or remote identity.

Ticket context is local annotation only. It has no remote fetch, OAuth, webhook, cloud-projection, synchronization, or remote-mutation behavior. A future adapter that needs canonical remote UUID identity must introduce its own normalized BLOB mapping instead of treating arbitrary ticket JSON as authority.

## Durable contract

- Protocol: `tickets` is optional at boundaries and normalizes to `[]`.
- Valid value: bounded JSON array whose members are objects; nested values are allowed as user context, but credentials, raw API responses, transcripts, and secrets are prohibited by product policy.
- Canonicalize object-key order and reject duplicate JSON keys so retry, journal replay, and projection rebuild are deterministic.
- Direction and WorkUnit ticket replacement is an explicit journaled update with prior/result values.
- Checkpoint tickets are present in the immutable checkpoint request/event and cannot be changed afterward.
- Projections add `tickets_json TEXT NOT NULL DEFAULT '[]'` to `directions`, `work_units`, and `checkpoint_requests`.
- UUID-bearing Agent Bridge relations remain validated at boundaries and stored as 16-byte BLOBs. Ticket JSON is not an identity relation.

## Pi vertical slice

Add `bridge_ticket` with create/list/replace/clear operations for the selected Direction or WorkUnit. It accepts an arbitrary JSON array rather than `provider`, `key`, or `URL` arguments. Checkpoint declaration accepts optional ticket maps. Tool descriptions provide the only needed prompt cue:

> When a user supplies ticket context, offer to record it on the selected Direction or WorkUnit. Say it was stored only after the daemon confirms success.

No tracker MCP tools are added to agent prompts and the extension never fetches or mutates remote data.

## Tests

1. protocol canonicalization, invalid JSON, non-array, non-object member, size limit, and duplicate-key rejection;
2. Direction/WorkUnit update and replay/idempotency tests;
3. immutable checkpoint ticket projection/replay tests;
4. SQLite projection/rebuild tests, retaining existing BLOB-length assertions for all UUID columns;
5. extension tool journey: select WorkUnit, store arbitrary maps, list/clear, then verify restart/replay; and
6. two fixtures with unrelated shapes to prove no Linear/GitHub/Jira key is required.

## Delivery order

1. daemon protocol/state/projection/query path plus focused Go tests;
2. Pi `bridge_ticket` tool, checkpoint argument, compact status display, and TypeScript tests;
3. local dogfood using unrelated ticket-map shapes with no network dependency.
