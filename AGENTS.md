# Repository Agent Rules

## UUID storage

- Canonical UUID identities are 128-bit UUIDs represented as canonical strings at protocol/API boundaries.
- Persist UUID identities in Turso/SQLite as exactly 16-byte `BLOB` values, never textual UUIDs or prefixed identifiers.
- Convert UUID strings to/from 16-byte blobs at the database boundary; validate length, variant, and version when decoding.
- Repository and workspace UUIDs must be deterministic for the same scope identity. Do not replace them with random `uuid4()` values.
- Keep the protocol field names `repository_uuid` and `workspace_uuid`; do not reintroduce `repository_id`/`workspace_id` or `repo:`/`workspace:` prefixes.
- Schema migrations must preserve existing UUID identity values while converting text storage to 16-byte blobs.
- Every persisted UUID-bearing column—including actor references in mutations, session events, messages, test results, checkpoints, and collision membership—must use `BLOB` storage and 16-byte values.
- Do not store UUID-bearing identifiers inside JSON when a relational BLOB column can represent them; use join tables such as `collision_actors` for membership.
- Text is allowed only at protocol/API boundaries and for legacy migration input; projection backfills must prune rows that cannot be converted to valid UUID blobs.
