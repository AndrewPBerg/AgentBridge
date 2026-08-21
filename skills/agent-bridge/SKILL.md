---
name: agent-bridge
description: Query Agent Bridge provenance, explain who changed a file, inspect another agent, recover context after compaction, and diagnose collisions. Use only when attribution or coordination history is relevant; load optional provenance tools progressively.
---

# Agent Bridge

Agent Bridge messaging and collision tools are normally available. Provenance is intentionally deferred to avoid adding its schema to every prompt.

## Load provenance only when needed

Call `bridge_tools` with:

```json
{"domain":"provenance"}
```

Then use `bridge_provenance`. Start with a limit of 10 or less.

## Common questions

```text
Who changed a file?        action=who-changed, path=/absolute/path
Why did this happen?       action=why, target=<mutation-id>
What is an agent doing?    action=agent, target=@alias
Since last compaction?     action=since-compaction, target=@alias
Raw mutation filter?       action=mutations, target=@alias or path=...
Session history?           action=session, target=@alias
Projection health?         action=status
```

Prefer deterministic `who-changed`, `why`, `agent`, and `since-compaction` answers over dumping raw timelines. Cite actor, session generation, turn ID, Git/JJ identity, success, and before/after hashes when available.

## Authority scopes

Interpret scope explicitly:

- global: all known actors
- repository: shared Git common directory or JJ repository
- workspace: one physical Git worktree/JJ workspace
- directory: cwd or recent mutation beneath a path

Same physical workspace can collide. Separate worktrees in one repository are related but do not share a physical file.

Read [references/provenance.md](references/provenance.md) only for advanced CLI, schema, or diagnosis workflows.
