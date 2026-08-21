# Harness compatibility

Agent Bridge keeps harness-specific observation and interruption in thin adapters. Every adapter normalizes into the same Go socket protocol:

```text
register → heartbeat → intent.begin/end → mailbox poll/ack → collision transition
```

## Pi

Status: operational.

The TypeScript extension has direct lifecycle and tool hooks, context/session access, steer injection, and explicit settlement events. It is the reference adapter.

## Codex CLI

Status: viable; adapter spike next.

The installed Codex exposes stable `hooks` and `plugins` features. Its experimental app-server has a generated typed protocol and Unix/WebSocket transports. Relevant methods and notifications include:

```text
thread/start, thread/resume, thread/read, thread/inject_items
thread/started, thread/status/changed, thread/tokenUsage/updated
turn/start, turn/steer, turn/interrupt
turn/started, turn/completed, turn/diff/updated
item/started, item/completed
item/commandExecution/*
item/fileChange/*
hook/started, hook/completed
item/reasoning/summary*
```

This is enough for actor identity, turn/session indexing, file and command observation, mailbox injection, steer, and interrupt. The preferred spike is an app-server client because it provides structured events and control; hook scripts can provide a lighter fallback for ordinary interactive CLI sessions.

Do not depend on unstable raw reasoning events. Index explicit reasoning summaries only when Codex emits them.

## OpenCode

Status: likely viable; plugin/API spike required.

The installed OpenCode exposes:

```text
opencode plugin <module>
opencode serve
opencode attach <url>
opencode acp
opencode export <sessionID>
```

A plugin should handle live tool/session events and speak directly to the Agent Bridge Unix socket. The headless server or ACP surface may provide structured session control and transcript access. Exact hook names and interruption guarantees must be verified before promising Pi-equivalent behavior.

## Compatibility levels

Adapters should advertise capabilities instead of pretending every harness is equivalent:

```text
session.register
session.index
mailbox.receive
turn.steer
turn.interrupt
tool.observe
file.preflight
collision.block
vcs.git
vcs.jj
```

The daemon routes according to these capabilities. A hooks-only adapter may observe and queue follow-ups but lack true mid-turn steer. Pi and Codex app-server can expose richer control.

## Safety

Harness adapters should never receive blanket parent/root authority implicitly. Break-glass operations such as reload, interrupt, abort, or shutdown require explicit capabilities, local authorization policy, and audit events. The human remains root; Agent Bridge is the supervisor and signal bus.
