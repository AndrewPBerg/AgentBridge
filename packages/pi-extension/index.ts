import { createHash, randomUUID } from "node:crypto";
import { dirname } from "node:path";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { BridgeClient, ensureDaemon } from "./client";
import { inspectGit } from "./git";
import { listHerdrAgents } from "./herdr";
import { inferIntentContext, inferMutation } from "./intent";
import { inspectJj } from "./jj";
import type { ActorRecord, BridgeMessage, CheckpointRequest, Collision, CollisionState, MutationIntent, SessionEvent } from "./protocol";
import { BRIDGE_MESSAGE_TYPE } from "./protocol";
import { snapshotFiles } from "./provenance";
import { initialTalkTargets, sameRepository, showTalkModal } from "./talk-modal";

const HEARTBEAT_MS = 2_000;
const JJ_REFRESH_MS = 10_000;
const MAILBOX_POLL_MS = 250;
const INTENT_TTL_MS = 5 * 60_000;
const ALIAS_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/;

type SessionsResult = { actors: ActorRecord[] };
type PollResult = { messages: BridgeMessage[] | null };
type IntentResult = { intent: MutationIntent; collisions: Collision[] };

export function createAgentBridge(pi: ExtensionAPI, client = new BridgeClient()) {
  const openIntents = new Map<string, MutationIntent>();
  const deliveredInRuntime = new Set<string>();
  const pendingAcknowledgements = new Set<string>();
  let actor: ActorRecord | undefined;
  let sessionCtx: ExtensionContext | undefined;
  let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  let jjTimer: ReturnType<typeof setInterval> | undefined;
  let mailboxTimer: ReturnType<typeof setInterval> | undefined;
  let mailboxPolling = false;
  let heartbeatRunning = false;
  let generation = 0;
  let clientSequence = 0;
  let sessionEventSequence = 0;
  let currentTurnIndex: number | undefined;
  let recoveryPromise: Promise<void> | undefined;
  let lastError = "";

  function reportError(ctx: ExtensionContext | undefined, error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (message === lastError) return;
    lastError = message;
    ctx?.ui.notify(`Agent Bridge: ${message}`, "warning");
  }

  function recoverable(error: unknown): boolean {
    const message = error instanceof Error ? error.message : String(error);
    return /ENOENT|ECONNREFUSED|EPIPE|closed before replying|unknown actor/i.test(message);
  }

  async function recoverConnection() {
    if (recoveryPromise) return recoveryPromise;
    recoveryPromise = (async () => {
      await ensureDaemon(client);
      if (actor) {
        const now = new Date().toISOString();
        actor = await client.call<ActorRecord>("actor.register", {
          actor: {
            ...actor,
            state: sessionCtx?.isIdle?.() === false ? "active" : "waiting",
            heartbeat_at: now,
            generation,
          },
        });
      }
    })().finally(() => {
      recoveryPromise = undefined;
    });
    return recoveryPromise;
  }

  async function call<T = unknown>(method: string, params: unknown = {}): Promise<T> {
    try {
      return await client.call<T>(method, params);
    } catch (error) {
      if (!recoverable(error)) throw error;
      await recoverConnection();
      return client.call<T>(method, params);
    }
  }

  async function heartbeat(state?: ActorRecord["state"]) {
    if (!actor || heartbeatRunning) return;
    heartbeatRunning = true;
    try {
      actor = await call<ActorRecord>("actor.heartbeat", {
        address: actor.address,
        state: state ?? (sessionCtx?.isIdle?.() === false ? "active" : "waiting"),
        cwd: sessionCtx?.cwd,
        generation,
      });
      lastError = "";
    } catch (error) {
      reportError(sessionCtx, error);
    } finally {
      heartbeatRunning = false;
    }
  }

  async function refreshVCS() {
    if (!actor || !sessionCtx) return;
    const [git, jj] = await Promise.all([inspectGit(pi, sessionCtx.cwd), inspectJj(pi, sessionCtx.cwd)]);
    actor = await call<ActorRecord>("actor.heartbeat", {
      address: actor.address,
      state: sessionCtx.isIdle?.() === false ? "active" : "waiting",
      cwd: sessionCtx.cwd,
      git,
      jj,
      generation,
    });
  }

  async function pollMailbox() {
    if (!actor || mailboxPolling) return;
    mailboxPolling = true;
    try {
      const result = await call<PollResult>("mailbox.poll", { actor: actor.address, limit: 100 });
      for (const message of result.messages ?? []) {
        if (deliveredInRuntime.has(message.id)) continue;
        pi.sendMessage(
          {
            customType: BRIDGE_MESSAGE_TYPE,
            content: [
              message.kind === "collision" ? "AGENT BRIDGE COLLISION" : `AGENT BRIDGE MESSAGE from @${message.from}`,
              message.body,
              "",
              `Sequences: global=${message.global_sequence} sender=${message.sender_sequence} recipient=${message.recipient_sequence}${message.client_sequence ? ` client=${message.client_sequence}` : ""}`,
              message.collision_id
                ? `Coordinate with bridge_message, then update ${message.collision_id} using bridge_collision.`
                : "Reply with bridge_message if coordination is needed.",
            ].join("\n"),
            display: true,
            details: { message },
          },
          { triggerTurn: true, deliverAs: "steer" },
        );
        deliveredInRuntime.add(message.id);
        pendingAcknowledgements.add(message.id);
      }
      lastError = "";
    } catch (error) {
      reportError(sessionCtx, error);
    } finally {
      mailboxPolling = false;
    }
  }

  async function recordSessionEvent(
    type: string,
    options: { at?: number | string; turnIndex?: number; summary?: string; data?: Record<string, unknown> } = {},
  ) {
    if (!actor) return;
    const sequence = ++sessionEventSequence;
    const at = typeof options.at === "number" ? new Date(options.at).toISOString() : (options.at ?? new Date().toISOString());
    const event: SessionEvent = {
      id: `${actor.address}:${generation}:session-event:${sequence}`,
      actor: actor.address,
      session_generation: generation,
      type,
      at,
      turn_index: options.turnIndex,
      summary: options.summary,
      data: options.data,
    };
    await call("session.event", { event });
  }

  async function requestCheckpoint(declaredBy: "agent" | "human", kind: string, workUnitUUID?: string): Promise<CheckpointRequest> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    const boundary = `${actor.address}:${generation}:${kind}:${currentTurnIndex ?? "session"}`;
    const id = `checkpoint:${createHash("sha256").update(boundary).digest("hex")}`;
    return call<CheckpointRequest>("checkpoint.request", {
      request: {
        id,
        actor: actor.address,
        declared_by: declaredBy,
        session_generation: generation,
        repository_uuid: actor.repository_uuid,
        workspace_uuid: actor.workspace_uuid,
        work_unit_uuid: workUnitUUID,
        checkpoint_kind: kind,
        boundary_event_id: boundary,
        boundary_type: "explicit",
        turn_id: currentTurnIndex === undefined ? undefined : `${actor.address}:${generation}:turn:${currentTurnIndex}`,
        turn_index: currentTurnIndex,
        git: actor.git,
        jj: actor.jj,
      },
    });
  }

  async function send(to: string, body: string): Promise<BridgeMessage> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    if (!body.trim()) throw new Error("Message body cannot be empty");
    // Reserve before awaiting the daemon so concurrent tool calls retain source order.
    const sequence = ++clientSequence;
    return call<BridgeMessage>("message.send", {
      id: `${actor.address}:${generation}:${sequence}`,
      from: actor.address,
      to,
      body: body.trim(),
      client_sequence: sequence,
      session_generation: generation,
    });
  }

  async function activeActors(): Promise<ActorRecord[]> {
    return (await call<SessionsResult>("sessions.list", { include_stale: false })).actors;
  }

  async function sessions(): Promise<string[]> {
    const [actors, herdrAgents] = await Promise.all([activeActors(), listHerdrAgents(pi)]);
    const bridge = { actors };
    const lines = bridge.actors.map((candidate) => {
      const herdr = herdrAgents.find(
        (entry) => entry.agentSession === candidate.session_file || (candidate.pane_id && entry.paneId === candidate.pane_id),
      );
      const identity = candidate.alias ? `@${candidate.alias} (${candidate.address})` : candidate.address;
      const git = candidate.git
        ? ` git=${candidate.git.branch ?? candidate.git.head?.slice(0, 12) ?? "unborn"} worktree=${candidate.git.worktree_root}`
        : " git=none";
      const jj = candidate.jj ? ` jj=${candidate.jj.change_id.slice(0, 12)} workspace=${candidate.jj.workspace_root}` : " jj=none";
      return `${identity} ${herdr?.status ?? candidate.state} cwd=${candidate.cwd}${git}${jj}${herdr ? ` pane=${herdr.paneId}` : ""}`;
    });
    for (const herdr of herdrAgents) {
      if (bridge.actors.some((candidate) => candidate.session_file === herdr.agentSession || candidate.pane_id === herdr.paneId)) continue;
      lines.push(`${herdr.agent}:unregistered ${herdr.status} cwd=${herdr.cwd ?? "unknown"} pane=${herdr.paneId}`);
    }
    return lines.sort();
  }

  pi.registerTool({
    name: "bridge_message",
    label: "Agent Bridge Message",
    description:
      "Send an ordered durable coordination message to a peer by @alias, canonical harness:session address, or @change:ID. Canonical addresses remain deliverable while a known session reloads.",
    promptSnippet: "Send ordered durable coordination messages to peer agents.",
    promptGuidelines: [
      "Use bridge_message to coordinate with peers named in Agent Bridge collisions; never revert unfamiliar shared-workspace edits without coordinating first.",
    ],
    parameters: Type.Object({
      to: Type.String({ description: "Target such as @walkie, @pi:<session UUID>, or @change:<JJ change ID>." }),
      body: Type.String({ description: "Coordination message." }),
    }),
    async execute(_toolCallId, params) {
      const message = await send(String(params.to ?? ""), String(params.body ?? ""));
      return {
        content: [
          {
            type: "text",
            text: `Delivered ${message.id} to ${message.to} (sender sequence ${message.sender_sequence}, recipient sequence ${message.recipient_sequence}).`,
          },
        ],
        details: { message },
      };
    },
  });

  pi.registerTool({
    name: "bridge_tools",
    label: "Agent Bridge Tools",
    description: "Load optional Agent Bridge tool domains only when needed, keeping normal session context small.",
    promptSnippet: "Load optional Agent Bridge provenance tools when attribution or historical diagnosis is required.",
    parameters: Type.Object({
      domain: Type.String({ description: "Optional domain to load. Currently: provenance" }),
    }),
    async execute(_toolCallId, params) {
      const domain = String(params.domain ?? "");
      if (domain !== "provenance") throw new Error(`Unknown Agent Bridge tool domain ${JSON.stringify(domain)}`);
      const active = pi.getActiveTools();
      pi.setActiveTools([...new Set([...active, "bridge_provenance"])]);
      return {
        content: [{ type: "text", text: "Loaded bridge_provenance for this session." }],
        details: { domain, added: active.includes("bridge_provenance") ? [] : ["bridge_provenance"] },
      };
    },
  });

  pi.registerTool({
    name: "bridge_provenance",
    label: "Agent Bridge Provenance",
    description:
      "Query persisted Agent Bridge attribution: status, mutations by actor/path, one explained mutation, actor timeline, or session/compaction events.",
    promptSnippet: "Query who changed files, what an agent did, failed mutations, and pre/post-compaction history.",
    promptGuidelines: [
      "Use bridge_provenance instead of guessing when asked who changed a file, what another agent did, or what happened before compaction.",
    ],
    parameters: Type.Object({
      action: Type.String({
        description: "status, who-changed, why, agent, since-compaction, mutations, explain, timeline, or session",
      }),
      target: Type.Optional(Type.String({ description: "Actor address/@alias, or mutation ID for explain." })),
      path: Type.Optional(Type.String({ description: "Exact canonical path filter for mutations." })),
      repositoryId: Type.Optional(Type.String({ description: "Repository authority scope filter." })),
      workspaceId: Type.Optional(Type.String({ description: "Workspace authority scope filter." })),
      eventType: Type.Optional(Type.String({ description: "Optional event type filter for timeline." })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 100 })),
      failed: Type.Optional(Type.Boolean({ description: "Only failed mutations." })),
    }),
    async execute(_toolCallId, params) {
      const action = String(params.action ?? "");
      const limit = Math.min(100, Math.max(1, Number(params.limit ?? 20)));
      let method: string;
      let request: Record<string, unknown>;
      if (action === "status") {
        method = "provenance.status";
        request = {};
      } else if (action === "who-changed") {
        if (!params.path) throw new Error("bridge_provenance who-changed requires path");
        method = "provenance.who_changed";
        request = { path: params.path, limit };
      } else if (action === "why") {
        if (!params.target) throw new Error("bridge_provenance why requires a mutation ID in target");
        method = "provenance.why";
        request = { id: params.target, limit };
      } else if (action === "agent") {
        if (!params.target) throw new Error("bridge_provenance agent requires an actor or @alias in target");
        method = "provenance.agent";
        request = { actor: params.target, repository_uuid: params.repositoryId, workspace_uuid: params.workspaceId, limit };
      } else if (action === "since-compaction") {
        if (!params.target) throw new Error("bridge_provenance since-compaction requires an actor or @alias in target");
        method = "provenance.since_compaction";
        request = { actor: params.target, repository_uuid: params.repositoryId, workspace_uuid: params.workspaceId, limit };
      } else if (action === "mutations") {
        method = "provenance.mutations";
        request = {
          actor: params.target,
          path: params.path,
          repository_uuid: params.repositoryId,
          workspace_uuid: params.workspaceId,
          limit,
          failed: Boolean(params.failed),
        };
      } else if (action === "explain") {
        if (!params.target) throw new Error("bridge_provenance explain requires a mutation ID in target");
        method = "provenance.explain";
        request = { id: params.target };
      } else if (action === "timeline") {
        method = "provenance.timeline";
        request = {
          actor: params.target,
          repository_uuid: params.repositoryId,
          workspace_uuid: params.workspaceId,
          type: params.eventType,
          limit,
        };
      } else if (action === "session") {
        if (!params.target) throw new Error("bridge_provenance session requires an actor or @alias in target");
        method = "provenance.session";
        request = { actor: params.target, repository_uuid: params.repositoryId, workspace_uuid: params.workspaceId, limit };
      } else {
        throw new Error(`Unknown provenance action ${JSON.stringify(action)}`);
      }
      const result = await call<unknown>(method, request);
      const encoded = JSON.stringify(result, null, 2);
      const text = encoded.length <= 20_000 ? encoded : `${encoded.slice(0, 19_900)}\n... provenance output truncated; lower limit`;
      return { content: [{ type: "text", text }], details: { action, result } };
    },
  });

  pi.registerTool({
    name: "bridge_checkpoint",
    label: "Agent Bridge Checkpoint",
    description: "Declare an immutable, evidence-linked checkpoint at a meaningful stopping point.",
    promptSnippet: "Use bridge_checkpoint when you reach a meaningful stopping point or need a durable handoff boundary.",
    parameters: Type.Object({
      kind: Type.String({ description: "Checkpoint kind, for example settled, handoff, test, or manual." }),
      workUnitUUID: Type.Optional(Type.String({ description: "Optional WorkUnit UUID to link this checkpoint to." })),
    }),
    async execute(_toolCallId, params) {
      const kind = String(params.kind ?? "").trim();
      if (!kind) throw new Error("Checkpoint kind is required");
      const checkpoint = await requestCheckpoint("agent", kind, params.workUnitUUID ? String(params.workUnitUUID) : undefined);
      return {
        content: [{ type: "text", text: `Checkpoint ${checkpoint.id} recorded.` }],
        details: { checkpoint },
      };
    },
  });

  pi.registerTool({
    name: "bridge_collision",
    label: "Agent Bridge Collision",
    description: "Transition an automatic collision to negotiating, yielded, or resolved after peer coordination.",
    parameters: Type.Object({
      collisionId: Type.String({ description: "Collision ID from an Agent Bridge collision message." }),
      state: Type.String({ description: "negotiating, yielded, or resolved" }),
      owner: Type.Optional(Type.String({ description: "Canonical actor taking ownership when yielding." })),
      resolution: Type.Optional(Type.String({ description: "Short resolution when marking resolved." })),
    }),
    async execute(_toolCallId, params) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      const state = String(params.state ?? "") as CollisionState;
      if (!(["negotiating", "yielded", "resolved"] as string[]).includes(state)) throw new Error(`Invalid collision state ${state}`);
      const collision = await call<Collision>("collision.transition", {
        collision_id: String(params.collisionId ?? ""),
        actor: actor.address,
        state,
        owner: params.owner ? String(params.owner) : undefined,
        resolution: params.resolution ? String(params.resolution) : undefined,
      });
      return {
        content: [{ type: "text", text: `Collision ${collision.id} is now ${collision.state}.` }],
        details: { collision },
      };
    },
  });

  async function sendMany(targets: string[], body: string): Promise<void> {
    const unique = [...new Set(targets.filter(Boolean))];
    if (unique.length === 0) throw new Error("No bus recipients selected");
    const results = await Promise.allSettled(unique.map((target) => send(target, body)));
    const failed = results.flatMap((result, index) =>
      result.status === "rejected"
        ? [`${unique[index]!} (${result.reason instanceof Error ? result.reason.message : String(result.reason)})`]
        : [],
    );
    if (failed.length > 0) {
      throw new Error(`Bus fan-out delivered ${unique.length - failed.length}/${unique.length}; failed: ${failed.join(", ")}`);
    }
  }

  async function repositoryTargets(): Promise<string[]> {
    if (!actor) throw new Error("Agent Bridge is not attached");
    return (await activeActors())
      .filter((candidate) => candidate.address !== actor?.address && sameRepository(actor!, candidate))
      .map((candidate) => candidate.address);
  }

  async function openBusTalk(ctx: ExtensionContext, initialExpression?: string): Promise<void> {
    if (!actor) throw new Error("Agent Bridge is not attached");
    const actors = await activeActors();
    const initialTargets = initialTalkTargets(actor, actors, initialExpression);
    const result = await showTalkModal(ctx, actor, actors, initialTargets);
    if (!result) return;
    await sendMany(result.targets, result.body);
    ctx.ui.notify(`Bus message sent to ${result.targets.length} agent${result.targets.length === 1 ? "" : "s"}.`, "info");
  }

  const busCommands = [
    { value: "talk", label: "talk", description: "Send a message: /bus talk @agent message" },
    { value: "list", label: "list", description: "List active agent sessions" },
    { value: "name", label: "name", description: "Set this session alias" },
    { value: "status", label: "status", description: "Show durable bus/provenance health" },
  ];
  const busUsage = "Usage: /bus talk [target[,target...]] [message] | talk --repo <message> | list | name <alias> | status";

  const handleBus = async (args: string, ctx: ExtensionContext) => {
    const [rawAction = "", ...rest] = String(args ?? "")
      .trim()
      .split(/\s+/);
    const action = rawAction === "send" ? "talk" : rawAction === "sessions" || rawAction === "ls" ? "list" : rawAction;
    try {
      if (!action) {
        ctx.ui.notify(busUsage, "info");
        return;
      }
      if (action === "list") {
        const lines = await sessions();
        ctx.ui.notify(lines.length ? lines.join("\n") : "No active agents found.", "info");
        return;
      }
      if (action === "name") {
        if (!actor) throw new Error("Agent Bridge is not attached");
        const alias = String(rest[0] ?? "").replace(/^@/, "");
        if (!ALIAS_PATTERN.test(alias)) throw new Error("Alias must be 1-32 letters, numbers, underscores, or hyphens");
        actor = await call<ActorRecord>("actor.alias", { address: actor.address, alias });
        ctx.ui.notify(`Bus alias set to @${actor.alias}.`, "info");
        return;
      }
      if (action === "talk") {
        const targetExpression = rest[0];
        const body = rest.slice(1).join(" ");
        if (!targetExpression || !body) {
          await openBusTalk(ctx, targetExpression);
          return;
        }
        const targets =
          targetExpression === "--repo"
            ? await repositoryTargets()
            : targetExpression
                .split(",")
                .map((target) => target.trim())
                .filter(Boolean);
        await sendMany(targets, body);
        ctx.ui.notify(`Bus message sent to ${targets.length} agent${targets.length === 1 ? "" : "s"}.`, "info");
        return;
      }
      if (action === "status") {
        const result = await call<{
          database?: { events?: number; mutations?: number; session_events?: number };
          projection?: { journal_sequence?: number; projected_sequence?: number; lag?: number; queue_depth?: number; last_error?: string };
        }>("provenance.status", {});
        const database = result.database ?? {};
        const projection = result.projection ?? {};
        ctx.ui.notify(
          `Bus healthy · events ${database.events ?? 0} · mutations ${database.mutations ?? 0} · sessions ${database.session_events ?? 0} · projection ${projection.projected_sequence ?? 0}/${projection.journal_sequence ?? 0} · lag ${projection.lag ?? 0} · queue ${projection.queue_depth ?? 0}${projection.last_error ? ` · error ${projection.last_error}` : ""}`,
          projection.last_error ? "warning" : "info",
        );
        return;
      }
      ctx.ui.notify(busUsage, "warning");
    } catch (error) {
      ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
    }
  };

  pi.registerCommand("bus", {
    description: "Agent bus: talk, list, name, or status",
    getArgumentCompletions: (prefix: string) => {
      if (prefix.trim().includes(" ")) return null;
      const normalized = prefix.trim();
      return busCommands.filter((command) => command.value.startsWith(normalized));
    },
    handler: handleBus,
  });

  pi.registerCommand("bridge", {
    description: "Deprecated alias for /bus (send→talk, sessions→list)",
    handler: handleBus,
  });

  pi.registerCommand("checkpoint", {
    description: "Declare a human checkpoint: /checkpoint [kind]",
    getArgumentCompletions: (prefix: string) => {
      if (prefix.trim().includes(" ")) return null;
      const normalized = prefix.trim();
      return ["manual", "settled", "handoff", "test"]
        .filter((value) => value.startsWith(normalized))
        .map((value) => ({ value, label: value }));
    },
    handler: async (args: string, ctx: ExtensionContext) => {
      const kind = String(args ?? "").trim() || "manual";
      try {
        const checkpoint = await requestCheckpoint("human", kind);
        ctx.ui.notify(`Checkpoint ${checkpoint.id} recorded.`, "info");
      } catch (error) {
        reportError(ctx, error);
      }
    },
  });

  pi.on("before_agent_start", (event) => ({
    systemPrompt: `${event.systemPrompt}\n\nAgent Bridge automatically detects shared-workspace file collisions. When it reports one, do not revert unfamiliar edits; coordinate using bridge_message, then record yield or resolution using bridge_collision. You never declare edit intent manually.`,
  }));

  pi.on("session_start", async (event, ctx) => {
    sessionCtx = ctx;
    pi.setActiveTools(pi.getActiveTools().filter((name) => name !== "bridge_provenance"));
    generation = Date.now();
    clientSequence = 0;
    sessionEventSequence = 0;
    currentTurnIndex = undefined;
    await ensureDaemon(client);
    const sessionId = randomUUID();
    const now = new Date().toISOString();
    const [git, jj] = await Promise.all([inspectGit(pi, ctx.cwd), inspectJj(pi, ctx.cwd)]);
    actor = await call<ActorRecord>("actor.register", {
      actor: {
        address: sessionId,
        harness: "pi",
        session_uuid: sessionId,
        session_file: ctx.sessionManager.getSessionFile(),
        cwd: ctx.cwd,
        pane_id: ctx.mode === "tui" ? process.env.HERDR_PANE_ID : undefined,
        herdr_workspace_id: ctx.mode === "tui" ? process.env.HERDR_WORKSPACE_ID : undefined,
        state: ctx.isIdle?.() === false ? "active" : "waiting",
        git,
        jj,
        capabilities: [
          "mailbox",
          "steer",
          "tool-preflight",
          "collision-transition",
          "git-context",
          "jj-context",
          "mutation-provenance",
          "provenance-query",
          "session-events",
          "checkpoint-declaration",
        ],
        generation,
        started_at: now,
        heartbeat_at: now,
      } satisfies ActorRecord,
    });
    await recordSessionEvent("session.started", { data: { reason: event.reason } });
    await pollMailbox();
    heartbeatTimer = setInterval(() => void heartbeat(), HEARTBEAT_MS);
    heartbeatTimer.unref?.();
    jjTimer = setInterval(() => void refreshVCS().catch((error) => reportError(sessionCtx, error)), JJ_REFRESH_MS);
    jjTimer.unref?.();
    mailboxTimer = setInterval(() => void pollMailbox(), MAILBOX_POLL_MS);
    mailboxTimer.unref?.();
    const vcs = actor.git && actor.jj ? "git+jj" : actor.git ? "git" : actor.jj ? "jj" : undefined;
    ctx.ui.setStatus(BRIDGE_MESSAGE_TYPE, vcs ? `bridge:go:${vcs}` : "bridge:go");
  });

  pi.on("turn_start", async (event) => {
    currentTurnIndex = event.turnIndex;
    await recordSessionEvent("turn.started", { at: event.timestamp, turnIndex: event.turnIndex });
  });

  pi.on("turn_end", async (event) => {
    await recordSessionEvent("turn.completed", { turnIndex: event.turnIndex });
    if (currentTurnIndex === event.turnIndex) currentTurnIndex = undefined;
  });

  pi.on("session_compact", async (event) => {
    const entry = event.compactionEntry as { summary?: unknown; tokensBefore?: unknown };
    const summary = String(entry?.summary ?? "");
    await recordSessionEvent("session.compacted", {
      summary: summary.length <= 20_000 ? summary : `${summary.slice(0, 19_999)}…`,
      data: { reason: event.reason, tokens_before: entry?.tokensBefore },
    });
  });

  pi.on("tool_call", async (event, ctx) => {
    if (!actor) return;
    const mutation = inferMutation(event, ctx.cwd);
    if (!mutation) return;
    const toolCallId = String(event.toolCallId ?? randomUUID());
    const started = new Date();
    const vcsCwd = mutation.paths[0] ? dirname(mutation.paths[0]) : ctx.cwd;
    const [before, mutationGit, mutationJj] = await Promise.all([
      snapshotFiles(mutation.paths),
      inspectGit(pi, vcsCwd),
      inspectJj(pi, vcsCwd),
    ]);
    const intent: MutationIntent = {
      id: `${actor.address}:${generation}:${toolCallId}`,
      actor: actor.address,
      session_generation: generation,
      turn_id: currentTurnIndex === undefined ? undefined : `${actor.address}:${generation}:turn:${currentTurnIndex}`,
      turn_index: currentTurnIndex,
      tool_call_id: toolCallId,
      ...mutation,
      cwd: ctx.cwd,
      workspace_key:
        mutationGit?.worktree_root ?? mutationJj?.workspace_root ?? actor.git?.worktree_root ?? actor.jj?.workspace_root ?? ctx.cwd,
      git: mutationGit ?? actor.git,
      jj: mutationJj ?? actor.jj,
      context: inferIntentContext(ctx.sessionManager.getBranch()),
      before,
      started_at: started.toISOString(),
      expires_at: new Date(started.getTime() + INTENT_TTL_MS).toISOString(),
    };
    openIntents.set(toolCallId, intent);
    await call<IntentResult>("intent.begin", { intent });
  });

  pi.on("tool_result", async (event) => {
    const toolCallId = String(event.toolCallId ?? "");
    const intent = openIntents.get(toolCallId);
    if (!intent) return;
    openIntents.delete(toolCallId);
    const vcsCwd = intent.paths[0] ? dirname(intent.paths[0]) : sessionCtx?.cwd;
    const [after, gitAfter, jjAfter] = await Promise.all([
      snapshotFiles(intent.paths),
      vcsCwd ? inspectGit(pi, vcsCwd) : undefined,
      vcsCwd ? inspectJj(pi, vcsCwd) : undefined,
    ]);
    await call("intent.end", {
      intent_id: intent.id,
      success: !event.isError,
      error: event.isError ? "tool result reported an error" : undefined,
      after,
      git_after: gitAfter,
      jj_after: jjAfter,
      completed_at: new Date().toISOString(),
    });
  });

  pi.on("agent_settled", async () => {
    if (!actor) return;
    await recordSessionEvent("agent.settled", { turnIndex: currentTurnIndex }).catch((error) => reportError(sessionCtx, error));
    if (pendingAcknowledgements.size === 0) return;
    const messageIds = [...pendingAcknowledgements];
    await call("mailbox.ack", { actor: actor.address, message_ids: messageIds });
    for (const id of messageIds) {
      pendingAcknowledgements.delete(id);
      deliveredInRuntime.delete(id);
    }
  });

  pi.on("session_shutdown", async (event, ctx) => {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    if (jjTimer) clearInterval(jjTimer);
    if (mailboxTimer) clearInterval(mailboxTimer);
    heartbeatTimer = undefined;
    jjTimer = undefined;
    mailboxTimer = undefined;
    await recordSessionEvent("session.shutdown", { data: { reason: event.reason } }).catch(() => undefined);
    for (const intent of openIntents.values()) {
      await call("intent.end", {
        intent_id: intent.id,
        success: false,
        error: "session ended before tool completion",
        after: await snapshotFiles(intent.paths),
        completed_at: new Date().toISOString(),
      }).catch(() => undefined);
    }
    openIntents.clear();
    if (actor) await call("actor.heartbeat", { address: actor.address, state: "dead", generation }).catch(() => undefined);
    actor = undefined;
    sessionCtx = undefined;
    deliveredInRuntime.clear();
    pendingAcknowledgements.clear();
    ctx.ui.setStatus(BRIDGE_MESSAGE_TYPE, undefined);
  });

  return { client, sessions };
}

export default function agentBridge(pi: ExtensionAPI) {
  createAgentBridge(pi);
}
