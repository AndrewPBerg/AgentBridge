import { randomUUID } from "node:crypto";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { BridgeClient, ensureDaemon } from "./client";
import { inspectGit } from "./git";
import { listHerdrAgents } from "./herdr";
import { inferIntentContext, inferMutation } from "./intent";
import { inspectJj } from "./jj";
import type { ActorRecord, BridgeMessage, Collision, CollisionState, MutationIntent } from "./protocol";
import { BRIDGE_MESSAGE_TYPE } from "./protocol";

const HEARTBEAT_MS = 2_000;
const JJ_REFRESH_MS = 10_000;
const MAILBOX_POLL_MS = 250;
const INTENT_TTL_MS = 5 * 60_000;
const ALIAS_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/;

type SessionsResult = { actors: ActorRecord[] };
type PollResult = { messages: BridgeMessage[] | null };
type IntentResult = { intent: MutationIntent; collisions: Collision[] };

export function createAgentBridge(pi: ExtensionAPI, client = new BridgeClient()) {
  const openIntents = new Map<string, string>();
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
  let lastError = "";

  function reportError(ctx: ExtensionContext | undefined, error: unknown) {
    const message = error instanceof Error ? error.message : String(error);
    if (message === lastError) return;
    lastError = message;
    ctx?.ui.notify(`Agent Bridge: ${message}`, "warning");
  }

  async function heartbeat(state?: ActorRecord["state"]) {
    if (!actor || heartbeatRunning) return;
    heartbeatRunning = true;
    try {
      actor = await client.call<ActorRecord>("actor.heartbeat", {
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
    actor = await client.call<ActorRecord>("actor.heartbeat", {
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
      const result = await client.call<PollResult>("mailbox.poll", { actor: actor.address, limit: 100 });
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

  async function send(to: string, body: string): Promise<BridgeMessage> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    if (!body.trim()) throw new Error("Message body cannot be empty");
    // Reserve before awaiting the daemon so concurrent tool calls retain source order.
    const sequence = ++clientSequence;
    return client.call<BridgeMessage>("message.send", {
      id: `${actor.address}:${generation}:${sequence}`,
      from: actor.address,
      to,
      body: body.trim(),
      client_sequence: sequence,
      session_generation: generation,
    });
  }

  async function sessions(): Promise<string[]> {
    const [bridge, herdrAgents] = await Promise.all([
      client.call<SessionsResult>("sessions.list", { include_stale: false }),
      listHerdrAgents(pi),
    ]);
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
      const collision = await client.call<Collision>("collision.transition", {
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

  pi.registerCommand("bridge", {
    description: "Agent Bridge: /bridge sessions | name <alias> | send <target> <message>",
    handler: async (args, ctx) => {
      const [action = "sessions", ...rest] = String(args ?? "")
        .trim()
        .split(/\s+/);
      try {
        if (action === "sessions") {
          const lines = await sessions();
          ctx.ui.notify(lines.length ? lines.join("\n") : "No active agents found.", "info");
          return;
        }
        if (action === "name") {
          if (!actor) throw new Error("Agent Bridge is not attached");
          const alias = String(rest[0] ?? "").replace(/^@/, "");
          if (!ALIAS_PATTERN.test(alias)) throw new Error("Alias must be 1-32 letters, numbers, underscores, or hyphens");
          actor = await client.call<ActorRecord>("actor.alias", { address: actor.address, alias });
          ctx.ui.notify(`Agent Bridge alias set to @${actor.alias}.`, "info");
          return;
        }
        if (action === "send") {
          await send(rest[0] ?? "", rest.slice(1).join(" "));
          ctx.ui.notify(`Message sent to ${rest[0]}.`, "info");
          return;
        }
        ctx.ui.notify("Usage: /bridge sessions | name <alias> | send <target> <message>", "warning");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
  });

  pi.on("before_agent_start", (event) => ({
    systemPrompt: `${event.systemPrompt}\n\nAgent Bridge automatically detects shared-workspace file collisions. When it reports one, do not revert unfamiliar edits; coordinate using bridge_message, then record yield or resolution using bridge_collision. You never declare edit intent manually.`,
  }));

  pi.on("session_start", async (_event, ctx) => {
    sessionCtx = ctx;
    generation = Date.now();
    clientSequence = 0;
    await ensureDaemon(client);
    const sessionId = ctx.sessionManager.getSessionId();
    const now = new Date().toISOString();
    const [git, jj] = await Promise.all([inspectGit(pi, ctx.cwd), inspectJj(pi, ctx.cwd)]);
    actor = await client.call<ActorRecord>("actor.register", {
      actor: {
        address: `pi:${sessionId}`,
        harness: "pi",
        session_id: sessionId,
        session_file: ctx.sessionManager.getSessionFile(),
        cwd: ctx.cwd,
        pane_id: ctx.mode === "tui" ? process.env.HERDR_PANE_ID : undefined,
        herdr_workspace_id: ctx.mode === "tui" ? process.env.HERDR_WORKSPACE_ID : undefined,
        state: ctx.isIdle?.() === false ? "active" : "waiting",
        git,
        jj,
        capabilities: ["mailbox", "steer", "tool-preflight", "collision-transition", "git-context", "jj-context"],
        generation,
        started_at: now,
        heartbeat_at: now,
      } satisfies ActorRecord,
    });
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

  pi.on("tool_call", async (event, ctx) => {
    if (!actor) return;
    const mutation = inferMutation(event, ctx.cwd);
    if (!mutation) return;
    const toolCallId = String(event.toolCallId ?? randomUUID());
    const started = new Date();
    const intent: MutationIntent = {
      id: `${actor.address}:${generation}:${toolCallId}`,
      actor: actor.address,
      tool_call_id: toolCallId,
      ...mutation,
      cwd: ctx.cwd,
      workspace_key: actor.git?.worktree_root ?? actor.jj?.workspace_root ?? ctx.cwd,
      git: actor.git,
      jj: actor.jj,
      context: inferIntentContext(ctx.sessionManager.getBranch()),
      started_at: started.toISOString(),
      expires_at: new Date(started.getTime() + INTENT_TTL_MS).toISOString(),
    };
    openIntents.set(toolCallId, intent.id);
    await client.call<IntentResult>("intent.begin", { intent });
  });

  pi.on("tool_result", async (event) => {
    const toolCallId = String(event.toolCallId ?? "");
    const intentId = openIntents.get(toolCallId);
    if (!intentId) return;
    openIntents.delete(toolCallId);
    await client.call("intent.end", { intent_id: intentId });
  });

  pi.on("agent_settled", async () => {
    if (!actor || pendingAcknowledgements.size === 0) return;
    const messageIds = [...pendingAcknowledgements];
    await client.call("mailbox.ack", { actor: actor.address, message_ids: messageIds });
    for (const id of messageIds) {
      pendingAcknowledgements.delete(id);
      deliveredInRuntime.delete(id);
    }
  });

  pi.on("session_shutdown", async (_event, ctx) => {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
    if (jjTimer) clearInterval(jjTimer);
    if (mailboxTimer) clearInterval(mailboxTimer);
    heartbeatTimer = undefined;
    jjTimer = undefined;
    mailboxTimer = undefined;
    for (const intentId of openIntents.values()) {
      await client.call("intent.end", { intent_id: intentId }).catch(() => undefined);
    }
    openIntents.clear();
    if (actor) await client.call("actor.heartbeat", { address: actor.address, state: "dead", generation }).catch(() => undefined);
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
