import { describe, expect, it, vi } from "vitest";
import { BridgeClient } from "./client";
import { createAgentBridge } from "./index";
import type { ActorRecord, BridgeMessage } from "./protocol";
import { createMockContext, createMockPi } from "./test/mocks/pi-coding-agent";

function actor(session = "sender"): ActorRecord {
  const now = new Date().toISOString();
  return {
    address: `pi:${session}`,
    harness: "pi",
    session_uuid: session,
    cwd: "/repo",
    state: "active",
    capabilities: [],
    generation: 1,
    started_at: now,
    heartbeat_at: now,
  };
}

function context(session = "sender") {
  return createMockContext({
    cwd: "/repo",
    isIdle: () => false,
    sessionManager: {
      getSessionId: () => session,
      getSessionFile: () => `/sessions/${session}.jsonl`,
      getBranch: () => [
        { type: "message", message: { role: "user", content: "change schema" } },
        { type: "message", message: { role: "assistant", content: [{ type: "text", text: "editing schema" }] } },
      ],
    },
  });
}

function mockClient(handler?: (method: string, params: any) => any) {
  const client = new BridgeClient("/unused");
  vi.spyOn(client, "call").mockImplementation(async (method: string, params: any) => {
    if (handler) {
      const value = await handler(method, params);
      if (value !== undefined) return value;
    }
    if (method === "actor.register" || method === "actor.heartbeat" || method === "actor.alias") return actor();
    if (method === "sessions.list") return { actors: [] };
    if (method === "mailbox.poll") return { messages: [] };
    if (method === "intent.begin") return { collisions: [] };
    return {};
  });
  return client;
}

async function start(pi: ReturnType<typeof createMockPi>, client: BridgeClient, ctx = context()) {
  createAgentBridge(pi, client);
  await pi.events.get("session_start")?.[0]?.({ reason: "startup" }, ctx);
  return ctx;
}

describe("Go Agent Bridge adapter", () => {
  it("automatically reports ordinary edit intent to the daemon", async () => {
    const pi = createMockPi();
    const client = mockClient();
    const ctx = await start(pi, client);
    await pi.events.get("turn_start")?.[0]?.({ turnIndex: 2, timestamp: Date.now() }, ctx);

    await pi.events.get("tool_call")?.[0]?.({ toolName: "edit", toolCallId: "edit-1", input: { path: "src/schema.ts" } }, ctx);
    expect(client.call).toHaveBeenCalledWith(
      "intent.begin",
      expect.objectContaining({
        intent: expect.objectContaining({
          actor: "pi:sender",
          turn_id: expect.stringContaining(":turn:2"),
          turn_index: 2,
          tool_call_id: "edit-1",
          operation: "edit",
          paths: ["/repo/src/schema.ts"],
          context: { assistant_excerpt: "editing schema" },
        }),
      }),
    );
    await pi.events.get("tool_result")?.[0]?.({ toolName: "edit", toolCallId: "edit-1" }, ctx);
    expect(client.call).toHaveBeenCalledWith(
      "intent.end",
      expect.objectContaining({
        intent_id: expect.stringContaining("edit-1"),
        success: true,
        after: [{ path: "/repo/src/schema.ts", exists: false }],
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("records turn and compaction provenance for post-compaction diagnosis", async () => {
    const pi = createMockPi();
    const client = mockClient();
    const ctx = await start(pi, client);
    await pi.events.get("turn_start")?.[0]?.({ turnIndex: 3, timestamp: 1_700_000_000_000 }, ctx);
    await pi.events.get("turn_end")?.[0]?.({ turnIndex: 3 }, ctx);
    await pi.events.get("session_compact")?.[0]?.(
      { reason: "threshold", compactionEntry: { summary: "Schema decision summary", tokensBefore: 42_000 } },
      ctx,
    );
    expect(client.call).toHaveBeenCalledWith(
      "session.event",
      expect.objectContaining({
        event: expect.objectContaining({ type: "turn.started", turn_index: 3 }),
      }),
    );
    expect(client.call).toHaveBeenCalledWith(
      "session.event",
      expect.objectContaining({
        event: expect.objectContaining({
          type: "session.compacted",
          summary: "Schema decision summary",
          data: { reason: "threshold", tokens_before: 42_000 },
        }),
      }),
    );
    await pi.events.get("agent_settled")?.[0]?.({}, ctx);
    expect(client.call).toHaveBeenCalledWith(
      "session.event",
      expect.objectContaining({ event: expect.objectContaining({ type: "agent.settled" }) }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("silently reconnects and re-registers after a daemon restart", async () => {
    const pi = createMockPi();
    let failNextSessionEvent = false;
    const client = mockClient((method) => {
      if (method === "session.event" && failNextSessionEvent) {
        failNextSessionEvent = false;
        throw new Error("connect ENOENT /home/test/.agent-bridge/bridge.sock");
      }
      return undefined;
    });
    const ctx = await start(pi, client);
    failNextSessionEvent = true;
    await pi.events.get("turn_start")?.[0]?.({ turnIndex: 1, timestamp: Date.now() }, ctx);
    const registrations = vi.mocked(client.call).mock.calls.filter(([method]) => method === "actor.register");
    expect(registrations).toHaveLength(2);
    expect(ctx.ui.notify).not.toHaveBeenCalledWith(expect.stringContaining("ENOENT"), "warning");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("does not attribute an inherited Herdr pane to a headless subagent", async () => {
    const originalPane = process.env.HERDR_PANE_ID;
    const originalWorkspace = process.env.HERDR_WORKSPACE_ID;
    process.env.HERDR_PANE_ID = "pane-parent";
    process.env.HERDR_WORKSPACE_ID = "workspace-parent";
    try {
      const pi = createMockPi();
      const client = mockClient();
      const ctx = context("headless");
      ctx.mode = "print";
      await start(pi, client, ctx);
      expect(client.call).toHaveBeenCalledWith(
        "actor.register",
        expect.objectContaining({
          actor: expect.objectContaining({ pane_id: undefined, herdr_workspace_id: undefined }),
        }),
      );
      await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
    } finally {
      if (originalPane === undefined) delete process.env.HERDR_PANE_ID;
      else process.env.HERDR_PANE_ID = originalPane;
      if (originalWorkspace === undefined) delete process.env.HERDR_WORKSPACE_ID;
      else process.env.HERDR_WORKSPACE_ID = originalWorkspace;
    }
  });

  it("keeps provenance schemas out of context until the loader opts in", async () => {
    const pi = createMockPi();
    const client = mockClient();
    createAgentBridge(pi, client);
    pi.activeTools.push("bridge_tools", "bridge_provenance");
    const ctx = context();
    await pi.events.get("session_start")?.[0]?.({ reason: "startup" }, ctx);
    expect(pi.getActiveTools()).not.toContain("bridge_provenance");
    await pi.tools.get("bridge_tools").execute("load", { domain: "provenance" });
    expect(pi.getActiveTools()).toContain("bridge_provenance");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("exposes persisted provenance as a first-class read-only Pi tool", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "provenance.status") return { projection: { journal_sequence: 12, projected_sequence: 12, lag: 0 } };
      return undefined;
    });
    const ctx = await start(pi, client);
    const result = await pi.tools.get("bridge_provenance").execute("provenance", { action: "status" });
    expect(client.call).toHaveBeenCalledWith("provenance.status", {});
    expect(result.content[0].text).toContain('"lag": 0');
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("exposes the concise /bus command with enum-like completion and talk routing", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "provenance.status") {
        return {
          database: { events: 10, mutations: 2, session_events: 3 },
          projection: { projected_sequence: 10, journal_sequence: 10, lag: 0 },
        };
      }
      return undefined;
    });
    const ctx = await start(pi, client);
    const bus = pi.commands.get("bus");
    expect(bus.getArgumentCompletions("t")).toEqual([
      expect.objectContaining({ value: "talk", description: expect.stringContaining("Send a message") }),
    ]);
    await bus.handler("talk pi:receiver hello from bus", ctx);
    expect(client.call).toHaveBeenCalledWith("message.send", expect.objectContaining({ to: "pi:receiver", body: "hello from bus" }));
    await bus.handler("status", ctx);
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("projection 10/10"), "info");
    expect(pi.commands.get("bridge").description).toContain("Deprecated alias");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("fans out /bus talk --repo client-side", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "sessions.list") return { actors: [actor("sender"), actor("peer-a"), actor("peer-b")] };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("bus").handler("talk --repo hello repository", ctx);
    const sends = vi.mocked(client.call).mock.calls.filter(([method]) => method === "message.send");
    expect(sends.map(([, params]) => (params as any).to)).toEqual(["pi:peer-a", "pi:peer-b"]);
    expect(sends.every(([, params]) => (params as any).body === "hello repository")).toBe(true);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("reserves client sequences before concurrent sends", async () => {
    const pi = createMockPi();
    const sent: number[] = [];
    const client = mockClient(async (method, params) => {
      if (method !== "message.send") return undefined;
      sent.push(params.client_sequence);
      if (params.client_sequence === 1) await new Promise((resolve) => setTimeout(resolve, 10));
      return {
        id: `message-${params.client_sequence}`,
        kind: "message",
        from: "pi:sender",
        to: "pi:receiver",
        body: params.body,
        global_sequence: params.client_sequence,
        sender_sequence: params.client_sequence,
        recipient_sequence: params.client_sequence,
        client_sequence: params.client_sequence,
        created_at: new Date().toISOString(),
      } satisfies BridgeMessage;
    });
    const ctx = await start(pi, client);
    const tool = pi.tools.get("bridge_message");
    await Promise.all([tool.execute("one", { to: "pi:receiver", body: "one" }), tool.execute("two", { to: "pi:receiver", body: "two" })]);
    expect(sent).toEqual([1, 2]);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("keeps polled mail pending until the injected agent run settles", async () => {
    const pi = createMockPi();
    pi.sendMessage = vi.fn();
    let polls = 0;
    const message: BridgeMessage = {
      id: "message-1",
      kind: "message",
      from: "pi:peer",
      to: "pi:sender",
      body: "hello",
      global_sequence: 1,
      sender_sequence: 1,
      recipient_sequence: 1,
      created_at: new Date().toISOString(),
    };
    const client = mockClient((method) => {
      if (method === "mailbox.poll") return { messages: polls++ === 0 ? [message] : [] };
      return undefined;
    });
    const ctx = await start(pi, client);
    expect(pi.sendMessage).toHaveBeenCalledWith(expect.objectContaining({ content: expect.stringContaining("hello") }), {
      triggerTurn: true,
      deliverAs: "steer",
    });
    expect(client.call).not.toHaveBeenCalledWith("mailbox.ack", expect.anything());
    await pi.events.get("agent_settled")?.[0]?.({}, ctx);
    expect(client.call).toHaveBeenCalledWith("mailbox.ack", { actor: "pi:sender", message_ids: ["message-1"] });
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });
});
