import { describe, expect, it, vi } from "vitest";
import { BridgeClient } from "./client";
import { createAgentBridge } from "./index";
import type { ActorRecord, BridgeMessage } from "./protocol";
import { createMockContext, createMockPi } from "./test/mocks/pi-coding-agent";

function actor(session = "sender"): ActorRecord {
  const now = new Date().toISOString();
  return {
    address: `${session}`,
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

async function emitAll(pi: ReturnType<typeof createMockPi>, name: string, event: any, ctx: any) {
  let result: any;
  for (const handler of pi.events.get(name) ?? []) {
    const next = await handler(event, ctx);
    if (next !== undefined) result = next;
  }
  return result;
}

describe("Go Agent Bridge adapter", () => {
  const repositoryUUID = "11111111-1111-5111-8111-111111111111";
  const workspaceUUID = "22222222-2222-5222-8222-222222222222";
  const defaultWorkUnitUUID = "33333333-3333-5333-8333-333333333333";
  const workUnit = (uuid = defaultWorkUnitUUID): any => ({
    work_unit_uuid: uuid,
    repository_uuid: repositoryUUID,
    workspace_uuid: workspaceUUID,
    objective: "ship orchestration",
    state: "active",
    participants: [],
    checkpoints: [],
  });

  it("toggles explicit stealth mode and restores ordinary presence", async () => {
    const pi = createMockPi();
    const states: string[] = [];
    const client = mockClient((method, params) => {
      if (method !== "actor.heartbeat") return undefined;
      states.push(params.state);
      return { ...actor(), state: params.state };
    });
    const ctx = await start(pi, client);
    const tool = pi.tools.get("bridge_stealth");

    const enabled = await tool.execute("stealth-on", { enabled: true });
    expect(enabled.details).toEqual({ enabled: true, state: "stealth" });
    expect(states).toEqual(["stealth"]);

    await tool.execute("stealth-off", { enabled: false });
    expect(states).toEqual(["stealth", "active"]);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("supports compact /stealth on, off, status, and usage validation", async () => {
    const pi = createMockPi();
    const states: string[] = [];
    const client = mockClient((method, params) => {
      if (method !== "actor.heartbeat") return undefined;
      states.push(params.state);
      return { ...actor(), state: params.state };
    });
    const ctx = await start(pi, client);
    const stealth = pi.commands.get("stealth");

    await stealth.handler("on", ctx);
    await stealth.handler("status", ctx);
    expect(states).toEqual(["stealth"]);
    expect(ctx.ui.notify).toHaveBeenCalledWith("Agent Bridge stealth mode enabled.", "info");

    await stealth.handler("off", ctx);
    await stealth.handler("status", ctx);
    expect(states).toEqual(["stealth", "active"]);
    expect(ctx.ui.notify).toHaveBeenCalledWith("Agent Bridge stealth mode disabled.", "info");

    await stealth.handler("", ctx);
    await stealth.handler("maybe", ctx);
    await stealth.handler("on extra", ctx);
    expect(states).toEqual(["stealth", "active"]);
    expect(ctx.ui.notify).toHaveBeenCalledWith("Usage: /stealth on | /stealth off | /stealth status", "warning");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("automatically reports ordinary edit intent to the daemon", async () => {
    const pi = createMockPi();
    const client = mockClient();
    const ctx = await start(pi, client);
    await pi.events.get("turn_start")?.[0]?.({ turnIndex: 2, timestamp: Date.now() }, ctx);

    for (const handler of pi.events.get("tool_call") ?? []) {
      await handler({ toolName: "edit", toolCallId: "edit-1", input: { path: "src/schema.ts" } }, ctx);
    }
    expect(client.call).toHaveBeenCalledWith(
      "intent.begin",
      expect.objectContaining({
        intent: expect.objectContaining({
          actor: "sender",
          turn_id: expect.stringContaining(":turn:2"),
          turn_index: 2,
          tool_call_id: "edit-1",
          operation: "edit",
          paths: ["/repo/src/schema.ts"],
          context: { assistant_excerpt: "editing schema" },
        }),
      }),
    );
    for (const handler of pi.events.get("tool_result") ?? []) {
      await handler({ toolName: "edit", toolCallId: "edit-1" }, ctx);
    }
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

  it("declares agent and human checkpoints through the daemon", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "checkpoint.request") {
        return { id: "test", actor: "sender", checkpoint_kind: "settled" };
      }
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.events.get("turn_start")?.[0]?.({ turnIndex: 4, timestamp: Date.now() }, ctx);
    const tool = pi.tools.get("bridge_checkpoint");
    await tool.execute("checkpoint-call", { kind: "settled" });
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({
        request: expect.objectContaining({
          actor: "sender",
          declared_by: "agent",
          checkpoint_kind: "settled",
          turn_index: 4,
          claims: [expect.objectContaining({ kind: "summary", status: "asserted" })],
        }),
      }),
    );
    await pi.commands.get("checkpoint").handler("handoff", ctx);
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({
        request: expect.objectContaining({
          declared_by: "human",
          checkpoint_kind: "handoff",
          claims: [expect.objectContaining({ kind: "summary", status: "asserted" })],
        }),
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("links successful captured test evidence to a verified claim and resets after checkpoint", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      if (method === "test.result") return { id: "result-1" };
      return undefined;
    });
    const ctx = await start(pi, client);
    await emitAll(pi, "tool_call", { toolName: "bash", toolCallId: "test-1", input: { command: "pnpm test" } }, ctx);
    await emitAll(
      pi,
      "tool_result",
      {
        toolName: "bash",
        toolCallId: "test-1",
        input: { command: "pnpm test" },
        isError: false,
        content: [{ type: "text", text: "passed" }],
        details: { output: "passed", exitCode: 0, truncated: false },
      },
      ctx,
    );
    await pi.tools.get("bridge_checkpoint").execute("verified", { kind: "test", statement: "Pi tests pass" });
    const first = vi.mocked(client.call).mock.calls.find(([method]) => method === "checkpoint.request");
    const calls = vi.mocked(client.call).mock.calls.map(([method]) => method);
    expect(calls.indexOf("test.result")).toBeLessThan(calls.indexOf("checkpoint.request"));
    expect(first?.[1]).toEqual(
      expect.objectContaining({
        request: expect.objectContaining({
          test_result_ids: ["result-1"],
          claims: [{ kind: "test", statement: "Pi tests pass", status: "verified", evidence: [{ kind: "test_result", ordinal: 0 }] }],
        }),
      }),
    );
    await pi.tools.get("bridge_checkpoint").execute("empty", { kind: "test" });
    const checkpoints = vi.mocked(client.call).mock.calls.filter(([method]) => method === "checkpoint.request");
    expect(checkpoints[1]?.[1]).toEqual(
      expect.objectContaining({
        request: expect.objectContaining({ test_result_ids: [], claims: [expect.objectContaining({ status: "asserted", evidence: [] })] }),
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("matches failed and blocked claim evidence to test_result_ids", async () => {
    const pi = createMockPi();
    let resultNumber = 0;
    const client = mockClient((method, params) => {
      if (method === "test.result") return { id: `durable-${++resultNumber}` };
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: String((params as any).request.checkpoint_kind) };
      return undefined;
    });
    const ctx = await start(pi, client);
    const record = async (toolCallId: string, exitCode: number | undefined, isError: boolean) => {
      const event = {
        toolName: "bash",
        toolCallId,
        input: { command: "pnpm test" },
        content: [{ type: "text", text: exitCode === undefined ? "cancelled" : `output\nexit code: ${exitCode}` }],
        details: { output: "output", exitCode, cancelled: exitCode === undefined, truncated: false },
        isError,
      };
      await emitAll(pi, "tool_call", event, ctx);
      await emitAll(pi, "tool_result", event, ctx);
    };
    await record("failed-1", 1, true);
    await pi.tools.get("bridge_checkpoint").execute("failed", { kind: "failed" });
    await record("blocked-1", undefined, true);
    await pi.tools.get("bridge_checkpoint").execute("blocked", { kind: "blocked" });
    const requests = vi
      .mocked(client.call)
      .mock.calls.filter(([method]) => method === "checkpoint.request")
      .map(([, params]) => (params as any).request);
    expect(requests).toEqual([
      expect.objectContaining({
        checkpoint_kind: "failed",
        test_result_ids: ["durable-1"],
        claims: [expect.objectContaining({ status: "failed", evidence: [{ kind: "test_result", ordinal: 0 }] })],
      }),
      expect.objectContaining({
        checkpoint_kind: "blocked",
        test_result_ids: ["durable-2"],
        claims: [expect.objectContaining({ status: "blocked", evidence: [{ kind: "test_result", ordinal: 0 }] })],
      }),
    ]);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("downgrades test claims without evidence and keeps old checkpoint calls compatible", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      return undefined;
    });
    const ctx = await start(pi, client);
    const result = await pi.tools.get("bridge_checkpoint").execute("asserted", { kind: "test" });
    expect(result.content[0].text).toContain("lacks successful captured evidence");
    expect(ctx.ui.notify).not.toHaveBeenCalled();
    await pi.commands.get("checkpoint").handler("manual", ctx);
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({
        request: expect.objectContaining({ claims: expect.any(Array), test_result_ids: [] }),
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("does not verify failed or arbitrary bash commands", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      if (method === "test.result") throw new Error("forced persistence failure");
      return undefined;
    });
    const ctx = await start(pi, client);
    await emitAll(pi, "tool_call", { toolName: "bash", toolCallId: "nope", input: { command: "echo hello" } }, ctx);
    await emitAll(
      pi,
      "tool_result",
      {
        toolName: "bash",
        toolCallId: "nope",
        isError: false,
        content: [{ type: "text", text: "hello\nexit code: 0" }],
        details: { output: "hello", exitCode: 0, cancelled: false, truncated: false },
      },
      ctx,
    );
    await emitAll(pi, "tool_call", { toolName: "bash", toolCallId: "bad", input: { command: "go test ./..." } }, ctx);
    await emitAll(
      pi,
      "tool_result",
      {
        toolName: "bash",
        toolCallId: "bad",
        isError: true,
        content: [{ type: "text", text: "FAIL" }],
        details: { output: "FAIL", exitCode: 1, cancelled: false, truncated: false },
      },
      ctx,
    );
    await pi.tools.get("bridge_checkpoint").execute("asserted", { kind: "test" });
    expect(client.call).toHaveBeenCalledWith(
      "test.result",
      expect.objectContaining({ result: expect.objectContaining({ outcome: "failed" }) }),
    );
    const lastCall = vi.mocked(client.call).mock.calls.at(-1);
    expect(lastCall?.[1]).toEqual(
      expect.objectContaining({
        request: expect.objectContaining({
          claims: [{ status: "asserted", kind: "test", statement: "test checkpoint", evidence: [] }],
        }),
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("retains evidence when checkpoint persistence fails", async () => {
    const pi = createMockPi();
    let fail = true;
    const client = mockClient((method) => {
      if (method === "test.result") return { id: "durable-result" };
      if (method === "checkpoint.request" && fail) {
        fail = false;
        throw new Error("checkpoint unavailable");
      }
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      return undefined;
    });
    const ctx = await start(pi, client);
    await emitAll(pi, "tool_call", { toolName: "bash", toolCallId: "keep", input: { command: "vitest run" } }, ctx);
    await emitAll(
      pi,
      "tool_result",
      { toolName: "bash", toolCallId: "keep", isError: false, details: { output: "ok", exitCode: 0, truncated: false } },
      ctx,
    );
    await expect(pi.tools.get("bridge_checkpoint").execute("first", { kind: "test" })).rejects.toThrow("checkpoint unavailable");
    await pi.tools.get("bridge_checkpoint").execute("second", { kind: "test" });
    const checkpoints = vi.mocked(client.call).mock.calls.filter(([method]) => method === "checkpoint.request");
    expect(checkpoints[1]?.[1]).toEqual(
      expect.objectContaining({ request: expect.objectContaining({ test_result_ids: ["durable-result"] }) }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("closes mutation intent when failed test evidence persistence is unavailable", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "test.result") throw new Error("test result unavailable");
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      return undefined;
    });
    const ctx = await start(pi, client);
    const event = { toolName: "bash", toolCallId: "verify-mutate", input: { command: "pnpm test && rm -f generated.txt" } };
    await emitAll(pi, "tool_call", event, ctx);
    await emitAll(pi, "tool_result", { ...event, isError: true, details: { output: "FAIL", exitCode: 1, truncated: false } }, ctx);
    expect(client.call).toHaveBeenCalledWith(
      "intent.end",
      expect.objectContaining({ intent_id: expect.stringContaining("verify-mutate") }),
    );
    await pi.tools.get("bridge_checkpoint").execute("no-fabrication", { kind: "test" });
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({
        request: expect.objectContaining({ test_result_ids: [], claims: [expect.objectContaining({ evidence: [] })] }),
      }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("uses stable declaration boundaries without conflicting separate checkpoints", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => (method === "checkpoint.request" ? { id: "checkpoint", checkpoint_kind: "manual" } : undefined));
    const ctx = await start(pi, client);
    const tool = pi.tools.get("bridge_checkpoint");
    await tool.execute("same-call", { kind: "manual" });
    await tool.execute("same-call", { kind: "manual" });
    await tool.execute("next-call", { kind: "manual" });
    await pi.commands.get("checkpoint").handler("manual", ctx);
    await pi.commands.get("checkpoint").handler("manual", ctx);
    const requests = vi
      .mocked(client.call)
      .mock.calls.filter(([method]) => method === "checkpoint.request")
      .map(([, params]) => (params as any).request);
    expect(requests[0].id).toBe(requests[1].id);
    expect(new Set(requests.map((request) => request.id)).size).toBe(4);
    expect(requests.slice(3).map((request) => request.boundary_event_id)).toEqual([
      expect.stringContaining(":human:1"),
      expect.stringContaining(":human:2"),
    ]);
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
    await bus.handler("talk receiver hello from bus", ctx);
    expect(client.call).toHaveBeenCalledWith("message.send", expect.objectContaining({ to: "receiver", body: "hello from bus" }));
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
    expect(sends.map(([, params]) => (params as any).to)).toEqual(["peer-a", "peer-b"]);
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
        from: "sender",
        to: "receiver",
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
    await Promise.all([tool.execute("one", { to: "receiver", body: "one" }), tool.execute("two", { to: "receiver", body: "two" })]);
    expect(sent).toEqual([1, 2]);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("shows compact Direction status and sends lifecycle transitions", async () => {
    const pi = createMockPi();
    const directionUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let direction = { direction_uuid: directionUUID, objective: "ship orchestration", state: "draft", created_by: "sender" };
    const units = [
      { ...workUnit("33333333-3333-5333-8333-333333333333"), state: "active" },
      { ...workUnit("44444444-4444-5444-8444-444444444444"), state: "active" },
      { ...workUnit("55555555-5555-5555-8555-555555555555"), state: "paused" },
    ];
    const client = mockClient((method, params) => {
      if (method === "direction.create") return direction;
      if (method === "direction.status") return { direction, work_units: units };
      if (method === "direction.transition") {
        direction = { ...direction, state: params.state };
        return direction;
      }
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("direction").handler("ship orchestration", ctx);
    await pi.commands.get("direction").handler("status", ctx);
    expect(client.call).toHaveBeenCalledWith("direction.status", { direction_uuid: directionUUID });
    expect(ctx.ui.notify).toHaveBeenCalledWith("ship orchestration · draft · WorkUnits active=2 paused=1", "info");
    for (const [command, state] of [
      ["start", "active"],
      ["pause", "paused"],
      ["converge", "converging"],
      ["verify", "verified"],
      ["complete", "completed"],
      ["abandon", "abandoned"],
    ]) {
      await pi.commands.get("direction").handler(command, ctx);
      expect(client.call).toHaveBeenCalledWith("direction.transition", { direction_uuid: directionUUID, actor: "sender", state });
    }
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("rejects invalid Direction actions and clears a stale selection", async () => {
    const pi = createMockPi();
    const directionUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const direction = { direction_uuid: directionUUID, objective: "stale", state: "active", created_by: "sender" };
    const client = mockClient((method) => {
      if (method === "direction.get") return direction;
      if (method === "direction.status") throw new Error("direction not found");
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("direction").handler("start", ctx);
    expect(client.call).not.toHaveBeenCalledWith("direction.transition", expect.anything());
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("No Direction selected"), "warning");
    await pi.commands.get("direction").handler(`use ${directionUUID}`, ctx);
    await pi.commands.get("direction").handler("status", ctx);
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("Selected Direction cleared"), "warning");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("clears Direction selection on shutdown", async () => {
    const pi = createMockPi();
    const directionUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const client = mockClient((method) =>
      method === "direction.get"
        ? { direction_uuid: directionUUID, objective: "shutdown", state: "active", created_by: "sender" }
        : undefined,
    );
    const ctx = await start(pi, client);
    await pi.commands.get("direction").handler(`use ${directionUUID}`, ctx);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
    const secondPi = createMockPi();
    const second = await start(secondPi, client, context("second"));
    await secondPi.commands.get("direction").handler("status", second);
    expect(second.ui.notify).toHaveBeenCalledWith("No Direction selected.", "info");
    await secondPi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, second);
  });

  it("creates, joins, and selects a WorkUnit, then reports and clears it", async () => {
    const pi = createMockPi();
    const client = mockClient((method, params) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      if (method === "work_unit.create") return workUnit(params.work_unit.work_unit_uuid);
      if (method === "provenance.work_unit") return workUnit(params.work_unit_uuid);
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler("<3 remaining tasks", ctx);
    expect(client.call).toHaveBeenCalledWith(
      "work_unit.create",
      expect.objectContaining({ work_unit: expect.objectContaining({ created_by: "sender", objective: "<3 remaining tasks" }) }),
    );
    expect(client.call).toHaveBeenCalledWith("work_unit.join", expect.objectContaining({ actor: "sender" }));
    await pi.commands.get("work").handler("status", ctx);
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("ship orchestration"), "info");
    await pi.commands.get("work").handler("clear", ctx);
    expect(ctx.ui.notify).toHaveBeenCalledWith("WorkUnit selection cleared.", "info");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("validates /work use before joining and rejects cross-scope units", async () => {
    const pi = createMockPi();
    const otherUUID = "44444444-4444-5444-8444-444444444444";
    const client = mockClient((method) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      if (method === "provenance.work_unit") return { ...workUnit(otherUUID), repository_uuid: "55555555-5555-5555-8555-555555555555" };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler(`use ${otherUUID}`, ctx);
    expect(client.call).not.toHaveBeenCalledWith("work_unit.join", expect.anything());
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("different repository"), "warning");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("rejects malformed WorkUnit UUIDs before querying the daemon", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler("use not-a-uuid", ctx);
    expect(client.call).not.toHaveBeenCalledWith("provenance.work_unit", expect.anything());
    expect(ctx.ui.notify).toHaveBeenCalledWith(expect.stringContaining("canonical UUID"), "warning");
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("uses the selected WorkUnit for implicit checkpoints but validates explicit overrides", async () => {
    const pi = createMockPi();
    const client = mockClient((method, params) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      if (method === "provenance.work_unit") return workUnit(params.work_unit_uuid);
      if (method === "work_unit.create") return workUnit(params.work_unit.work_unit_uuid);
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler("objective", ctx);
    await pi.tools.get("bridge_checkpoint").execute("implicit", { kind: "test" });
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({ request: expect.objectContaining({ work_unit_uuid: expect.any(String) }) }),
    );
    const explicitWorkUnitUUID = "44444444-4444-5444-8444-444444444444";
    await pi.tools.get("bridge_checkpoint").execute("explicit", { kind: "test", workUnitUUID: explicitWorkUnitUUID });
    expect(client.call).toHaveBeenCalledWith("provenance.work_unit", { work_unit_uuid: explicitWorkUnitUUID });
    expect(client.call).toHaveBeenCalledWith(
      "checkpoint.request",
      expect.objectContaining({ request: expect.objectContaining({ work_unit_uuid: explicitWorkUnitUUID }) }),
    );
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("clears WorkUnit selection on session shutdown and preserves unselected checkpoints", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      if (method === "work_unit.create") return workUnit();
      if (method === "checkpoint.request") return { id: "checkpoint", checkpoint_kind: "test" };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler("objective", ctx);
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
    const secondPi = createMockPi();
    const second = await start(secondPi, client, context("second"));
    await secondPi.tools.get("bridge_checkpoint").execute("plain", { kind: "test" });
    expect(vi.mocked(client.call).mock.calls.at(-1)?.[1]).toEqual(
      expect.objectContaining({ request: expect.objectContaining({ work_unit_uuid: undefined }) }),
    );
    await secondPi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, second);
  });

  it("rejects stale selected WorkUnits without recording a checkpoint", async () => {
    const pi = createMockPi();
    const client = mockClient((method) => {
      if (method === "actor.register") return { ...actor(), repository_uuid: repositoryUUID, workspace_uuid: workspaceUUID };
      if (method === "work_unit.create") return workUnit();
      if (method === "provenance.work_unit") throw new Error("not found");
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("work").handler("objective", ctx);
    await expect(pi.tools.get("bridge_checkpoint").execute("stale", { kind: "test" })).rejects.toThrow("not found");
    expect(client.call).not.toHaveBeenCalledWith("checkpoint.request", expect.anything());
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("lists ticket context through a read and replaces or clears it through updates", async () => {
    const pi = createMockPi();
    const directionUUID = "44444444-4444-5444-8444-444444444444";
    const stored = [{ key: "AB-1", title: "ticket" }];
    const direction = { direction_uuid: directionUUID, objective: "ship", state: "active", tickets: stored };
    const client = mockClient((method, params) => {
      if (method === "direction.get") return direction;
      if (method === "direction.update") return { ...direction, tickets: (params as any).tickets };
      return undefined;
    });
    const ctx = await start(pi, client);
    await pi.commands.get("direction").handler(`use ${directionUUID}`, ctx);

    const listed = await pi.tools.get("bridge_ticket").execute("list", { action: "list", target: "direction" });
    expect(listed.content[0].text).toBe(JSON.stringify(stored));
    expect(client.call).toHaveBeenCalledWith("direction.get", { direction_uuid: directionUUID });
    expect(client.call).not.toHaveBeenCalledWith("direction.update", expect.anything());

    await pi.tools.get("bridge_ticket").execute("replace", { action: "replace", target: "direction", tickets: stored });
    await pi.tools.get("bridge_ticket").execute("clear", { action: "clear", target: "direction" });
    expect(client.call).toHaveBeenCalledWith("direction.update", expect.objectContaining({ tickets: stored }));
    expect(client.call).toHaveBeenCalledWith("direction.update", expect.objectContaining({ tickets: [] }));
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });

  it("keeps polled mail pending until the injected agent run settles", async () => {
    const pi = createMockPi();
    pi.sendMessage = vi.fn();
    let polls = 0;
    const message: BridgeMessage = {
      id: "message-1",
      kind: "message",
      from: "peer",
      to: "sender",
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
    expect(client.call).toHaveBeenCalledWith("mailbox.ack", { actor: "sender", message_ids: ["message-1"] });
    await pi.events.get("session_shutdown")?.[0]?.({ reason: "quit" }, ctx);
  });
});
