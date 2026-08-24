import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { basename, dirname } from "node:path";
import { StringEnum } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { isBashToolResult } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { BridgeClient, ensureDaemon } from "./client";
import { inspectGit } from "./git";
import { listHerdrAgents } from "./herdr";
import { inferIntentContext, inferMutation } from "./intent";
import { inspectJj } from "./jj";
import type {
  ActorRecord,
  BridgeContext,
  BridgeMessage,
  CheckpointRequest,
  Collision,
  CollisionState,
  Direction,
  DirectionStatus,
  GitContext,
  JjContext,
  MutationIntent,
  MutationLease,
  MutationLeaseResult,
  SessionEvent,
  TestResult,
  WorkUnit,
} from "./protocol";
import { BRIDGE_MESSAGE_TYPE } from "./protocol";
import { snapshotFiles } from "./provenance";
import { initialTalkTargets, sameRepository, showTalkModal } from "./talk-modal";

const HEARTBEAT_MS = 15_000;
const JJ_REFRESH_MS = 10_000;
const MAILBOX_POLL_MS = 1_000;
const BACKGROUND_RETRY_MAX_MS = 30_000;
const INTENT_TTL_MS = 5 * 60_000;
const LEASE_RETRY_MS = 500;
const LEASE_RENEW_MS = 15_000;
const AWAKEN_ATTACH_TIMEOUT_MS = 30_000;
const ALIAS_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,31}$/;

type SessionsResult = { actors: ActorRecord[] };
type PollResult = { messages: BridgeMessage[] | null };
type IntentResult = { intent: MutationIntent; collisions: Collision[] };
type LeaseEntry = { lease: MutationLease; renewalFailures: number };
type AwakenLauncher = (command: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }) => Promise<void>;
type AwakenScheduler = (callback: () => void, delayMillis: number) => void;

function scheduleAwakenCheck(callback: () => void, delayMillis: number) {
  const timer = setTimeout(callback, delayMillis);
  timer.unref?.();
}

function piExecutable(): string {
  // Reuse the binary that started this Pi runtime, avoiding shell wrappers that
  // may inject incompatible local flags into the awakened child.
  return process.env.AGENT_BRIDGE_PI_BIN || process.argv[1] || "pi";
}

function launchAwakenedPi(command: string, args: string[], options: { cwd: string; env: NodeJS.ProcessEnv }): Promise<void> {
  const child = spawn(command, args, { cwd: options.cwd, env: options.env, detached: true, stdio: "ignore" });
  return new Promise((resolve, reject) => {
    child.once("spawn", () => {
      child.unref();
      resolve();
    });
    child.once("error", reject);
  });
}

export function createAgentBridge(
  pi: ExtensionAPI,
  client = new BridgeClient(),
  awakenLauncher: AwakenLauncher = launchAwakenedPi,
  awakenScheduler: AwakenScheduler = scheduleAwakenCheck,
) {
  const openIntents = new Map<string, MutationIntent>();
  const openLeases = new Map<string, LeaseEntry>();
  const deliveredInRuntime = new Set<string>();
  const pendingAcknowledgements = new Set<string>();
  let actor: ActorRecord | undefined;
  let sessionCtx: ExtensionContext | undefined;
  let heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  let jjTimer: ReturnType<typeof setInterval> | undefined;
  let mailboxTimer: ReturnType<typeof setInterval> | undefined;
  let leaseRenewTimer: ReturnType<typeof setInterval> | undefined;
  let mailboxPolling = false;
  let heartbeatRunning = false;
  let compacting = false;
  let backgroundOutage = false;
  const backgroundRecovered = new Set<"heartbeat" | "mailbox">();
  const backgroundRetry = {
    heartbeat: { failures: 0, nextAt: 0 },
    mailbox: { failures: 0, nextAt: 0 },
  };
  let generation = 0;
  let clientSequence = 0;
  let sessionEventSequence = 0;
  let declarationSequence = 0;
  let currentTurnIndex: number | undefined;
  let recoveryPromise: Promise<void> | undefined;
  let reconcilingLeases = false;
  let renewingLeases: Promise<void> | undefined;
  let lastError = "";
  let selectedWorkUnit: WorkUnit | undefined;
  let selectedDirection: Direction | undefined;
  // Test results are emitted by the Pi tool/runtime boundary. Keep only results
  // captured after the last checkpoint; the daemon assigns evidence ordinals.
  let capturedTestResults: Array<{ id: string; outcome: "passed" | "failed" | "blocked" }> = [];
  const verificationRuns = new Map<string, { command: string; cwd: string; startedAt: Date }>();

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

  function backgroundAttemptAllowed(kind: "heartbeat" | "mailbox"): boolean {
    return Date.now() >= backgroundRetry[kind].nextAt;
  }

  function backgroundFailed(kind: "heartbeat" | "mailbox", error: unknown) {
    const retry = backgroundRetry[kind];
    retry.failures += 1;
    retry.nextAt = Date.now() + Math.min(2 ** (retry.failures - 1) * 2_000, BACKGROUND_RETRY_MAX_MS);
    backgroundRecovered.clear();
    if (backgroundOutage) return;
    backgroundOutage = true;
    const message = error instanceof Error ? error.message : String(error);
    sessionCtx?.ui.notify(`Agent Bridge background connection unavailable; retrying quietly (${message})`, "warning");
  }

  function backgroundSucceeded(kind: "heartbeat" | "mailbox") {
    const retry = backgroundRetry[kind];
    retry.failures = 0;
    retry.nextAt = 0;
    if (!backgroundOutage) return;
    backgroundRecovered.add(kind);
    if (backgroundRecovered.size === 2) {
      backgroundOutage = false;
      backgroundRecovered.clear();
    }
  }

  function ordinaryPresenceState(): ActorRecord["state"] {
    return sessionCtx?.isIdle?.() === false ? "active" : "waiting";
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
            state: actor.state === "stealth" ? "stealth" : ordinaryPresenceState(),
            heartbeat_at: now,
            generation,
          },
        });
        if (!reconcilingLeases) await reconcileLeases(true);
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

  function waitForRetry(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new Error("tool call canceled while waiting for mutation lease"));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        signal?.removeEventListener("abort", abort);
        resolve();
      }, LEASE_RETRY_MS);
      const abort = () => {
        clearTimeout(timer);
        signal?.removeEventListener("abort", abort);
        reject(new Error("tool call canceled while waiting for mutation lease"));
      };
      signal?.addEventListener("abort", abort, { once: true });
    });
  }

  async function releaseLease(entry: LeaseEntry): Promise<void> {
    await call("mutation_lease.release", {
      lease_uuid: entry.lease.lease_uuid,
      fencing_token: entry.lease.fencing_token,
      actor_uuid: entry.lease.actor_uuid,
      generation: entry.lease.generation,
    });
  }

  async function admitMutationLease(intent: MutationIntent, signal?: AbortSignal): Promise<MutationLease> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    const request = {
      lease_uuid: randomUUID(),
      fencing_token: randomUUID(),
      actor_uuid: actor.address,
      generation,
      repository_uuid: canonicalUUID(actor.repository_uuid, "Repository UUID"),
      workspace_uuid: canonicalUUID(actor.workspace_uuid, "Workspace UUID"),
      intent_id: intent.id,
      tool_call_id: intent.tool_call_id,
      paths: intent.paths,
    };
    const entry: LeaseEntry = {
      lease: {
        ...request,
        paths: intent.paths,
        granted_at: "",
        renewed_at: "",
        expires_at: "",
        hard_deadline: "",
        state: "waiting",
      },
      renewalFailures: 0,
    };
    openLeases.set(intent.tool_call_id, entry);
    for (;;) {
      if (openLeases.get(intent.tool_call_id) !== entry) throw new Error("tool call canceled during lease admission");
      if (signal?.aborted) {
        await closeLease(intent.tool_call_id);
        throw new Error("tool call canceled before mutation lease grant");
      }
      let result: MutationLeaseResult;
      try {
        result = await call<MutationLeaseResult>("mutation_lease.acquire", request);
      } catch (error) {
        await closeLease(intent.tool_call_id);
        throw new Error(`mutation lease admission unavailable: ${error instanceof Error ? error.message : String(error)}`);
      }
      if (openLeases.get(intent.tool_call_id) !== entry) {
        if (result.lease) {
          entry.lease = result.lease;
          await releaseDetachedLease(entry);
        }
        throw new Error("tool call canceled during lease admission");
      }
      if (result.decision === "grant" && result.lease?.state === "active") {
        entry.lease = result.lease;
        entry.renewalFailures = 0;
        if (signal?.aborted) {
          await closeLease(intent.tool_call_id);
          throw new Error("tool call canceled before mutation lease grant");
        }
        return result.lease;
      }
      if (result.decision === "block") {
        await closeLease(intent.tool_call_id);
        throw new Error(result.reason || "mutation lease admission blocked");
      }
      if (result.decision !== "wait") {
        await closeLease(intent.tool_call_id);
        throw new Error(result.reason || "unknown mutation lease admission decision");
      }
      if (result.lease) {
        entry.lease = result.lease;
        entry.renewalFailures = 0;
      }
      try {
        await waitForRetry(signal);
      } catch (error) {
        await closeLease(intent.tool_call_id);
        throw error;
      }
    }
  }

  async function reconcileLeases(useRawClient = false): Promise<void> {
    if (!actor || reconcilingLeases) return;
    reconcilingLeases = true;
    try {
      await reconcileLeasesImpl(useRawClient);
    } finally {
      reconcilingLeases = false;
    }
  }

  async function reconcileLeasesImpl(useRawClient = false): Promise<void> {
    if (!actor) return;
    // An actor rooted above multiple repositories can hold mutation-scoped
    // leases in several workspaces. Reconcile by fenced actor identity, not by
    // the actor's current workspace.
    const scope = { actor_uuid: actor.address };
    const invoke = <T>(method: string, params: unknown): Promise<T> =>
      useRawClient ? client.call<T>(method, params) : call<T>(method, params);
    const result = await invoke<{ leases?: MutationLease[] }>("mutation_lease.list", scope);
    // Preserve claims that this runtime can still finish. The fencing tuple is
    // the identity; tool-call IDs alone are not safe across reconnects.
    for (const lease of result.leases ?? []) {
      if (lease.actor_uuid !== actor.address) continue;
      const local = [...openLeases.values()].find(
        (entry) =>
          entry.lease.lease_uuid === lease.lease_uuid &&
          entry.lease.fencing_token === lease.fencing_token &&
          entry.lease.actor_uuid === lease.actor_uuid &&
          entry.lease.generation === lease.generation &&
          (entry.lease.state === "active" || entry.lease.state === "waiting"),
      );
      if (local) {
        local.lease = lease;
        local.renewalFailures = 0;
        continue;
      }
      await invoke("mutation_lease.release", {
        lease_uuid: lease.lease_uuid,
        fencing_token: lease.fencing_token,
        actor_uuid: lease.actor_uuid,
        generation: lease.generation,
      }).catch((error) => reportError(sessionCtx, error));
    }
  }

  function detachLease(toolCallId: string): LeaseEntry | undefined {
    const entry = openLeases.get(toolCallId);
    if (entry) openLeases.delete(toolCallId);
    return entry;
  }

  async function releaseDetachedLease(entry: LeaseEntry | undefined): Promise<void> {
    if (entry) await releaseLease(entry).catch((error) => reportError(sessionCtx, error));
  }

  async function closeLease(toolCallId: string): Promise<void> {
    await releaseDetachedLease(detachLease(toolCallId));
  }

  async function handleLeaseRenewalFailure(toolCallId: string, entry: LeaseEntry, reason: string): Promise<void> {
    if (openLeases.get(toolCallId) !== entry) return;
    entry.renewalFailures += 1;
    const expiry = Date.parse(entry.lease.expires_at);
    const unsafeToWait = !Number.isFinite(expiry) || Date.now() + LEASE_RENEW_MS >= expiry;
    reportError(
      sessionCtx,
      new Error(
        `mutation lease renewal failed (${entry.renewalFailures})${unsafeToWait ? "; lease expiry is imminent" : "; retrying before lease expiry"}: ${reason}`,
      ),
    );
    if (!unsafeToWait) return;
    sessionCtx?.abort();
    await closeLease(toolCallId);
  }

  async function renewOpenLeases(): Promise<void> {
    if (renewingLeases) return renewingLeases;
    renewingLeases = (async () => {
      if (!actor) return;
      for (const [toolCallId, entry] of openLeases) {
        if (openLeases.get(toolCallId) !== entry || entry.lease.state !== "active") continue;
        try {
          const result = await call<MutationLeaseResult>("mutation_lease.renew", {
            lease_uuid: entry.lease.lease_uuid,
            fencing_token: entry.lease.fencing_token,
            actor_uuid: actor.address,
            generation,
            repository_uuid: entry.lease.repository_uuid,
            workspace_uuid: entry.lease.workspace_uuid,
            intent_id: entry.lease.intent_id,
            tool_call_id: entry.lease.tool_call_id,
            paths: entry.lease.paths,
          });
          // The result may arrive after tool_result closed this entry. That is
          // an expected race, not a failed renewal and not an orphan to delete.
          if (openLeases.get(toolCallId) !== entry) continue;
          if (result.decision === "grant" && result.lease) {
            entry.lease = result.lease;
            entry.renewalFailures = 0;
            continue;
          }
          if (result.decision === "block") {
            reportError(sessionCtx, new Error(result.reason || "mutation lease renewal was blocked"));
            sessionCtx?.abort();
            await closeLease(toolCallId);
            continue;
          }
          await handleLeaseRenewalFailure(toolCallId, entry, result.reason || "not granted");
        } catch (error) {
          if (openLeases.get(toolCallId) !== entry) continue;
          await handleLeaseRenewalFailure(toolCallId, entry, error instanceof Error ? error.message : String(error));
        }
      }
    })().finally(() => {
      renewingLeases = undefined;
    });
    return renewingLeases;
  }

  async function heartbeat(state?: ActorRecord["state"]) {
    const explicitState = state !== undefined;
    if (!actor || heartbeatRunning || (!explicitState && !backgroundAttemptAllowed("heartbeat"))) return;
    heartbeatRunning = true;
    try {
      actor = await call<ActorRecord>("actor.heartbeat", {
        address: actor.address,
        state: state ?? (actor.state === "stealth" ? "stealth" : ordinaryPresenceState()),
        cwd: sessionCtx?.cwd,
        generation,
      });
      backgroundSucceeded("heartbeat");
    } catch (error) {
      if (explicitState) reportError(sessionCtx, error);
      else backgroundFailed("heartbeat", error);
    } finally {
      heartbeatRunning = false;
    }
  }

  async function refreshVCS() {
    if (!actor || !sessionCtx || compacting || heartbeatRunning || !backgroundAttemptAllowed("heartbeat")) return;
    heartbeatRunning = true;
    try {
      const [git, jj] = await Promise.all([inspectGit(pi, sessionCtx.cwd), inspectJj(pi, sessionCtx.cwd)]);
      actor = await call<ActorRecord>("actor.heartbeat", {
        address: actor.address,
        state: actor.state === "stealth" ? "stealth" : ordinaryPresenceState(),
        cwd: sessionCtx.cwd,
        git,
        jj,
        generation,
      });
      backgroundSucceeded("heartbeat");
    } catch (error) {
      backgroundFailed("heartbeat", error);
    } finally {
      heartbeatRunning = false;
    }
  }

  async function pollMailbox() {
    if (!actor || mailboxPolling || compacting || !backgroundAttemptAllowed("mailbox")) return;
    mailboxPolling = true;
    try {
      const result = await call<PollResult>("mailbox.poll", { actor: actor.address, limit: 100 });
      // A poll already in flight when compaction begins must not inject a steer
      // turn. Messages remain durable and unacknowledged for the next poll.
      if (compacting) return;
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
                : message.kind === "external_change"
                  ? "This source is non-addressable. Do not reply; re-read the affected path before writing."
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
      backgroundSucceeded("mailbox");
    } catch (error) {
      backgroundFailed("mailbox", error);
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

  function canonicalUUID(value: unknown, label: string): string {
    if (
      typeof value !== "string" ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value) ||
      value === "00000000-0000-0000-0000-000000000000"
    ) {
      throw new Error(`${label} must be a canonical UUID`);
    }
    return value;
  }

  function requireWorkUnitScope(): { repository_uuid: string; workspace_uuid: string } {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    return {
      repository_uuid: canonicalUUID(actor.repository_uuid, "Repository UUID"),
      workspace_uuid: canonicalUUID(actor.workspace_uuid, "Workspace UUID"),
    };
  }

  function normalizeWorkUnit(result: unknown): WorkUnit {
    const candidate = (result as { work_unit?: unknown } | undefined)?.work_unit ?? result;
    if (!candidate || typeof candidate !== "object") throw new Error("Daemon returned no WorkUnit");
    const unit = candidate as Partial<WorkUnit>;
    if (typeof unit.objective !== "string" || typeof unit.state !== "string") throw new Error("Daemon returned an invalid WorkUnit");
    canonicalUUID(unit.work_unit_uuid, "WorkUnit UUID");
    canonicalUUID(unit.repository_uuid, "Repository UUID");
    canonicalUUID(unit.workspace_uuid, "Workspace UUID");
    return unit as WorkUnit;
  }

  function validateWorkUnit(unit: WorkUnit): WorkUnit {
    const scope = requireWorkUnitScope();
    if (unit.repository_uuid !== scope.repository_uuid || unit.workspace_uuid !== scope.workspace_uuid) {
      throw new Error("WorkUnit belongs to a different repository or workspace");
    }
    return unit;
  }

  function normalizeDirection(result: unknown): Direction {
    const direction = ((result as { direction?: unknown } | undefined)?.direction ?? result) as Direction;
    if (!direction || typeof direction !== "object") throw new Error("Daemon returned no Direction");
    canonicalUUID(direction.direction_uuid, "Direction UUID");
    if (typeof direction.objective !== "string" || typeof direction.state !== "string")
      throw new Error("Daemon returned an invalid Direction");
    return direction;
  }

  async function fetchDirection(uuid: string): Promise<Direction> {
    const directionUUID = canonicalUUID(uuid, "Direction UUID");
    return normalizeDirection(await call("direction.get", { direction_uuid: directionUUID }));
  }

  async function fetchDirectionStatus(uuid: string): Promise<DirectionStatus> {
    const directionUUID = canonicalUUID(uuid, "Direction UUID");
    const result = await call<DirectionStatus>("direction.status", { direction_uuid: directionUUID });
    const direction = normalizeDirection(result);
    if (!Array.isArray(result.work_units)) throw new Error("Daemon returned an invalid Direction status");
    return { ...result, direction, work_units: result.work_units as WorkUnit[] };
  }

  function formatDirectionStatus(status: DirectionStatus): string {
    const counts = new Map<string, number>();
    for (const unit of status.work_units) {
      const scope = unit.workspace_kind && unit.workspace_root ? `@${unit.workspace_kind}:${unit.workspace_root}` : "";
      const key = `${unit.state}${scope}`;
      counts.set(key, (counts.get(key) ?? 0) + 1);
    }
    const summary = [...counts.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([label, count]) => {
        const separator = label.indexOf("@");
        return separator < 0 ? `${label}=${count}` : `${label.slice(0, separator)}=${count}${label.slice(separator)}`;
      })
      .join(" ");
    const participants = (status.participants ?? [])
      .map(
        (participant) =>
          `${participant.live ? "live" : "dead"}:${participant.alias ? `@${participant.alias}` : participant.actor.slice(0, 8)}`,
      )
      .join(",");
    const collisions = status.open_collisions ? ` · collisions=${status.open_collisions}` : "";
    const checkpoints = status.latest_checkpoints?.length ? ` · checkpoints=${status.latest_checkpoints.length}` : "";
    const people = participants ? ` · participants=${participants}` : "";
    return `${status.direction.objective} · ${status.direction.state} · WorkUnits ${summary || "none"}${people}${collisions}${checkpoints}`;
  }

  async function fetchSelectedDirection(): Promise<Direction | undefined> {
    if (!selectedDirection) return undefined;
    try {
      selectedDirection = await fetchDirection(selectedDirection.direction_uuid);
      return selectedDirection;
    } catch (error) {
      selectedDirection = undefined;
      throw new Error(`Selected Direction cleared: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  async function fetchWorkUnit(workUnitUUID: string): Promise<WorkUnit> {
    const uuid = canonicalUUID(workUnitUUID, "WorkUnit UUID");
    return validateWorkUnit(normalizeWorkUnit(await call("provenance.work_unit", { work_unit_uuid: uuid })));
  }

  async function selectWorkUnit(unit: WorkUnit): Promise<WorkUnit> {
    selectedWorkUnit = validateWorkUnit(unit);
    return selectedWorkUnit;
  }

  function persistSelection() {
    pi.appendEntry("agent-bridge-coordination", {
      direction_uuid: selectedDirection?.direction_uuid,
      work_unit_uuid: selectedWorkUnit?.work_unit_uuid,
    });
  }

  function savedSelection(ctx: ExtensionContext): { direction_uuid?: string; work_unit_uuid?: string } | undefined {
    const entries = (ctx as ExtensionContext & { sessionManager?: { getEntries?: () => unknown[] } }).sessionManager?.getEntries?.() ?? [];
    for (let index = entries.length - 1; index >= 0; index -= 1) {
      const entry = entries[index] as { customType?: string; data?: unknown };
      if (entry.customType !== "agent-bridge-coordination" || !entry.data || typeof entry.data !== "object") continue;
      const data = entry.data as { direction_uuid?: unknown; work_unit_uuid?: unknown };
      return {
        direction_uuid: typeof data.direction_uuid === "string" ? data.direction_uuid : undefined,
        work_unit_uuid: typeof data.work_unit_uuid === "string" ? data.work_unit_uuid : undefined,
      };
    }
    return undefined;
  }

  function isVerificationKind(kind: string): boolean {
    return /^(test|build|runtime)$/i.test(kind.trim());
  }

  function claimFor(
    kind: string,
    statement: string | undefined,
    evidence: Array<{ id: string; outcome: "passed" | "failed" | "blocked" }>,
  ) {
    const normalizedKind = kind.trim().toLowerCase();
    const claimKind = isVerificationKind(normalizedKind) ? normalizedKind : "summary";
    const explicitStatus = /^failed$/i.test(normalizedKind) ? "failed" : /^blocked$/i.test(normalizedKind) ? "blocked" : undefined;
    const matching = explicitStatus
      ? evidence.filter((result) => result.outcome === explicitStatus)
      : isVerificationKind(normalizedKind)
        ? evidence.filter(
            (result) =>
              result.outcome ===
              (evidence.some((item) => item.outcome === "passed")
                ? "passed"
                : evidence.some((item) => item.outcome === "failed")
                  ? "failed"
                  : "blocked"),
          )
        : [];
    const status =
      explicitStatus ?? (matching.length > 0 ? (matching[0]?.outcome === "passed" ? "verified" : matching[0]?.outcome) : "asserted");
    return [
      {
        kind: claimKind,
        statement: statement?.trim() || `${normalizedKind} checkpoint`,
        status,
        evidence: matching.map((_, ordinal) => ({ kind: "test_result", ordinal })),
      },
    ];
  }

  function pollingSleepCommand(command: string): boolean {
    return /^\s*sleep\s+(?:\d+(?:\.\d+)?|\.\d+)(?:[smhd])?(?:\s*(?:;|&&)\s*(?:echo|printf)\b[^;&|]*)?\s*$/i.test(command);
  }

  function verificationCommand(command: string): boolean {
    const trimmed = command.trim();
    const afterCd = trimmed.match(/^cd\s+(?:"[^"]+"|'[^']+'|[^\s;&|]+)\s*&&\s*(.+)$/s)?.[1] ?? trimmed;
    const words = afterCd.trim().split(/\s+/);
    const first = words[0]?.toLowerCase();
    let second = words[1]?.toLowerCase();
    let third = words[2]?.toLowerCase();
    if ((first === "npm" || first === "pnpm") && (second === "--dir" || second === "--prefix") && words[2]) {
      second = words[3]?.toLowerCase();
      third = words[4]?.toLowerCase();
    }
    if (first === "go" && ["test", "build", "run"].includes(second ?? "")) return true;
    if (
      ["bun", "npm", "pnpm"].includes(first ?? "") &&
      (second === "test" || (second === "run" && ["test", "typecheck", "build"].includes(third ?? "")))
    )
      return true;
    if (
      ["vitest", "jest", "pytest"].includes(first ?? "") ||
      (first === "cargo" && ["test", "build", "run"].includes(second ?? "")) ||
      ((first === "npm" || first === "pnpm") && second === "start")
    )
      return true;
    if (first === "tsc" || first === "typecheck") return true;
    return false;
  }

  function bashResultMetadata(
    details: { truncation?: { truncated?: boolean } | null } | undefined,
    content: unknown,
    isError: boolean,
  ): { exitCode?: number; output: string; truncated: boolean } {
    const output = Array.isArray(content)
      ? content
          .filter((part) => part && typeof part === "object" && (part as { type?: string }).type === "text")
          .map((part) => String((part as { text?: unknown }).text ?? ""))
          .join("\n")
      : "";
    const exitMatch = output.match(/exit code: (-?\d+)/i);
    const blocked = /\b(cancelled|canceled|aborted|timed out)\b/i.test(output);
    return {
      exitCode: exitMatch?.[1] === undefined ? (blocked ? undefined : isError ? 1 : 0) : Number(exitMatch[1]),
      output,
      truncated: details?.truncation?.truncated === true,
    };
  }

  async function requestCheckpoint(
    declaredBy: "agent" | "human",
    kind: string,
    workUnitUUID?: string,
    metadata?: Record<string, string>,
    statement?: string,
    declarationId?: string,
    tickets?: Array<Record<string, unknown>>,
  ): Promise<CheckpointRequest> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    // An explicit UUID is authoritative. An implicit selection is re-read so stale
    // or cross-scope state can never receive a checkpoint.
    let linkedWorkUnit: string | undefined;
    if (workUnitUUID) {
      linkedWorkUnit = (await fetchWorkUnit(canonicalUUID(workUnitUUID, "WorkUnit UUID"))).work_unit_uuid;
    } else if (selectedWorkUnit) {
      try {
        linkedWorkUnit = (await fetchWorkUnit(selectedWorkUnit.work_unit_uuid)).work_unit_uuid;
      } catch (error) {
        selectedWorkUnit = undefined;
        throw error;
      }
    }
    const boundary = declarationId ?? `${actor.address}:${generation}:human:${++declarationSequence}`;
    const id = createHash("sha256").update(boundary).digest("hex");
    const evidence = [...capturedTestResults];
    const normalizedKind = kind.trim();
    const checkpointEvidence = /^failed$/i.test(normalizedKind)
      ? evidence.filter((result) => result.outcome === "failed")
      : /^blocked$/i.test(normalizedKind)
        ? evidence.filter((result) => result.outcome === "blocked")
        : isVerificationKind(normalizedKind)
          ? evidence.filter(
              (result) =>
                result.outcome ===
                (evidence.some((item) => item.outcome === "passed")
                  ? "passed"
                  : evidence.some((item) => item.outcome === "failed")
                    ? "failed"
                    : "blocked"),
            )
          : [];
    const evidenceIds = checkpointEvidence.map((result) => result.id);
    const result = await call<CheckpointRequest>("checkpoint.request", {
      request: {
        id,
        actor: actor.address,
        declared_by: declaredBy,
        session_generation: generation,
        repository_uuid: actor.repository_uuid,
        workspace_uuid: actor.workspace_uuid,
        work_unit_uuid: linkedWorkUnit,
        tickets,
        checkpoint_kind: kind,
        claims: claimFor(kind, statement, checkpointEvidence),
        test_result_ids: evidenceIds,
        boundary_event_id: boundary,
        boundary_type: "explicit",
        turn_id: currentTurnIndex === undefined ? undefined : `${actor.address}:${generation}:turn:${currentTurnIndex}`,
        turn_index: currentTurnIndex,
        git: actor.git,
        jj: actor.jj,
        metadata,
      },
    });
    if (!result.test_result_ids?.length && evidenceIds.length > 0) result.test_result_ids = evidenceIds;
    capturedTestResults = [];
    return result;
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

  async function activeActors(
    filters: { directory?: string; repository_uuid?: string; workspace_uuid?: string } = {},
  ): Promise<ActorRecord[]> {
    return (await call<SessionsResult>("sessions.list", { include_stale: false, ...filters })).actors;
  }

  async function dormantPiActor(selector: string): Promise<ActorRecord> {
    const normalized = selector.trim().replace(/^@/, "");
    if (!normalized) throw new Error("Awaken target is required");
    const actors = (await call<SessionsResult>("sessions.list", { include_stale: true })).actors;
    const directActorUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(normalized);
    const identityMatches = actors.filter(
      (candidate) =>
        candidate.harness === "pi" &&
        (candidate.address === normalized ||
          (!directActorUUID && (candidate.session_uuid === normalized || candidate.alias === normalized))),
    );
    const live = identityMatches.find((candidate) => candidate.state !== "dead");
    if (live) throw new Error(`Refusing to awaken live actor ${live.address}; bridge_awaken only accepts dead actors`);
    const matches = identityMatches.filter((candidate) => candidate.state === "dead");
    if (matches.length !== 1) throw new Error(`Expected one dead Pi actor for ${JSON.stringify(selector)}`);
    const target = matches[0]!;
    if (!target.session_file || !target.cwd) throw new Error("Dead Pi session has no resumable session file and working directory");
    const scope = requireWorkUnitScope();
    if (target.repository_uuid !== scope.repository_uuid || target.workspace_uuid !== scope.workspace_uuid) {
      throw new Error("Dead Pi session belongs to a different repository or workspace");
    }
    return target;
  }

  async function sessions(): Promise<string[]> {
    const [actors, herdrAgents] = await Promise.all([activeActors(), listHerdrAgents(pi)]);
    const bridge = { actors };
    const lines = bridge.actors.map((candidate) => {
      const herdr = herdrAgents.find(
        (entry) => entry.agentSession === candidate.session_file || (candidate.pane_id && entry.paneId === candidate.pane_id),
      );
      const identity = candidate.alias ? `@${candidate.alias} (${candidate.address})` : candidate.address;
      const piSession = candidate.session_file
        ? ` pi_session=${
            basename(candidate.session_file)
              .replace(/\.jsonl$/, "")
              .split("_")
              .pop() ?? "unknown"
          }`
        : "";
      const git = candidate.git
        ? ` git=${candidate.git.branch ?? candidate.git.head?.slice(0, 12) ?? "unborn"} worktree=${candidate.git.worktree_root}`
        : " git=none";
      const jj = candidate.jj ? ` jj=${candidate.jj.change_id.slice(0, 12)} workspace=${candidate.jj.workspace_root}` : " jj=none";
      return `${identity} ${herdr?.status ?? candidate.state}${piSession} cwd=${candidate.cwd}${git}${jj}${herdr ? ` pane=${herdr.paneId}` : ""}`;
    });
    for (const herdr of herdrAgents) {
      if (bridge.actors.some((candidate) => candidate.session_file === herdr.agentSession || candidate.pane_id === herdr.paneId)) continue;
      lines.push(`${herdr.agent}:unregistered ${herdr.status} cwd=${herdr.cwd ?? "unknown"} pane=${herdr.paneId}`);
    }
    return lines.sort();
  }

  pi.registerTool({
    name: "bridge_stealth",
    label: "Agent Bridge Stealth",
    description: "Temporarily stop receiving mailbox messages while keeping this actor registered and its messages queued.",
    promptSnippet: "Enable or disable Agent Bridge stealth mode.",
    parameters: Type.Object({
      enabled: Type.Boolean({ description: "Whether to suppress mailbox delivery." }),
    }),
    async execute(_toolCallId, params) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      await heartbeat(params.enabled ? "stealth" : ordinaryPresenceState());
      return {
        content: [{ type: "text", text: params.enabled ? "Agent Bridge stealth mode enabled." : "Agent Bridge stealth mode disabled." }],
        details: { enabled: params.enabled, state: actor.state },
      };
    },
  });

  const stealthUsage = "Usage: /stealth on | /stealth off | /stealth status";
  pi.registerCommand("stealth", {
    description: "Enable, disable, or inspect Agent Bridge stealth mode",
    getArgumentCompletions: (prefix: string) => {
      if (prefix.trim().includes(" ")) return null;
      const normalized = prefix.trim();
      return ["on", "off", "status"].filter((value) => value.startsWith(normalized)).map((value) => ({ value, label: value }));
    },
    handler: async (args: string, ctx: ExtensionContext) => {
      const input = String(args ?? "").trim();
      const parts = input ? input.split(/\s+/) : [];
      if (parts.length !== 1 || !["on", "off", "status"].includes(parts[0] ?? "")) {
        ctx.ui.notify(stealthUsage, "warning");
        return;
      }
      try {
        if (!actor) throw new Error("Agent Bridge is not attached");
        if (parts[0] === "status") {
          ctx.ui.notify(`Agent Bridge stealth mode ${actor.state === "stealth" ? "enabled" : "disabled"}.`, "info");
          return;
        }
        await heartbeat(parts[0] === "on" ? "stealth" : ordinaryPresenceState());
        ctx.ui.notify(`Agent Bridge stealth mode ${parts[0] === "on" ? "enabled" : "disabled"}.`, "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
  });

  const awakenTool = {
    name: "bridge_awaken",
    label: "Agent Bridge Awaken",
    description:
      "WARNING: only awaken a confirmed dead actor. Refuses live actors. Uses an Agent Bridge actor address first; legacy Pi session UUID and alias selectors remain supported. The original session remains dead; the child gets a new identity and can communicate directly through Agent Bridge.",
    promptSnippet: "Awaken a dead Pi agent when the user explicitly requests it.",
    parameters: Type.Object({
      target: Type.String({
        description:
          "Agent Bridge actor address (preferred), or legacy Pi session UUID/@alias. Live actors are refused; only a confirmed dead actor may be awakened.",
      }),
      request: Type.String({ description: "Bounded question or task for the awakened child." }),
      workUnitUUID: Type.Optional(Type.String({ description: "Optional same-scope WorkUnit for the child." })),
    }),
    async execute(_toolCallId: string, params: Record<string, any>) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      const request = String(params.request ?? "").trim();
      if (!request || request.length > 4_000) throw new Error("Awaken request must contain 1 to 4000 characters");
      const target = await dormantPiActor(String(params.target ?? ""));
      const sessionFile = target.session_file;
      if (!sessionFile) throw new Error("Dead Pi session has no resumable session file");
      let workUnitUUID: string | undefined;
      if (params.workUnitUUID) {
        workUnitUUID = (await fetchWorkUnit(canonicalUUID(params.workUnitUUID, "WorkUnit UUID"))).work_unit_uuid;
      } else if (selectedWorkUnit) {
        workUnitUUID = (await fetchWorkUnit(selectedWorkUnit.work_unit_uuid)).work_unit_uuid;
      }
      const launchUUID = randomUUID();
      await call("launch.create", { launch_uuid: launchUUID, parent_actor_uuids: [actor.address] });
      try {
        if (workUnitUUID) await call("launch.attach_work_unit", { launch_uuid: launchUUID, work_unit_uuid: workUnitUUID });
        const prompt = [
          "You are an awakened Agent Bridge child, forked from a dead Pi session.",
          "Review the current repository state and your durable mailbox before acting; your prior transcript may be stale.",
          "Coordinate directly with your parent through Agent Bridge when needed.",
          `Awakening request: ${request}`,
        ].join("\n\n");
        await awakenLauncher(piExecutable(), ["--fork", sessionFile, "--name", `awakened-${target.session_uuid.slice(0, 8)}`, prompt], {
          cwd: target.cwd,
          env: { ...process.env, AGENT_BRIDGE_LAUNCH_UUID: launchUUID, AGENT_BRIDGE_WORK_UNIT_UUID: workUnitUUID },
        });
        awakenScheduler(() => {
          void (async () => {
            const launch = await call<{ child_actor_uuid?: string; terminated_at?: string }>("launch.get", { launch_uuid: launchUUID });
            if (!launch.child_actor_uuid && !launch.terminated_at) {
              await call("launch.terminate", {
                launch_uuid: launchUUID,
                reason: `child did not register within ${AWAKEN_ATTACH_TIMEOUT_MS}ms`,
              });
            }
          })().catch((error) => reportError(sessionCtx, error));
        }, AWAKEN_ATTACH_TIMEOUT_MS);
      } catch (error) {
        await call("launch.terminate", { launch_uuid: launchUUID, reason: error instanceof Error ? error.message : String(error) });
        throw error;
      }
      return {
        content: [
          {
            type: "text",
            text: `Awakening launch ${launchUUID} started for ${target.address}. It will register as a new child identity and can then receive direct bridge messages.`,
          },
        ],
        details: { launch_uuid: launchUUID, source_actor: target.address, work_unit_uuid: workUnitUUID },
      };
    },
  };
  pi.registerTool(awakenTool);

  pi.registerCommand("awaken", {
    description: "Fork a dead Pi session into an awakened child: /awaken <actor> <request>",
    handler: async (args: string, ctx: ExtensionContext) => {
      const [target, ...request] = args.trim().split(/\s+/);
      if (!target || request.length === 0) {
        ctx.ui.notify("Usage: /awaken <dead Pi actor> <bounded request>", "warning");
        return;
      }
      try {
        const result = await awakenTool.execute("/awaken", { target, request: request.join(" ") });
        ctx.ui.notify(result?.content?.[0]?.text ?? "Awakening started.", "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
  });

  pi.registerTool({
    name: "bridge_message",
    label: "Agent Bridge Message",
    description:
      "Send an ordered durable coordination message to a peer by Agent Bridge actor UUID/address, @alias, or @ID. Pass an actor UUID from the Agent Bridge binding or bridge_context directly; Pi session UUIDs are only legacy recovery metadata.",
    promptSnippet: "Send ordered durable coordination messages to peer agents.",
    promptGuidelines: [
      "Use bridge_message to coordinate with peers named in Agent Bridge collisions; never revert unfamiliar shared-workspace edits without coordinating first.",
    ],
    parameters: Type.Object({
      to: Type.String({
        description: "Agent Bridge actor UUID/address from the binding or bridge_context, @alias, or @<JJ change ID>.",
      }),
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
    promptSnippet: "Load optional Agent Bridge coordination or provenance tools only when needed.",
    parameters: Type.Object({
      domain: StringEnum(["provenance", "coordination"] as const, { description: "Optional domain to load." }),
    }),
    async execute(_toolCallId, params) {
      const domain = String(params.domain ?? "");
      if (domain !== "provenance" && domain !== "coordination")
        throw new Error(`Unknown Agent Bridge tool domain ${JSON.stringify(domain)}`);
      const names = domain === "provenance" ? ["bridge_provenance"] : ["bridge_context", "bridge_direction", "bridge_work"];
      const active = pi.getActiveTools();
      const added = names.filter((name) => !active.includes(name));
      pi.setActiveTools([...new Set([...active, ...names])]);
      return {
        content: [{ type: "text", text: `Loaded ${names.join(", ")} for this session.` }],
        details: { domain, added },
      };
    },
  });

  function boundedCoordinationText(value: unknown): string {
    const encoded = JSON.stringify(value);
    return encoded.length <= 20_000 ? encoded : `${encoded.slice(0, 19_900)}\n... coordination output truncated; narrow the scope`;
  }

  async function coordinationContext(
    filters: { directory?: string; repository_uuid?: string; workspace_uuid?: string } = {},
  ): Promise<BridgeContext> {
    if (!actor) throw new Error("Agent Bridge is not attached to an active session");
    const scopedFilters =
      filters.directory || filters.repository_uuid || filters.workspace_uuid
        ? filters
        : actor.repository_uuid
          ? { repository_uuid: actor.repository_uuid }
          : { directory: actor.cwd };
    const peers = (await activeActors(scopedFilters)).filter((candidate) => candidate.address !== actor?.address);
    const direction = selectedDirection ? await fetchDirectionStatus(selectedDirection.direction_uuid) : undefined;
    const work_unit = selectedWorkUnit ? await fetchWorkUnit(selectedWorkUnit.work_unit_uuid) : undefined;
    return { self: actor, peers: peers.slice(0, 50), direction, work_unit };
  }

  pi.registerTool({
    name: "bridge_context",
    label: "Agent Bridge Context",
    description: "Return bounded actor, peer, selected Direction, and selected WorkUnit coordination state.",
    parameters: Type.Object({
      directory: Type.Optional(Type.String({ description: "Optional directory filter." })),
      repository_uuid: Type.Optional(Type.String({ description: "Optional repository UUID filter." })),
      workspace_uuid: Type.Optional(Type.String({ description: "Optional workspace UUID filter." })),
      under: Type.Optional(Type.String({ description: "Optional directory subtree filter; translated to the daemon directory scope." })),
    }),
    async execute(_toolCallId, params) {
      if (params.directory && params.under) throw new Error("Use either directory or under, not both");
      const result = await coordinationContext({
        directory: params.under ?? params.directory,
        repository_uuid: params.repository_uuid,
        workspace_uuid: params.workspace_uuid,
      });
      return { content: [{ type: "text", text: boundedCoordinationText(result) }], details: result };
    },
  });

  pi.registerTool({
    name: "bridge_direction",
    label: "Agent Bridge Direction",
    description: "Propose, select, inspect, or transition a Direction using existing daemon RPCs.",
    parameters: Type.Object({
      action: StringEnum(["propose", "use", "status", "transition", "clear"] as const),
      objective: Type.Optional(Type.String()),
      direction_uuid: Type.Optional(Type.String()),
      state: Type.Optional(StringEnum(["active", "paused", "converging", "verified", "completed", "abandoned"] as const)),
    }),
    async execute(_toolCallId, params) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      const action = String(params.action);
      if (action === "clear") {
        selectedDirection = undefined;
        persistSelection();
        return { content: [{ type: "text", text: "Direction selection cleared." }] };
      }
      if (action === "propose") {
        const objective = String(params.objective ?? "").trim();
        if (!objective) throw new Error("Direction objective is required");
        selectedDirection = normalizeDirection(
          await call("direction.create", { direction: { direction_uuid: randomUUID(), objective, created_by: actor.address } }),
        );
      } else if (action === "use") {
        selectedDirection = await fetchDirection(String(params.direction_uuid ?? ""));
      } else if (action === "status") {
        if (!selectedDirection) throw new Error("No Direction selected");
        const status = await fetchDirectionStatus(selectedDirection.direction_uuid);
        selectedDirection = status.direction;
        persistSelection();
        return { content: [{ type: "text", text: boundedCoordinationText(status) }], details: { status } };
      } else if (action === "transition") {
        if (!selectedDirection || !params.state) throw new Error("Selected Direction and state are required");
        selectedDirection = normalizeDirection(
          await call("direction.transition", {
            direction_uuid: selectedDirection.direction_uuid,
            actor: actor.address,
            state: params.state,
          }),
        );
      }
      persistSelection();
      return {
        content: [{ type: "text", text: boundedCoordinationText(selectedDirection) }],
        details: { direction: selectedDirection },
      };
    },
  });

  pi.registerTool({
    name: "bridge_work",
    label: "Agent Bridge WorkUnit",
    description: "Propose or explicitly join, use, leave, transition, inspect, and clear a WorkUnit.",
    parameters: Type.Object({
      action: StringEnum(["propose", "use", "join", "leave", "status", "transition", "clear"] as const),
      objective: Type.Optional(Type.String()),
      work_unit_uuid: Type.Optional(Type.String()),
      state: Type.Optional(StringEnum(["active", "blocked", "verified", "completed", "abandoned"] as const)),
    }),
    async execute(_toolCallId, params) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      const action = String(params.action);
      if (action === "clear") {
        selectedWorkUnit = undefined;
        persistSelection();
        return { content: [{ type: "text", text: "WorkUnit selection cleared." }] };
      }
      if (action === "propose") {
        const objective = String(params.objective ?? "").trim();
        if (!objective) throw new Error("WorkUnit objective is required");
        const scope = requireWorkUnitScope();
        const direction = selectedDirection ? await fetchSelectedDirection() : undefined;
        selectedWorkUnit = validateWorkUnit(
          normalizeWorkUnit(
            await call("work_unit.create", {
              work_unit: {
                work_unit_uuid: randomUUID(),
                ...scope,
                objective,
                created_by: actor.address,
                direction_uuid: direction?.direction_uuid,
              },
            }),
          ),
        );
      } else {
        const unit =
          selectedWorkUnit && !params.work_unit_uuid ? selectedWorkUnit : await fetchWorkUnit(String(params.work_unit_uuid ?? ""));
        if (action === "use" || action === "join") {
          await call("work_unit.join", { work_unit_uuid: unit.work_unit_uuid, actor: actor.address });
          selectedWorkUnit = unit;
        } else if (action === "leave") {
          await call("work_unit.leave", { work_unit_uuid: unit.work_unit_uuid, actor: actor.address });
          if (selectedWorkUnit?.work_unit_uuid === unit.work_unit_uuid) selectedWorkUnit = undefined;
        } else if (action === "status") selectedWorkUnit = await fetchWorkUnit(unit.work_unit_uuid);
        else if (action === "transition") {
          if (!params.state) throw new Error("WorkUnit state is required");
          selectedWorkUnit = normalizeWorkUnit(
            await call("work_unit.transition", { work_unit_uuid: unit.work_unit_uuid, actor: actor.address, state: params.state }),
          );
        }
      }
      persistSelection();
      return { content: [{ type: "text", text: boundedCoordinationText(selectedWorkUnit) }], details: { work_unit: selectedWorkUnit } };
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
    name: "bridge_ticket",
    label: "Agent Bridge Ticket Context",
    description:
      "Store, list, replace, or clear arbitrary local ticket context on the selected Direction or WorkUnit. No provider is inferred and no remote tracker is contacted. Offer to record user-supplied ticket context; say stored only after daemon success.",
    promptSnippet:
      "When the user supplies ticket context, offer to record it on the selected Direction or WorkUnit. Report it stored only after the daemon confirms success.",
    parameters: Type.Object({
      action: Type.Optional(Type.Union([Type.Literal("replace"), Type.Literal("clear"), Type.Literal("list")])),
      target: Type.Optional(Type.Union([Type.Literal("direction"), Type.Literal("work_unit")])),
      tickets: Type.Optional(Type.Array(Type.Record(Type.String(), Type.Any()))),
    }),
    async execute(_toolCallId, params) {
      const action = String(params.action ?? "replace");
      const target = String(params.target ?? (selectedWorkUnit ? "work_unit" : "direction"));
      if (action !== "replace" && action !== "clear" && action !== "list") throw new Error(`Unsupported ticket action: ${action}`);
      const tickets = action === "clear" ? [] : (params.tickets as Array<Record<string, unknown>> | undefined);
      if (action === "replace" && !tickets) throw new Error("tickets is required for replace");
      // Listing is deliberately a read: never send an update with undefined tickets.
      if (action === "list") {
        const direction = target === "direction" ? await fetchSelectedDirection() : undefined;
        const unit =
          target === "work_unit" ? (selectedWorkUnit ? await fetchWorkUnit(selectedWorkUnit.work_unit_uuid) : undefined) : undefined;
        if (!direction && !unit) throw new Error(`No selected ${target} is available`);
        const current = target === "direction" ? direction!.tickets : unit!.tickets;
        return { content: [{ type: "text", text: JSON.stringify(current ?? []) }], details: { tickets: current ?? [] } };
      }
      const direction = target === "direction" ? await fetchSelectedDirection() : undefined;
      const unit =
        target === "work_unit" ? (selectedWorkUnit ? await fetchWorkUnit(selectedWorkUnit.work_unit_uuid) : undefined) : undefined;
      if (!direction && !unit) throw new Error(`No selected ${target} is available`);
      const result =
        target === "direction"
          ? await call("direction.update", { direction_uuid: direction!.direction_uuid, actor: actor?.address, tickets })
          : await call("work_unit.update", { work_unit_uuid: unit!.work_unit_uuid, actor: actor?.address, tickets });
      return {
        content: [{ type: "text", text: `Ticket context ${action === "clear" ? "cleared" : "stored"} on ${target}.` }],
        details: { result },
      };
    },
  });

  const leaseTakeover = {
    name: "bridge_lease_takeover",
    label: "Agent Bridge Lease Takeover",
    description: "Explicitly take over a mutation lease with a new lease UUID and fencing token; the old holder is notified durably.",
    promptSnippet: "Use only when an explicit audited mutation lease takeover is required.",
    parameters: Type.Object({
      predecessorLeaseUUID: Type.String({ description: "Lease UUID being superseded." }),
      reason: Type.String({ description: "Bounded human-readable reason (1-1000 characters)." }),
      source: Type.Optional(Type.Union([Type.Literal("agent"), Type.Literal("human")])),
      workUnitUUID: Type.Optional(Type.String()),
      collisionID: Type.Optional(Type.String()),
    }),
    async execute(_toolCallId: string, params: Record<string, any>) {
      if (!actor) throw new Error("Agent Bridge is not attached to an active session");
      const predecessor = canonicalUUID(String(params.predecessorLeaseUUID ?? ""), "Predecessor lease UUID");
      const reason = String(params.reason ?? "").trim();
      if (!reason || reason.length > 1000) throw new Error("Takeover reason must contain 1 to 1000 characters");
      const result = await call("mutation_lease.takeover", {
        predecessor_lease_uuid: predecessor,
        lease_uuid: randomUUID(),
        fencing_token: randomUUID(),
        requester_actor_uuid: actor.address,
        requester_generation: generation,
        acquisition_source: params.source ?? "agent",
        reason,
        work_unit_uuid: params.workUnitUUID ? canonicalUUID(String(params.workUnitUUID), "WorkUnit UUID") : undefined,
        collision_id: params.collisionID ? canonicalUUID(String(params.collisionID), "Collision UUID") : undefined,
      });
      return {
        content: [{ type: "text", text: `Lease takeover recorded; predecessor ${predecessor} was superseded and its holder notified.` }],
        details: { result },
      };
    },
  };
  pi.registerTool(leaseTakeover);
  pi.registerCommand("lease-takeover", {
    description: "Explicitly take over a mutation lease: /lease-takeover <lease UUID> <reason>",
    handler: async (args: string, ctx: ExtensionContext) => {
      const [predecessor, ...rest] = String(args ?? "")
        .trim()
        .split(/\s+/);
      if (!predecessor || rest.length === 0) return void ctx.ui.notify("Usage: /lease-takeover <lease UUID> <reason>", "warning");
      try {
        const result = await leaseTakeover.execute("/lease-takeover", {
          predecessorLeaseUUID: predecessor,
          reason: rest.join(" "),
          source: "human",
        });
        ctx.ui.notify(result.content?.[0]?.text ?? "Lease takeover recorded.", "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
  });

  pi.registerTool({
    name: "bridge_checkpoint",
    label: "Agent Bridge Checkpoint",
    description:
      "Declare an immutable checkpoint with an optional claim statement. Test/build/runtime claims are verified only with successful captured evidence; otherwise they remain asserted.",
    promptSnippet:
      "Use bridge_checkpoint at a durable boundary; distinguish asserted, verified, failed, and blocked, and never claim tests/build/runtime passed without attached evidence.",
    promptGuidelines: [
      "Use a concise statement to say what changed or was checked.",
      "Verification requires a successful persisted test result; otherwise report the claim as asserted and lacking verification.",
    ],
    parameters: Type.Object({
      kind: Type.String({ description: "Checkpoint kind, for example settled, handoff, test, or manual." }),
      workUnitUUID: Type.Optional(Type.String({ description: "Optional WorkUnit UUID to link this checkpoint to." })),
      metadata: Type.Optional(Type.Record(Type.String(), Type.String(), { description: "Optional authored metadata for this boundary." })),
      statement: Type.Optional(Type.String({ description: "Optional concise claim statement." })),
      tickets: Type.Optional(
        Type.Array(Type.Record(Type.String(), Type.Any(), { description: "Optional local ticket context object maps." })),
      ),
    }),
    async execute(_toolCallId, params) {
      const kind = String(params.kind ?? "").trim();
      if (!kind) throw new Error("Checkpoint kind is required");
      const checkpoint = await requestCheckpoint(
        "agent",
        kind,
        params.workUnitUUID ? String(params.workUnitUUID) : undefined,
        params.metadata,
        params.statement ? String(params.statement) : undefined,
        `bridge:${actor?.address ?? "unknown"}:${generation}:${String(_toolCallId)}`,
        params.tickets as Array<Record<string, unknown>> | undefined,
      );
      const unverified = isVerificationKind(kind) && !checkpoint.test_result_ids?.length;
      return {
        content: [
          {
            type: "text",
            text: `Checkpoint ${checkpoint.id} recorded${unverified ? "; claim asserted and lacks successful captured evidence" : "."}`,
          },
        ],
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

  const directionUsage =
    "Usage: /direction <objective> | /direction status | /direction use <uuid> | /direction start|pause|converge|verify|complete|abandon | /direction clear";
  const workUsage = "Usage: /work <objective> | /work use <uuid> | /work status | /work clear";
  pi.registerCommand("direction", {
    description: "Manage session Direction",
    getArgumentCompletions: (prefix: string) => {
      if (prefix.trim().includes(" ")) return null;
      const normalized = prefix.trim();
      return ["status", "use", "start", "pause", "converge", "verify", "complete", "abandon", "clear"]
        .filter((value) => value.startsWith(normalized))
        .map((value) => ({ value, label: value }));
    },
    handler: async (args: string, ctx: ExtensionContext) => {
      const input = String(args ?? "").trim();
      const [action, ...rest] = input.split(/\s+/);
      try {
        if (!actor) throw new Error("Agent Bridge is not attached to an active session");
        if (action === "status" || !input) {
          if (!selectedDirection) return void ctx.ui.notify("No Direction selected.", "info");
          try {
            const status = await fetchDirectionStatus(selectedDirection.direction_uuid);
            selectedDirection = status.direction;
            return void ctx.ui.notify(formatDirectionStatus(status), "info");
          } catch (error) {
            selectedDirection = undefined;
            throw new Error(`Selected Direction cleared: ${error instanceof Error ? error.message : String(error)}`);
          }
        }
        if (action === "clear") {
          selectedDirection = undefined;
          persistSelection();
          return void ctx.ui.notify("Direction selection cleared.", "info");
        }
        if (action === "use") {
          if (!rest[0]) throw new Error("/direction use requires a Direction UUID");
          selectedDirection = await fetchDirection(rest[0]);
          persistSelection();
          return void ctx.ui.notify(`Selected Direction ${selectedDirection.direction_uuid}.`, "info");
        }
        const transitions: Record<string, string> = {
          start: "active",
          pause: "paused",
          converge: "converging",
          verify: "verified",
          complete: "completed",
          abandon: "abandoned",
        };
        const transition = action === undefined ? undefined : transitions[action];
        if (transition) {
          if (!selectedDirection) throw new Error("No Direction selected.");
          selectedDirection = normalizeDirection(
            await call("direction.transition", {
              direction_uuid: selectedDirection.direction_uuid,
              actor: actor.address,
              state: transition,
            }),
          );
          persistSelection();
          return void ctx.ui.notify(`Direction ${selectedDirection.state}.`, "info");
        }
        if (!input) throw new Error(directionUsage);
        selectedDirection = normalizeDirection(
          await call("direction.create", {
            direction: { direction_uuid: randomUUID(), objective: input, created_by: actor.address },
          }),
        );
        persistSelection();
        ctx.ui.notify(`Created and selected Direction ${selectedDirection.direction_uuid}.`, "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
  });

  pi.registerCommand("work", {
    description: "Create, select, inspect, or clear the session WorkUnit",
    getArgumentCompletions: (prefix: string) => {
      if (prefix.trim().includes(" ")) return null;
      const normalized = prefix.trim();
      return ["use", "status", "clear"].filter((value) => value.startsWith(normalized)).map((value) => ({ value, label: value }));
    },
    handler: async (args: string, ctx: ExtensionContext) => {
      const input = String(args ?? "").trim();
      const [action, ...rest] = input.split(/\s+/);
      try {
        if (!actor) throw new Error("Agent Bridge is not attached to an active session");
        if (action === "status") {
          if (!selectedWorkUnit) {
            ctx.ui.notify("No WorkUnit selected.", "info");
            return;
          }
          try {
            const unit = await fetchWorkUnit(selectedWorkUnit.work_unit_uuid);
            selectedWorkUnit = unit;
            ctx.ui.notify(`WorkUnit ${unit.work_unit_uuid}: ${unit.state} · ${unit.objective}`, "info");
          } catch (error) {
            selectedWorkUnit = undefined;
            throw new Error(`Selected WorkUnit cleared: ${error instanceof Error ? error.message : String(error)}`);
          }
          return;
        }
        if (action === "clear") {
          selectedWorkUnit = undefined;
          persistSelection();
          ctx.ui.notify("WorkUnit selection cleared.", "info");
          return;
        }
        if (action === "use") {
          const uuid = rest[0];
          if (!uuid) throw new Error("/work use requires a WorkUnit UUID");
          const unit = await fetchWorkUnit(uuid);
          await call("work_unit.join", { work_unit_uuid: unit.work_unit_uuid, actor: actor.address });
          await selectWorkUnit(unit);
          persistSelection();
          ctx.ui.notify(`Selected WorkUnit ${unit.work_unit_uuid}.`, "info");
          return;
        }
        const objective = input;
        if (!objective || objective === "use" || objective === "status" || objective === "clear") throw new Error(workUsage);
        const scope = requireWorkUnitScope();
        const direction = await fetchSelectedDirection();
        const unit = normalizeWorkUnit(
          await call("work_unit.create", {
            work_unit: {
              work_unit_uuid: randomUUID(),
              repository_uuid: scope.repository_uuid,
              workspace_uuid: scope.workspace_uuid,
              objective,
              created_by: actor.address,
              direction_uuid: direction?.direction_uuid,
            },
          }),
        );
        validateWorkUnit(unit);
        await call("work_unit.join", { work_unit_uuid: unit.work_unit_uuid, actor: actor.address });
        await selectWorkUnit(unit);
        persistSelection();
        ctx.ui.notify(`Created and selected WorkUnit ${unit.work_unit_uuid}.`, "info");
      } catch (error) {
        ctx.ui.notify(error instanceof Error ? error.message : String(error), "warning");
      }
    },
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
      const input = String(args ?? "").trim();
      const [kind = "manual", ...statementParts] = input ? input.split(/\s+/) : [];
      const statement = statementParts.join(" ") || undefined;
      try {
        const checkpoint = await requestCheckpoint("human", kind, undefined, undefined, statement);
        const unverified = isVerificationKind(kind) && !checkpoint.test_result_ids?.length;
        ctx.ui.notify(
          `Checkpoint ${checkpoint.id} recorded${unverified ? "; claim asserted and lacks successful captured evidence." : "."}`,
          unverified ? "warning" : "info",
        );
      } catch (error) {
        reportError(ctx, error);
      }
    },
  });

  pi.on("before_agent_start", async (event) => {
    let coordination = "";
    if (selectedDirection || selectedWorkUnit) {
      coordination = `\n\nSelected coordination: Direction=${selectedDirection?.direction_uuid ?? "none"} WorkUnit=${selectedWorkUnit?.work_unit_uuid ?? "none"}. Use bridge_context for bounded current state.`;
    }
    return {
      systemPrompt: `${event.systemPrompt}\n\nAgent Bridge uses actor addresses (not Pi session UUIDs) for direct coordination. Do not revert unfamiliar shared-workspace edits; coordinate using bridge_message, then record yield or resolution using bridge_collision. Checkpoint guidance: record proposals, findings, decisions, and handoffs as concise checkpoints; test/build/runtime claims are verified only with successful persisted evidence, otherwise say asserted. Never poll background agents or the mailbox with sleep commands; finish the turn and let event-driven mailbox delivery reactivate the session.${coordination}`,
    };
  });

  pi.on("session_start", async (event, ctx) => {
    sessionCtx = ctx;
    const deferredTools = new Set(["bridge_provenance", "bridge_context", "bridge_direction", "bridge_work"]);
    pi.setActiveTools(pi.getActiveTools().filter((name) => !deferredTools.has(name)));
    generation = Date.now();
    clientSequence = 0;
    sessionEventSequence = 0;
    currentTurnIndex = undefined;
    selectedDirection = undefined;
    selectedWorkUnit = undefined;
    capturedTestResults = [];
    declarationSequence = 0;
    verificationRuns.clear();
    await ensureDaemon(client);
    const sessionId = randomUUID();
    const now = new Date().toISOString();
    const [git, jj] = await Promise.all([inspectGit(pi, ctx.cwd), inspectJj(pi, ctx.cwd)]);
    const launchUUID = process.env.AGENT_BRIDGE_LAUNCH_UUID;
    if (launchUUID !== undefined) canonicalUUID(launchUUID, "Agent Bridge launch UUID");
    actor = await call<ActorRecord>("actor.register", {
      launch_uuid: launchUUID,
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
    await reconcileLeases();
    const saved = savedSelection(ctx);
    let staleSavedSelection = false;
    if (saved?.direction_uuid) {
      try {
        selectedDirection = await fetchDirection(saved.direction_uuid);
      } catch (error) {
        reportError(ctx, error);
        selectedDirection = undefined;
        staleSavedSelection = true;
      }
    }
    if (saved?.work_unit_uuid) {
      try {
        const restored = await fetchWorkUnit(saved.work_unit_uuid);
        if (restored.direction_uuid && !selectedDirection) selectedDirection = await fetchDirection(restored.direction_uuid);
        selectedWorkUnit = restored;
      } catch (error) {
        reportError(ctx, error);
        selectedWorkUnit = undefined;
        staleSavedSelection = true;
      }
    }
    if (staleSavedSelection) persistSelection();
    const workUnitUUID = process.env.AGENT_BRIDGE_WORK_UNIT_UUID;
    if (workUnitUUID !== undefined) {
      canonicalUUID(workUnitUUID, "Agent Bridge WorkUnit UUID");
      const inheritedWorkUnit = await fetchWorkUnit(workUnitUUID);
      if (inheritedWorkUnit.direction_uuid && !selectedDirection)
        selectedDirection = await fetchDirection(inheritedWorkUnit.direction_uuid);
      await call("work_unit.join", { work_unit_uuid: workUnitUUID, actor: actor.address });
      await selectWorkUnit(inheritedWorkUnit);
      persistSelection();
    }
    await recordSessionEvent("session.started", { data: { reason: event.reason, work_unit_uuid: workUnitUUID } });
    await pollMailbox();
    heartbeatTimer = setInterval(() => void heartbeat(), HEARTBEAT_MS);
    heartbeatTimer.unref?.();
    jjTimer = setInterval(() => void refreshVCS().catch((error) => reportError(sessionCtx, error)), JJ_REFRESH_MS);
    jjTimer.unref?.();
    mailboxTimer = setInterval(() => void pollMailbox(), MAILBOX_POLL_MS);
    mailboxTimer.unref?.();
    leaseRenewTimer = setInterval(() => void renewOpenLeases(), LEASE_RENEW_MS);
    leaseRenewTimer.unref?.();
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

  pi.on("session_before_compact", async () => {
    compacting = true;
  });

  pi.on("session_compact", async (event) => {
    try {
      const entry = event.compactionEntry as { summary?: unknown; tokensBefore?: unknown };
      const summary = String(entry?.summary ?? "");
      await recordSessionEvent("session.compacted", {
        summary: summary.length <= 20_000 ? summary : `${summary.slice(0, 19_999)}…`,
        data: { reason: event.reason, tokens_before: entry?.tokensBefore },
      });
    } finally {
      compacting = false;
      void heartbeat();
      void pollMailbox();
    }
  });

  pi.on("tool_call", async (event, ctx) => {
    if (!actor) return;
    const toolName =
      String(event.toolName ?? "")
        .split(".")
        .at(-1) ?? "";
    const input = event.input as Record<string, unknown> | undefined;
    if (toolName === "bash" && typeof input?.command === "string" && pollingSleepCommand(input.command)) {
      return {
        block: true,
        reason: "Polling sleeps are disabled. Finish the turn and let Pi mailbox events reactivate the session.",
      };
    }
    const toolCallId = String(event.toolCallId ?? "");
    if (toolName === "bash" && typeof input?.command === "string" && verificationCommand(input.command) && toolCallId) {
      verificationRuns.set(toolCallId, { command: input.command, cwd: ctx.cwd, startedAt: new Date() });
    }
    const mutation = inferMutation(event, ctx.cwd);
    if (!mutation) return;
    const mutationToolCallId = String(event.toolCallId ?? randomUUID());
    const started = new Date();
    const vcsCwd = mutation.paths[0] ? dirname(mutation.paths[0]) : ctx.cwd;
    const [before, mutationGit, mutationJj] = await Promise.all([
      snapshotFiles(mutation.paths),
      inspectGit(pi, vcsCwd),
      inspectJj(pi, vcsCwd),
    ]);
    const intent: MutationIntent = {
      id: `${actor.address}:${generation}:${mutationToolCallId}`,
      actor: actor.address,
      session_generation: generation,
      turn_id: currentTurnIndex === undefined ? undefined : `${actor.address}:${generation}:turn:${currentTurnIndex}`,
      turn_index: currentTurnIndex,
      tool_call_id: mutationToolCallId,
      ...mutation,
      cwd: ctx.cwd,
      workspace_key:
        mutationGit?.worktree_root ?? mutationJj?.workspace_root ?? actor.git?.worktree_root ?? actor.jj?.workspace_root ?? ctx.cwd,
      git: mutationGit ?? actor.git,
      jj: mutationJj ?? actor.jj,
      context: inferIntentContext(ctx.sessionManager.getBranch()),
      repository_uuid: actor.repository_uuid,
      workspace_uuid: actor.workspace_uuid,
      before,
      started_at: started.toISOString(),
      expires_at: new Date(started.getTime() + INTENT_TTL_MS).toISOString(),
    };
    openIntents.set(mutationToolCallId, intent);
    await call<IntentResult>("intent.begin", { intent });
    if (
      mutationToolCallId &&
      (mutation.operation === "edit" || mutation.operation === "write") &&
      actor.repository_uuid &&
      actor.workspace_uuid
    ) {
      try {
        await admitMutationLease(intent, ctx.signal);
      } catch (error) {
        openIntents.delete(mutationToolCallId);
        await call("intent.end", {
          intent_id: intent.id,
          success: false,
          error: error instanceof Error ? error.message : String(error),
          after: before,
          completed_at: new Date().toISOString(),
        }).catch(() => undefined);
        return { block: true, reason: error instanceof Error ? error.message : String(error) };
      }
    }
    return;
  });

  async function endMutation(toolCallId: string, isError: boolean, errorReason = "tool result reported an error"): Promise<void> {
    const intent = openIntents.get(toolCallId);
    if (!intent) return;
    openIntents.delete(toolCallId);
    const vcsCwd = intent.paths[0] ? dirname(intent.paths[0]) : sessionCtx?.cwd;
    let after = intent.before;
    let gitAfter: GitContext | undefined;
    let jjAfter: JjContext | undefined;
    try {
      [after, gitAfter, jjAfter] = await Promise.all([
        snapshotFiles(intent.paths),
        vcsCwd ? inspectGit(pi, vcsCwd) : undefined,
        vcsCwd ? inspectJj(pi, vcsCwd) : undefined,
      ]);
    } catch (error) {
      reportError(sessionCtx, error);
    }
    await call("intent.end", {
      intent_id: intent.id,
      success: !isError,
      error: isError ? errorReason : undefined,
      after,
      git_after: gitAfter,
      jj_after: jjAfter,
      completed_at: new Date().toISOString(),
    }).catch((error) => reportError(sessionCtx, error));
  }

  pi.on("tool_result", async (event) => {
    const toolCallId = String(event.toolCallId ?? "");
    const lease = detachLease(toolCallId);
    try {
      const verification = verificationRuns.get(toolCallId);
      verificationRuns.delete(toolCallId);
      if (verification && actor && isBashToolResult(event)) {
        const metadata = bashResultMetadata(event.details, event.content, event.isError);
        const outcome = metadata.exitCode === undefined ? "blocked" : metadata.exitCode === 0 && !event.isError ? "passed" : "failed";
        const completedAt = new Date();
        const outputBytes = Buffer.byteLength(metadata.output, "utf8");
        try {
          const persisted = await call<TestResult>("test.result", {
            result: {
              id: createHash("sha256").update(`${actor.address}:${generation}:test-result:${toolCallId}`).digest("hex"),
              actor: actor.address,
              session_generation: generation,
              turn_id: currentTurnIndex === undefined ? undefined : `${actor.address}:${generation}:turn:${currentTurnIndex}`,
              turn_index: currentTurnIndex,
              tool_call_id: toolCallId,
              command: verification.command,
              cwd: verification.cwd,
              exit_code: metadata.exitCode,
              outcome,
              started_at: verification.startedAt.toISOString(),
              completed_at: completedAt.toISOString(),
              duration_ms: completedAt.getTime() - verification.startedAt.getTime(),
              output_excerpt: metadata.output.slice(0, 600),
              output_sha256: createHash("sha256").update(metadata.output).digest("hex"),
              output_bytes: outputBytes,
              output_truncated: metadata.truncated,
              repository_uuid: actor.repository_uuid,
              workspace_uuid: actor.workspace_uuid,
              git: actor.git,
              jj: actor.jj,
            },
          });
          if (typeof persisted?.id === "string" && persisted.id.trim()) {
            const daemonOutcome = persisted.outcome;
            const confirmedOutcome = outcome === "failed" || outcome === "blocked" ? outcome : (daemonOutcome ?? outcome);
            capturedTestResults.push({ id: persisted.id.trim(), outcome: confirmedOutcome });
          }
        } catch (error) {
          reportError(sessionCtx, error);
        }
      }
      await endMutation(toolCallId, Boolean(event.isError));
    } finally {
      await releaseDetachedLease(lease);
    }
  });

  pi.on("tool_execution_end", async (event) => {
    const toolCallId = String(event.toolCallId ?? "");
    const lease = detachLease(toolCallId);
    try {
      await endMutation(toolCallId, Boolean(event.isError));
    } finally {
      await releaseDetachedLease(lease);
    }
  });

  pi.on("agent_settled", async () => {
    if (compacting) {
      // Failed or canceled compaction has no session_compact event.
      compacting = false;
      void heartbeat();
      void pollMailbox();
    }
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
    if (leaseRenewTimer) clearInterval(leaseRenewTimer);
    heartbeatTimer = undefined;
    jjTimer = undefined;
    mailboxTimer = undefined;
    leaseRenewTimer = undefined;
    const pendingToolCalls = new Set([...openIntents.keys(), ...openLeases.keys()]);
    for (const toolCallId of pendingToolCalls) {
      const lease = detachLease(toolCallId);
      try {
        await endMutation(toolCallId, true, "session ended before tool completion");
      } finally {
        await releaseDetachedLease(lease);
      }
    }
    openIntents.clear();
    openLeases.clear();
    await recordSessionEvent("session.shutdown", { data: { reason: event.reason } }).catch(() => undefined);
    if (actor) await call("actor.heartbeat", { address: actor.address, state: "dead", generation }).catch(() => undefined);
    actor = undefined;
    sessionCtx = undefined;
    compacting = false;
    backgroundOutage = false;
    backgroundRecovered.clear();
    for (const retry of Object.values(backgroundRetry)) {
      retry.failures = 0;
      retry.nextAt = 0;
    }
    selectedWorkUnit = undefined;
    selectedDirection = undefined;
    capturedTestResults = [];
    verificationRuns.clear();
    deliveredInRuntime.clear();
    pendingAcknowledgements.clear();
    ctx.ui.setStatus(BRIDGE_MESSAGE_TYPE, undefined);
  });

  return { client, sessions };
}

export default function agentBridge(pi: ExtensionAPI) {
  createAgentBridge(pi);
}
