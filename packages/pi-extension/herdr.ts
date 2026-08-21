import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { HerdrAgent } from "./protocol";

type ExecResult = { stdout?: string; code?: number | null; killed?: boolean };

type RawAgent = {
  agent?: unknown;
  agent_session?: { value?: unknown };
  agent_status?: unknown;
  cwd?: unknown;
  pane_id?: unknown;
  workspace_id?: unknown;
  terminal_title_stripped?: unknown;
};

export async function listHerdrAgents(pi: ExtensionAPI, signal?: AbortSignal): Promise<HerdrAgent[]> {
  try {
    const binary = process.env.HERDR_BIN_PATH || "herdr";
    const result = (await pi.exec(binary, ["agent", "list"], { signal, timeout: 2_000 })) as ExecResult;
    if ((result.code ?? 0) !== 0 || result.killed) return [];
    const envelope = JSON.parse(String(result.stdout ?? "")) as { result?: { agents?: RawAgent[] } };
    return (envelope.result?.agents ?? [])
      .filter((agent): agent is RawAgent & { pane_id: string } => typeof agent.pane_id === "string")
      .map((agent) => ({
        agent: String(agent.agent ?? "unknown"),
        agentSession: typeof agent.agent_session?.value === "string" ? agent.agent_session.value : undefined,
        status: String(agent.agent_status ?? "unknown"),
        cwd: typeof agent.cwd === "string" ? agent.cwd : undefined,
        paneId: agent.pane_id,
        workspaceId: typeof agent.workspace_id === "string" ? agent.workspace_id : undefined,
        title: typeof agent.terminal_title_stripped === "string" ? agent.terminal_title_stripped : undefined,
      }));
  } catch {
    return [];
  }
}
