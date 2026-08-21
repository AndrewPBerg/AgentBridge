import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { JjContext } from "./protocol";

type ExecResult = { stdout?: string; code?: number | null; killed?: boolean };

async function run(pi: ExtensionAPI, cwd: string, args: string[]): Promise<string | undefined> {
  try {
    const result = (await pi.exec("jj", ["--ignore-working-copy", ...args], { timeout: 2_000, cwd })) as ExecResult;
    if ((result.code ?? 0) !== 0 || result.killed) return undefined;
    const value = String(result.stdout ?? "").trim();
    return value || undefined;
  } catch {
    return undefined;
  }
}

export async function inspectJj(pi: ExtensionAPI, cwd: string): Promise<JjContext | undefined> {
  const workspaceRoot = await run(pi, cwd, ["workspace", "root"]);
  if (!workspaceRoot) return undefined;
  const changeId = await run(pi, cwd, ["log", "-r", "@", "--no-graph", "-T", 'change_id ++ "\\n"']);
  if (!changeId) return undefined;
  return { workspace_root: workspaceRoot, change_id: changeId.split("\n")[0]! };
}
