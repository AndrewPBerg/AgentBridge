import { realpath } from "node:fs/promises";
import { join } from "node:path";
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
  const identity = await run(pi, cwd, ["log", "-r", "@", "--no-graph", "-T", 'change_id ++ "\\n" ++ commit_id ++ "\\n"']);
  if (!identity) return undefined;
  const [changeId, commitId] = identity.split("\n");
  if (!changeId) return undefined;
  let repoPath: string | undefined;
  try {
    repoPath = await realpath(join(workspaceRoot, ".jj", "repo"));
  } catch {
    // Older/external workspace metadata may not expose a local repo path.
  }
  const workspaces = await run(pi, cwd, ["workspace", "list", "-T", 'name ++ "\\t" ++ root ++ "\\t" ++ target.change_id() ++ "\\n"']);
  const workspaceName = workspaces
    ?.split("\n")
    .map((line) => line.split("\t"))
    .find(([, root, candidateChange]) => root === workspaceRoot && candidateChange === changeId)?.[0];
  return {
    repo_path: repoPath,
    workspace_name: workspaceName,
    workspace_root: workspaceRoot,
    change_id: changeId,
    commit_id: commitId,
  };
}
