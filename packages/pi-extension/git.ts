import { basename, dirname, resolve } from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import type { GitContext } from "./protocol";

type ExecResult = { stdout?: string; code?: number | null; killed?: boolean };

async function run(pi: ExtensionAPI, cwd: string, args: string[]): Promise<string | undefined> {
  try {
    const result = (await pi.exec("git", args, { timeout: 2_000, cwd })) as ExecResult;
    if ((result.code ?? 0) !== 0 || result.killed) return undefined;
    const value = String(result.stdout ?? "").trim();
    return value || undefined;
  } catch {
    return undefined;
  }
}

export async function inspectGit(pi: ExtensionAPI, cwd: string): Promise<GitContext | undefined> {
  const worktreeRoot = await run(pi, cwd, ["rev-parse", "--show-toplevel"]);
  if (!worktreeRoot) return undefined;
  const directories = await run(pi, cwd, ["rev-parse", "--path-format=absolute", "--absolute-git-dir", "--git-common-dir"]);
  if (!directories) return undefined;
  const [gitDirValue, commonDirValue] = directories.split("\n");
  if (!gitDirValue || !commonDirValue) return undefined;
  const gitDir = resolve(cwd, gitDirValue);
  const commonDir = resolve(cwd, commonDirValue);
  const branch = await run(pi, cwd, ["symbolic-ref", "--quiet", "--short", "HEAD"]);
  const head = await run(pi, cwd, ["rev-parse", "--verify", "HEAD"]);
  return {
    repo_root: basename(commonDir) === ".git" ? dirname(commonDir) : worktreeRoot,
    worktree_root: worktreeRoot,
    git_dir: gitDir,
    common_dir: commonDir,
    branch,
    head,
    detached: !branch && Boolean(head),
  };
}
