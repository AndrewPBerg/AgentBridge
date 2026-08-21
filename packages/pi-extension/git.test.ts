import { describe, expect, it } from "vitest";
import { inspectGit } from "./git";
import { createMockPi } from "./test/mocks/pi-coding-agent";

describe("Git context inspection", () => {
  it("reports first-class worktree, repository, branch, and HEAD identity", async () => {
    const pi = createMockPi();
    pi.exec
      .mockResolvedValueOnce({ stdout: "/repo/worktree\n", stderr: "", code: 0 })
      .mockResolvedValueOnce({ stdout: "/repo/worktree/.git\n/repo/.git\n", stderr: "", code: 0 })
      .mockResolvedValueOnce({ stdout: "feature/bridge\n", stderr: "", code: 0 })
      .mockResolvedValueOnce({ stdout: "abcdef1234567890\n", stderr: "", code: 0 });

    await expect(inspectGit(pi, "/repo/worktree/src")).resolves.toEqual({
      repo_root: "/repo",
      worktree_root: "/repo/worktree",
      git_dir: "/repo/worktree/.git",
      common_dir: "/repo/.git",
      branch: "feature/bridge",
      head: "abcdef1234567890",
      detached: false,
    });
    expect(pi.exec).toHaveBeenNthCalledWith(1, "git", ["rev-parse", "--show-toplevel"], {
      timeout: 2_000,
      cwd: "/repo/worktree/src",
    });
  });

  it("returns undefined outside Git repositories", async () => {
    const pi = createMockPi();
    pi.exec.mockResolvedValue({ stdout: "", stderr: "not a repository", code: 128 });
    await expect(inspectGit(pi, "/tmp/plain")).resolves.toBeUndefined();
  });
});
