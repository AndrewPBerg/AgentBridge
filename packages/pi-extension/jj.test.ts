import { describe, expect, it } from "vitest";
import { inspectJj } from "./jj";
import { createMockPi } from "./test/mocks/pi-coding-agent";

describe("JJ context inspection", () => {
  it("reads workspace and change identity without snapshotting the shared working copy", async () => {
    const pi = createMockPi();
    pi.exec
      .mockResolvedValueOnce({ stdout: "/repo\n", stderr: "", code: 0 })
      .mockResolvedValueOnce({ stdout: "qpvuntsmabcdef\n", stderr: "", code: 0 });

    await expect(inspectJj(pi, "/repo/subdir")).resolves.toEqual({
      workspace_root: "/repo",
      change_id: "qpvuntsmabcdef",
    });
    expect(pi.exec).toHaveBeenNthCalledWith(1, "jj", ["--ignore-working-copy", "workspace", "root"], {
      timeout: 2_000,
      cwd: "/repo/subdir",
    });
    expect(pi.exec).toHaveBeenNthCalledWith(
      2,
      "jj",
      ["--ignore-working-copy", "log", "-r", "@", "--no-graph", "-T", 'change_id ++ "\\n"'],
      { timeout: 2_000, cwd: "/repo/subdir" },
    );
  });
});
