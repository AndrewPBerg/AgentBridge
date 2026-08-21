import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { snapshotFile } from "./provenance";

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("mutation snapshots", () => {
  it("records metadata and content hashes without storing file contents", async () => {
    const root = await mkdtemp(join(tmpdir(), "agent-bridge-provenance-"));
    roots.push(root);
    const path = join(root, "schema.ts");
    await writeFile(path, "status=base\n", "utf8");
    const snapshot = await snapshotFile(path);
    expect(snapshot).toMatchObject({ path, exists: true, kind: "file", size: 12 });
    expect(snapshot.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(JSON.stringify(snapshot)).not.toContain("status=base");
  });

  it("does not follow or hash symlink targets", async () => {
    const root = await mkdtemp(join(tmpdir(), "agent-bridge-provenance-link-"));
    roots.push(root);
    const target = join(root, "sensitive.txt");
    const link = join(root, "repo-link.txt");
    await writeFile(target, "secret-content\n", "utf8");
    await symlink(target, link);
    const snapshot = await snapshotFile(link);
    expect(snapshot).toMatchObject({ path: link, exists: true, kind: "symlink" });
    expect(snapshot.sha256).toBeUndefined();
    expect(JSON.stringify(snapshot)).not.toContain(target);
    expect(JSON.stringify(snapshot)).not.toContain("secret-content");
  });

  it("represents missing paths explicitly", async () => {
    await expect(snapshotFile("/definitely/missing/agent-bridge-file")).resolves.toEqual({
      path: "/definitely/missing/agent-bridge-file",
      exists: false,
    });
  });
});
