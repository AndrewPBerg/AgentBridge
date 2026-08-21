import { mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { inferIntentContext, inferMutation } from "./intent";

const roots: string[] = [];

afterEach(async () => {
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe("Agent Bridge intent inference", () => {
  it("automatically infers direct edits without an agent declaration", () => {
    expect(inferMutation({ toolName: "edit", toolCallId: "call-1", input: { path: "src/schema.ts" } }, "/repo")).toEqual({
      tool: "edit",
      operation: "edit",
      paths: [resolve("/repo/src/schema.ts")],
    });
  });

  it("preserves repository symlink paths instead of leaking real targets", async () => {
    const root = await mkdtemp(join(tmpdir(), "agent-bridge-intent-link-"));
    roots.push(root);
    const target = join(root, "secret");
    const link = join(root, "repo-link");
    await writeFile(target, "secret", "utf8");
    await symlink(target, link);
    expect(inferMutation({ toolName: "edit", input: { path: link } }, root)?.paths).toEqual([link]);
  });

  it("recognizes destructive restore commands conservatively", () => {
    expect(inferMutation({ toolName: "bash", input: { command: "jj restore -- src/schema.ts" } }, "/repo")).toEqual({
      tool: "bash",
      operation: "restore",
      paths: [resolve("/repo/src/schema.ts")],
    });
    expect(inferMutation({ toolName: "bash", input: { command: "pnpm test" } }, "/repo")).toBeUndefined();
  });

  it("captures bounded visible context instead of hidden reasoning or the entire transcript", () => {
    const context = inferIntentContext([
      { type: "message", message: { role: "user", content: "Update the shared schema" } },
      {
        type: "message",
        message: {
          role: "assistant",
          content: [
            { type: "thinking", thinking: "private" },
            { type: "text", text: "Editing schema first" },
          ],
        },
      },
    ]);
    expect(context).toEqual({ assistant_excerpt: "Editing schema first" });
  });
});
