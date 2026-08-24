import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "../..");
const installer = join(repoRoot, "scripts/install-pi-extension.sh");
const extensionFiles = [
  "client.ts",
  "git.ts",
  "herdr.ts",
  "index.ts",
  "intent.ts",
  "jj.ts",
  "provenance.ts",
  "protocol.ts",
  "talk-modal.ts",
  "README.md",
  "client.test.ts",
  "git.test.ts",
  "index.test.ts",
  "install-pi-extension.test.ts",
  "intent.test.ts",
  "jj.test.ts",
  "pi-events.test.ts",
  "provenance.test.ts",
  "talk-modal.test.ts",
  "test/mocks/pi-coding-agent.ts",
  "test/mocks/pi-ai.ts",
  "test/mocks/typebox.ts",
  "test/mocks/pi-tui.ts",
];
const skillFiles = ["SKILL.md", "references/provenance.md"];
function digest(base: string, files: string[]) {
  return createHash("sha256")
    .update(
      files
        .map(
          (file) =>
            `${createHash("sha256")
              .update(readFileSync(join(base, file)))
              .digest("hex")}  ${file}\n`,
        )
        .join(""),
    )
    .digest("hex");
}
function snapshot(base: string) {
  const out = new Map<string, Buffer>();
  const visit = (dir: string) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) visit(path);
      else out.set(relative(base, path), readFileSync(path));
    }
  };
  visit(base);
  return out;
}
function expectUnchanged(before: Map<string, Buffer>, base: string) {
  const after = snapshot(base);
  expect([...after.keys()].sort()).toEqual([...before.keys()].sort());
  for (const [path, bytes] of before) expect(after.get(path)).toEqual(bytes);
}

describe("install-pi-extension.sh", () => {
  it("installs managed bytes, preserves unrelated files, and verifies --check digest", () => {
    const piHome = mkdtempSync(join(tmpdir(), "agent-bridge-pi-"));
    try {
      const ext = join(piHome, "agent/extensions/agent-bridge"),
        skill = join(piHome, "agent/skills/agent-bridge");
      mkdirSync(join(ext, "local"), { recursive: true });
      mkdirSync(join(skill, "local"), { recursive: true });
      writeFileSync(join(ext, "local/unrelated.txt"), "keep extension\n");
      writeFileSync(join(skill, "local/unrelated.txt"), "keep skill\n");
      const installed = spawnSync("bash", [installer], { env: { ...process.env, PI_HOME: piHome }, encoding: "utf8" });
      expect(installed.status, installed.stderr).toBe(0);
      for (const file of extensionFiles)
        expect(readFileSync(join(ext, file))).toEqual(readFileSync(join(repoRoot, "packages/pi-extension", file)));
      for (const file of skillFiles)
        expect(readFileSync(join(skill, file))).toEqual(readFileSync(join(repoRoot, "skills/agent-bridge", file)));
      expect(readFileSync(join(ext, "local/unrelated.txt"), "utf8")).toBe("keep extension\n");
      expect(readFileSync(join(skill, "local/unrelated.txt"), "utf8")).toBe("keep skill\n");
      const deploymentDigest = digest(repoRoot, [
        ...extensionFiles.map((f) => join("packages/pi-extension", f)),
        ...skillFiles.map((f) => join("skills/agent-bridge", f)),
      ]);
      expect(readFileSync(join(ext, ".agent-bridge-deployment"), "utf8")).toBe(`agent-bridge-deployment-v1 ${deploymentDigest}\n`);
      expect(execFileSync("bash", [installer, "--check"], { env: { ...process.env, PI_HOME: piHome }, encoding: "utf8" })).toContain(
        deploymentDigest,
      );
    } finally {
      rmSync(piHome, { recursive: true, force: true });
    }
  });

  it("rolls both trees back when staged skill replacement fails", () => {
    const piHome = mkdtempSync(join(tmpdir(), "agent-bridge-pi-"));
    try {
      const ext = join(piHome, "agent/extensions/agent-bridge"),
        skill = join(piHome, "agent/skills/agent-bridge"),
        bin = join(piHome, "bin");
      mkdirSync(join(ext, "local"), { recursive: true });
      mkdirSync(join(skill, "local"), { recursive: true });
      writeFileSync(join(ext, "old.ts"), "old extension\n");
      writeFileSync(join(ext, "local/unrelated"), "extension untouched\n");
      writeFileSync(join(skill, "old.md"), "old skill\n");
      writeFileSync(join(skill, "local/unrelated"), "skill untouched\n");
      const beforeExt = snapshot(ext),
        beforeSkill = snapshot(skill);
      mkdirSync(bin);
      writeFileSync(
        join(bin, "mv"),
        '#!/usr/bin/env bash\nif [[ "$2" == */.agent-bridge-install.*/skill && "$3" == */agent/skills/agent-bridge ]]; then exit 73; fi\nexec /usr/bin/mv "$@"\n',
      );
      execFileSync("chmod", ["+x", join(bin, "mv")]);
      const failed = spawnSync("bash", [installer], {
        env: { ...process.env, PI_HOME: piHome, PATH: `${bin}:${process.env.PATH}` },
        encoding: "utf8",
      });
      expect(failed.status).not.toBe(0);
      expectUnchanged(beforeExt, ext);
      expectUnchanged(beforeSkill, skill);
    } finally {
      rmSync(piHome, { recursive: true, force: true });
    }
  });
});
