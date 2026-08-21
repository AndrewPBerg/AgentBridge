import { createHash } from "node:crypto";
import { constants } from "node:fs";
import { type FileHandle, lstat, open } from "node:fs/promises";
import type { FileSnapshot } from "./protocol";

const DEFAULT_HASH_LIMIT = 16 * 1024 * 1024;

function hashLimit(): number {
  const configured = Number(process.env.AGENT_BRIDGE_HASH_MAX_BYTES ?? DEFAULT_HASH_LIMIT);
  return Number.isSafeInteger(configured) && configured >= 0 ? configured : DEFAULT_HASH_LIMIT;
}

async function hashFile(handle: FileHandle): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of handle.createReadStream({ autoClose: false, start: 0 })) hash.update(chunk);
  return hash.digest("hex");
}

export async function snapshotFile(path: string): Promise<FileSnapshot> {
  try {
    const pathInfo = await lstat(path);
    if (pathInfo.isSymbolicLink()) {
      return { path, exists: true, kind: "symlink", size: pathInfo.size, modified_at: pathInfo.mtime.toISOString() };
    }
    if (!pathInfo.isFile()) {
      const kind = pathInfo.isDirectory() ? "directory" : "other";
      return { path, exists: true, kind, size: pathInfo.size, modified_at: pathInfo.mtime.toISOString() };
    }

    // O_NOFOLLOW closes the lstat/open race: replacing the path with a symlink
    // causes open to fail instead of following and hashing the target. Hashing
    // then uses the already-open descriptor, never a second path lookup. On a
    // platform without O_NOFOLLOW, fail closed by recording metadata only.
    if (typeof constants.O_NOFOLLOW !== "number") {
      return { path, exists: true, kind: "file", size: pathInfo.size, modified_at: pathInfo.mtime.toISOString() };
    }
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const info = await handle.stat();
      if (!info.isFile()) return { path, exists: true, kind: "changed", size: info.size, modified_at: info.mtime.toISOString() };
      const snapshot: FileSnapshot = {
        path,
        exists: true,
        kind: "file",
        size: info.size,
        modified_at: info.mtime.toISOString(),
      };
      if (info.size <= hashLimit()) snapshot.sha256 = await hashFile(handle);
      return snapshot;
    } finally {
      await handle.close();
    }
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ENOENT") return { path, exists: false };
    if (code === "ELOOP") return { path, exists: true, kind: "symlink" };
    return { path, exists: false, kind: `unreadable:${code ?? "unknown"}` };
  }
}

export async function snapshotFiles(paths: string[]): Promise<FileSnapshot[]> {
  return Promise.all(paths.map(snapshotFile));
}
