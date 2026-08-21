import net from "node:net";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { BridgeClient } from "./client";

const servers: net.Server[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise<void>((resolve) => server.close(() => resolve()))));
});

describe("BridgeClient", () => {
  it("exchanges newline-delimited JSON RPC over a Unix socket", async () => {
    const path = join("/tmp", `agent-bridge-client-${process.pid}-${Date.now()}.sock`);
    const server = net.createServer((socket) => {
      socket.once("data", (chunk) => {
        const request = JSON.parse(chunk.toString());
        socket.end(`${JSON.stringify({ id: request.id, result: { version: 1 } })}\n`);
      });
    });
    servers.push(server);
    await new Promise<void>((resolve, reject) => server.listen(path, resolve).once("error", reject));

    await expect(new BridgeClient(path).call("ping")).resolves.toEqual({ version: 1 });
  });

  it("surfaces daemon error codes", async () => {
    const path = join("/tmp", `agent-bridge-client-error-${process.pid}-${Date.now()}.sock`);
    const server = net.createServer((socket) => {
      socket.once("data", (chunk) => {
        const request = JSON.parse(chunk.toString());
        socket.end(`${JSON.stringify({ id: request.id, error: { code: "send_failed", message: "unknown actor" } })}\n`);
      });
    });
    servers.push(server);
    await new Promise<void>((resolve, reject) => server.listen(path, resolve).once("error", reject));

    await expect(new BridgeClient(path).call("message.send", {})).rejects.toThrow("send_failed: unknown actor");
  });
});
