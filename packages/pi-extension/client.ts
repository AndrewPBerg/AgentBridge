import { randomUUID } from "node:crypto";
import net from "node:net";
import { homedir } from "node:os";
import { join } from "node:path";

export type RPCError = { code: string; message: string };
type Response<T> = { id: string; result?: T; error?: RPCError };

export function defaultSocketPath(): string {
  if (process.env.AGENT_BRIDGE_SOCKET) return process.env.AGENT_BRIDGE_SOCKET;
  const state = process.env.AGENT_BRIDGE_STATE_DIR || join(homedir(), ".agent-bridge");
  return join(state, "bridge.sock");
}

export class BridgeClient {
  constructor(
    readonly socketPath = defaultSocketPath(),
    private readonly timeoutMs = 2_000,
  ) {}

  call<T>(method: string, params: unknown = {}): Promise<T> {
    const id = randomUUID();
    return new Promise<T>((resolve, reject) => {
      const socket = net.createConnection(this.socketPath);
      let buffer = "";
      let settled = false;
      const timeout = setTimeout(() => finish(new Error(`Agent Bridge ${method} timed out`)), this.timeoutMs);
      timeout.unref?.();
      const finish = (error?: Error, result?: T) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        socket.destroy();
        if (error) reject(error);
        else resolve(result as T);
      };
      socket.on("connect", () => socket.write(`${JSON.stringify({ id, method, params })}\n`));
      socket.on("data", (chunk) => {
        buffer += chunk.toString();
        const newline = buffer.indexOf("\n");
        if (newline < 0) return;
        try {
          const response = JSON.parse(buffer.slice(0, newline)) as Response<T>;
          if (response.error) finish(new Error(`${response.error.code}: ${response.error.message}`));
          else finish(undefined, response.result);
        } catch (error) {
          finish(error instanceof Error ? error : new Error(String(error)));
        }
      });
      socket.on("error", (error) => finish(error));
      socket.on("end", () => finish(new Error(`Agent Bridge closed before replying to ${method}`)));
    });
  }
}

export async function ensureDaemon(client: BridgeClient): Promise<void> {
  try {
    await client.call("ping");
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    throw new Error(`Agent Bridge daemon is unavailable (${detail}). Restart it with: systemctl --user restart agent-bridge.service`);
  }
}
