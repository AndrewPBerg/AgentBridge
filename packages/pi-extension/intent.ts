import { realpathSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import type { IntentContext, MutationIntent } from "./protocol";

type ToolEvent = { toolName?: string; toolCallId?: string; input?: Record<string, unknown> };

type InferredMutation = Pick<MutationIntent, "tool" | "operation" | "paths">;

const MUTATING_BASH = /^\s*(jj\s+restore|git\s+restore|git\s+checkout\s+--|rm\s+|mv\s+|cp\s+)/;
const SHELL_TOKEN = /"([^"\\]*(?:\\.[^"\\]*)*)"|'([^']*)'|([^\s;&|<>]+)/g;
const BASH_OPERATIONS: Array<{ prefix: string[]; operation: MutationIntent["operation"] }> = [
  { prefix: ["jj", "restore"], operation: "restore" },
  { prefix: ["git", "restore"], operation: "restore" },
  { prefix: ["git", "checkout", "--"], operation: "restore" },
  { prefix: ["rm"], operation: "delete" },
  { prefix: ["mv"], operation: "move" },
  { prefix: ["cp"], operation: "copy" },
];

function canonicalPath(cwd: string, path: string): string {
  const absolute = isAbsolute(path) ? resolve(path) : resolve(cwd, path);
  try {
    return realpathSync(absolute);
  } catch {
    return absolute;
  }
}

function shellWords(command: string): string[] {
  return [...command.matchAll(SHELL_TOKEN)].map((match) => match[1] ?? match[2] ?? match[3] ?? "");
}

function bashMutation(command: string, cwd: string): InferredMutation | undefined {
  if (!MUTATING_BASH.test(command)) return undefined;
  const words = shellWords(command);
  const match = BASH_OPERATIONS.find(({ prefix }) => prefix.every((word, index) => words[index] === word));
  if (!match) return undefined;

  const operands = words
    .slice(match.prefix.length)
    .filter((word) => word && word !== "--" && !word.startsWith("-"))
    .map((path) => canonicalPath(cwd, path));
  if (!operands.length) return undefined;
  return { tool: "bash", operation: match.operation, paths: [...new Set(operands)] };
}

export function inferMutation(event: ToolEvent, cwd: string): InferredMutation | undefined {
  const tool =
    String(event.toolName ?? "")
      .split(".")
      .at(-1) ?? "";
  const input = event.input ?? {};
  if ((tool === "edit" || tool === "write") && typeof input.path === "string") {
    return {
      tool,
      operation: tool,
      paths: [canonicalPath(cwd, input.path)],
    };
  }
  if (tool === "bash" && typeof input.command === "string") return bashMutation(input.command, cwd);
  return undefined;
}

function contentText(content: unknown): string {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .filter((part) => part && typeof part === "object" && (part as { type?: string }).type === "text")
    .map((part) => String((part as { text?: unknown }).text ?? ""))
    .join("\n");
}

function boundedExcerpt(value: string, limit = 600): string | undefined {
  const compact = value.replace(/\s+/g, " ").trim();
  if (!compact) return undefined;
  return compact.length <= limit ? compact : `${compact.slice(0, limit - 1)}…`;
}

export function inferIntentContext(entries: unknown[]): IntentContext {
  let assistantExcerpt: string | undefined;
  for (let index = entries.length - 1; index >= 0 && !assistantExcerpt; index -= 1) {
    const entry = entries[index] as { type?: string; message?: { role?: string; content?: unknown } };
    if (entry.type !== "message" || entry.message?.role !== "assistant") continue;
    assistantExcerpt = boundedExcerpt(contentText(entry.message.content));
  }
  return { assistant_excerpt: assistantExcerpt };
}

export const __intentTest = { bashMutation, canonicalPath, shellWords };
