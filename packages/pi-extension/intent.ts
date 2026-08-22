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
  // Never realpath here: following a repository symlink could leak the target
  // path and cause provenance hashing outside the workspace trust boundary.
  return isAbsolute(path) ? resolve(path) : resolve(cwd, path);
}

function shellWords(command: string): string[] {
  return [...command.matchAll(SHELL_TOKEN)].map((match) => match[1] ?? match[2] ?? match[3] ?? "");
}

function bashMutation(command: string, cwd: string): InferredMutation | undefined {
  const words = shellWords(command);
  if (
    !MUTATING_BASH.test(command) &&
    !words.some((_word, index) => BASH_OPERATIONS.some(({ prefix }) => prefix.every((part, offset) => words[index + offset] === part)))
  ) {
    return undefined;
  }
  let match: (typeof BASH_OPERATIONS)[number] | undefined;
  let matchIndex = -1;
  for (let index = 0; index < words.length; index += 1) {
    const candidate = BASH_OPERATIONS.find(({ prefix }) => prefix.every((word, offset) => words[index + offset] === word));
    if (candidate) {
      match = candidate;
      matchIndex = index;
      break;
    }
  }
  if (!match) return undefined;

  const operandWords = words.slice(matchIndex + match.prefix.length);
  const stop = operandWords.findIndex((word) => ["&&", "||", ";", "|"].includes(word));
  const operands = (stop < 0 ? operandWords : operandWords.slice(0, stop))
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
