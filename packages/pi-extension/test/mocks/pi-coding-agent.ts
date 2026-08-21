import { resolve } from "node:path";

export type ExtensionAPI = MockPi;
export type ExtensionContext = any;
export type ExtensionCommandContext = any;
export type Theme = any;

export function isToolCallEventType(toolName: string, event: { toolName?: string }) {
  return event.toolName === toolName;
}

export function isBashToolResult(event: { toolName?: string }) {
  return event.toolName === "bash";
}

export function createLocalBashOperations() {
  return {
    exec: async () => ({ output: "", exitCode: 0, cancelled: false, truncated: false }),
  };
}

export function createBashTool(_cwd: string, options: any = {}) {
  return {
    name: "bash",
    label: "bash",
    description: "Run a shell command",
    parameters: {},
    execute: vi.fn(async (_id, params, signal, _onUpdate) => {
      if (options.operations) {
        const chunks: Buffer[] = [];
        const result = await options.operations.exec(params.command, _cwd, {
          onData: (data: Buffer) => chunks.push(data),
          signal,
          timeout: params.timeout,
          env: process.env,
        });
        return { content: [{ type: "text", text: Buffer.concat(chunks).toString() }], details: result };
      }
      return { content: [{ type: "text", text: "local" }], details: {} };
    }),
  };
}

export function createReadTool(cwd: string, options: any = {}) {
  return {
    name: "read",
    label: "read",
    description: "Read a file",
    parameters: {},
    execute: vi.fn(async (_id, params) => {
      if (!options.operations) return { content: [{ type: "text", text: "local read" }], details: undefined };
      const path = resolve(cwd, params.path);
      await options.operations.access(path);
      const content = await options.operations.readFile(path);
      return { content: [{ type: "text", text: content.toString() }], details: undefined };
    }),
  };
}

export function createWriteTool(cwd: string, options: any = {}) {
  return {
    name: "write",
    label: "write",
    description: "Write a file",
    parameters: {},
    execute: vi.fn(async (_id, params) => {
      if (options.operations) {
        const path = resolve(cwd, params.path);
        await options.operations.mkdir(resolve(path, ".."));
        await options.operations.writeFile(path, params.content);
      }
      return { content: [{ type: "text", text: "written" }], details: undefined };
    }),
  };
}

export function createEditTool(cwd: string, options: any = {}) {
  return {
    name: "edit",
    label: "edit",
    description: "Edit a file",
    parameters: {},
    execute: vi.fn(async (_id, params) => {
      if (options.operations) {
        const path = resolve(cwd, params.path);
        await options.operations.access(path);
        const content = (await options.operations.readFile(path)).toString();
        await options.operations.writeFile(path, content);
      }
      return { content: [{ type: "text", text: "edited" }], details: undefined };
    }),
  };
}

export function createGrepTool(_cwd: string) {
  return {
    name: "grep",
    label: "grep",
    description: "Search file contents",
    parameters: {},
    execute: vi.fn(async () => ({ content: [{ type: "text", text: "local grep" }], details: undefined })),
  };
}

export class SessionManager {
  static async list() {
    return [];
  }
  static async listAll() {
    return [];
  }
}

export const CONFIG_DIR_NAME = ".pi";

class MockEventBus extends Map<string, Array<(...args: any[]) => any>> {
  on(channel: string, handler: (...args: any[]) => any) {
    const handlers = this.get(channel) ?? [];
    handlers.push(handler);
    this.set(channel, handlers);
    return () =>
      this.set(
        channel,
        (this.get(channel) ?? []).filter((candidate) => candidate !== handler),
      );
  }

  emit(channel: string, data: unknown) {
    for (const handler of this.get(channel) ?? []) handler(data);
  }
}

export class MockPi {
  events = new MockEventBus();
  commands = new Map<string, any>();
  tools = new Map<string, any>();
  messageRenderers = new Map<string, any>();
  entryRenderers = new Map<string, any>();
  toolRenderers = new Map<string, any>();
  shortcuts = new Map<string, any>();
  flags = new Map<string, any>();
  entries: Array<{ customType: string; data: any }> = [];
  activeTools = ["read", "bash", "edit", "write"];
  allTools = ["read", "bash", "edit", "write", "grep", "find", "ls"].map((name) => ({ name }));
  flagValues = new Map<string, any>();

  on(event: string, handler: (...args: any[]) => any) {
    const handlers = this.events.get(event) ?? [];
    handlers.push(handler);
    this.events.set(event, handlers);
  }

  registerCommand(name: string, definition: any) {
    this.commands.set(name, definition);
  }

  registerTool(definition: any) {
    this.tools.set(definition.name, definition);
    if (!this.allTools.some((tool) => tool.name === definition.name)) this.allTools.push({ name: definition.name });
  }

  registerMessageRenderer(type: string, renderer: any) {
    this.messageRenderers.set(type, renderer);
  }

  registerEntryRenderer(type: string, renderer: any) {
    this.entryRenderers.set(type, renderer);
  }

  registerToolRenderer(name: string, renderer: any) {
    this.toolRenderers.set(name, renderer);
  }

  registerShortcut(key: string, definition: any) {
    this.shortcuts.set(key, definition);
  }

  registerFlag(name: string, definition: any) {
    this.flags.set(name, definition);
  }

  appendEntry(customType: string, data: any) {
    this.entries.push({ customType, data });
  }

  sendUserMessage(_content?: unknown, _options?: unknown) {}
  sendMessage(_message?: unknown, _options?: unknown) {}
  exec = vi.fn(async (_command: string, _args: string[], _options?: unknown) => ({ stdout: "", stderr: "", code: 0 }));
  getActiveTools() {
    return this.activeTools;
  }
  getAllTools() {
    return this.allTools;
  }
  setActiveTools(names: string[]) {
    this.activeTools = names;
  }
  getFlag(name: string) {
    return this.flagValues.get(name);
  }
}

export function createMockPi() {
  return new MockPi();
}

export function createMockContext(overrides: Partial<any> = {}) {
  return {
    ui: {
      setStatus: vi.fn(),
      notify: vi.fn(),
      setWidget: vi.fn(),
      setTitle: vi.fn(),
      setEditorText: vi.fn(),
      getEditorText: vi.fn(() => ""),
      pasteToEditor: vi.fn(),
      addAutocompleteProvider: vi.fn(),
      confirm: vi.fn(),
      select: vi.fn(),
      input: vi.fn(),
    },
    sessionManager: {
      getBranch: vi.fn(() => []),
      getEntries: vi.fn(() => []),
      getLeafId: vi.fn(() => undefined),
      getSessionFile: vi.fn(() => undefined),
      getSessionId: vi.fn(() => "session-id"),
      getHeader: vi.fn(() => ({ id: "session-id" })),
    },
    cwd: process.cwd(),
    mode: "tui",
    hasUI: true,
    ...overrides,
  };
}

import { vi } from "vitest";
