import { describe, expect, it, vi } from "vitest";
import type { ActorRecord } from "./protocol";
import { initialTalkTargets, showTalkModal } from "./talk-modal";

function actor(session: string, options: Partial<ActorRecord> = {}): ActorRecord {
  const now = new Date().toISOString();
  return {
    address: `${session}`,
    harness: "pi",
    session_uuid: session,
    cwd: "/repo",
    state: "active",
    git: {
      repo_root: "/repo",
      worktree_root: "/repo",
      git_dir: "/repo/.git",
      common_dir: "/repo/.git",
      branch: "main",
      detached: false,
    },
    capabilities: [],
    generation: 1,
    started_at: now,
    heartbeat_at: now,
    ...options,
  };
}

const theme = {
  fg: (_color: string, value: string) => value,
  bg: (_color: string, value: string) => value,
  bold: (value: string) => value,
};

function modalContext(interact: (component: any, options: any) => void) {
  return {
    mode: "tui",
    ui: {
      custom: vi.fn(
        async (factory: any, options: any) =>
          new Promise((resolve) => {
            const component = factory({ requestRender: vi.fn() }, theme, {}, resolve);
            interact(component, options);
          }),
      ),
    },
  } as any;
}

function type(component: any, value: string) {
  for (const character of value) component.handleInput(character);
}

describe("Bus talk modal", () => {
  it("parses comma targets and repository fan-out consistently before compose", () => {
    const current = actor("current");
    const peers = [
      actor("a"),
      actor("b"),
      actor("outside", {
        cwd: "/other",
        git: {
          repo_root: "/other",
          worktree_root: "/other",
          git_dir: "/other/.git",
          common_dir: "/other/.git",
          branch: "main",
          detached: false,
        },
      }),
    ];
    expect(initialTalkTargets(current, peers, "@a,@b")).toEqual(["@a", "@b"]);
    expect(initialTalkTargets(current, peers, "--repo")).toEqual(["a", "b"]);
  });
  it("fans out to all active agents in the current repository", async () => {
    const current = actor("current");
    const peers = [
      actor("a", { alias: "walkie" }),
      actor("b", { cwd: "/repo/sub" }),
      actor("outside", {
        cwd: "/other",
        git: {
          repo_root: "/other",
          worktree_root: "/other",
          git_dir: "/other/.git",
          common_dir: "/other/.git",
          branch: "main",
          detached: false,
        },
      }),
    ];
    const ctx = modalContext((component, options) => {
      expect(options).toMatchObject({ overlay: true, overlayOptions: expect.objectContaining({ anchor: "center" }) });
      for (const width of [1, 2, 3, 10, 80]) {
        expect(component.render(width).every((line: string) => line.length <= width)).toBe(true);
      }
      component.handleInput("space"); // toggle All in this repo
      component.handleInput("enter"); // compose
      type(component, "hello repo");
      component.handleInput("enter");
    });

    await expect(showTalkModal(ctx, current, peers)).resolves.toEqual({
      targets: ["a", "b"],
      body: "hello repo",
    });
  });

  it("supports j/k navigation while preserving j/k typing in compose", async () => {
    const current = actor("current");
    const peers = [actor("a"), actor("b")];
    const ctx = modalContext((component) => {
      component.handleInput("j"); // All-in-repo → actor a
      component.handleInput("space");
      component.handleInput("enter");
      type(component, "jk message"); // j/k route to Editor in compose mode
      component.handleInput("enter");
    });
    await expect(showTalkModal(ctx, current, peers)).resolves.toEqual({
      targets: ["a"],
      body: "jk message",
    });
  });

  it("opens directly in compose mode for specified targets", async () => {
    const current = actor("current");
    const peer = actor("a", { alias: "walkie" });
    const second = actor("b", { alias: "talkie" });
    const ctx = modalContext((component) => {
      type(component, "direct hello");
      component.handleInput("enter");
    });
    await expect(showTalkModal(ctx, current, [peer, second], ["@walkie", "@talkie"])).resolves.toEqual({
      targets: ["a", "b"],
      body: "direct hello",
    });
  });

  it("returns from compose to recipients on escape, then cancels", async () => {
    const current = actor("current");
    const ctx = modalContext((component) => {
      component.focused = true;
      expect(component.focused).toBe(true);
      component.handleInput("enter"); // select current row and compose
      component.handleInput("escape"); // back to recipients
      component.handleInput("escape"); // cancel modal
    });
    await expect(showTalkModal(ctx, current, [actor("a")])).resolves.toBeNull();
  });
});
